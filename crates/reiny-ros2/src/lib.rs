//! The reiny ↔ ROS 2 bridge — **an explicit conversion per type**. A library, not an engine
//! (`docs/design/0.5.0.md` §4).
//!
//! ROS 2 is typed by `.msg`, so pushing raw protobuf bytes at it leaves `RViz` unable to read them.
//! You therefore write a bridge launch and convert per type through a closure:
//!
//! ```ignore
//! use reiny_ros2::ros2_client::{MessageTypeName, ServiceTypeName};
//!
//! #[reiny::main]
//! async fn main(cloudy: Cloudy) -> reiny::Result<()> {
//!     let ros = reiny_ros2::Ros::new(&cloudy, "reiny_bridge")?;          // ROS_DOMAIN_ID from the env
//!     ros.export::<RobotState, JointState>(                                 // reiny → ROS
//!         "/joint_states", MessageTypeName::new("sensor_msgs", "JointState"), Qos::SENSOR,
//!         |s| JointState { .. })?;
//!     ros.import::<Twist, CmdVel>(                                          // ROS → reiny
//!         "/cmd_vel", MessageTypeName::new("geometry_msgs", "Twist"), Qos::COMMAND,
//!         |t| CmdVel { .. })?;
//!     ros.export_service::<Calibrate, SetBoolReq, SetBoolRes, _, _>(       // a ROS client → a reiny service
//!         "/calibrate", &ServiceTypeName::new("std_srvs", "SetBool"), |q| .., |r| ..)?;
//!     cloudy.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! - No ROS 2 installation is needed — [`ros2_client`] is pure-Rust DDS (`RustDDS`) and talks to a ROS 2
//!   running on Fast DDS / Connext.
//! - A ROS type is a [`ros2_client::Message`] (a serde struct). Standard messages come from
//!   `ros2-interfaces-<distro>`, or from three lines of struct of your own.
//! - For identically shaped types there are [`Ros::export_auto`] / [`Ros::import_auto`], which map by
//!   field name through `Topic::DESCRIPTOR` (the proto descriptor). Zero lines; a type that does not
//!   fit that rule uses the closure form instead.
//! - ROS's domain (`ROS_DOMAIN_ID`) and reiny's domain are different things; a bridge holds both.
//! - presence: `import` publishes under the bridge's id as the source. The ROS graph is not mirrored.
//! - `QoS`: reliability / durability / history map one to one. `priority` / `express` are dropped.

use std::sync::{Mutex, MutexGuard, PoisonError};

use prost::Message as ProstMessage;
use reiny::{Cloudy, Durability, History, Qos, Reliability, Result, Service, Topic};
use ros2_client::ros2::{QosPolicies, QosPolicyBuilder, policy};
use ros2_client::rustdds::DomainParticipantBuilder;
use ros2_client::{
    AService, Context, ContextOptions, Message, MessageTypeName, Name, Node, NodeName, NodeOptions,
    ServiceMapping, ServiceTypeName,
};
use tokio::task::JoinHandle;

pub use ros2_client;

mod auto;

/// `max_blocking_time` for `Reliable`. A starting point, matched to ROS 2's rmw default.
const RELIABLE_BLOCKING_MS: i64 = 100;
/// The `QoS` of a service's request / response topics (ROS 2's `rmw_qos_profile_services_default`).
const SERVICE_DEPTH: i32 = 10;
/// The deadline when calling a ROS service (10 s, the same as reiny's `Caller` default).
const ROS_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The way in to the ROS 2 side. One ROS node, with as many `export` / `import` routes as you like.
///
/// Dropping it stops every route and the spinner. The routes also stop when the `Cloudy` shuts down
/// (the reiny-side subscription starts returning `None`).
pub struct Ros<'c> {
    cloudy: &'c Cloudy,
    context: Context,
    node: Mutex<Node>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<'c> Ros<'c> {
    /// Create the ROS node `node_name` (in the namespace `/`) and put its spinner on tokio.
    /// The DDS domain comes from `ROS_DOMAIN_ID` (0 when unset), and `ROS_LOCALHOST_ONLY=1` keeps it
    /// to loopback (the same environment variables ROS 2 uses). Call it inside a tokio runtime.
    ///
    /// `ROS_LOCALHOST_ONLY=1` is unusable on Linux with rustdds 0.14: a loopback-only participant
    /// discovers its peers, but their loopback locators are lost when the discovery DB re-publishes
    /// the endpoint, so no user data is ever sent ("No locators for `RtpsReaderProxy`"). Leave it unset
    /// and separate the traffic with `ROS_DOMAIN_ID` until rustdds keeps that bucket.
    pub fn new(cloudy: &'c Cloudy, node_name: &str) -> Result<Self> {
        let domain_id = std::env::var("ROS_DOMAIN_ID")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(0);
        let localhost_only = std::env::var("ROS_LOCALHOST_ONLY").is_ok_and(|v| v == "1");
        let context = if localhost_only {
            let participant = DomainParticipantBuilder::new(domain_id)
                .with_only_networks([std::net::Ipv4Addr::LOCALHOST])
                .build()
                .map_err(err)?;
            Context::from_domain_participant(participant).map_err(err)?
        } else {
            Context::with_options(ContextOptions::new().domain_id(domain_id)).map_err(err)?
        };
        Self::with_context(cloudy, context, node_name)
    }

    /// With a [`Context`] of your own (DDS domain / security settings).
    pub fn with_context(cloudy: &'c Cloudy, context: Context, node_name: &str) -> Result<Self> {
        let name = NodeName::new("/", node_name).map_err(err)?;
        let mut node = context
            .new_node(name, NodeOptions::new().enable_rosout(false))
            .map_err(err)?;
        let spinner = node.spinner().map_err(err)?;
        let spin = tokio::spawn(async move {
            if let Err(e) = spinner.spin().await {
                tracing::warn!(error = ?e, "ros2: spinner stopped");
            }
        });
        Ok(Self {
            cloudy,
            context,
            node: Mutex::new(node),
            tasks: Mutex::new(vec![spin]),
        })
    }

    /// The DDS [`Context`].
    #[must_use]
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// The ROS [`Node`]. The escape hatch to what reiny-ros2 does not wrap (parameters / actions / arbitrary topics).
    #[must_use]
    pub fn node(&self) -> &Mutex<Node> {
        &self.node
    }

    /// reiny → ROS: subscribe to the type `T`, turn it into `R` with `convert`, and publish it on the ROS topic `topic`.
    pub fn export<T, R, F>(
        &self,
        topic: &str,
        ty: MessageTypeName,
        qos: Qos,
        convert: F,
    ) -> Result<()>
    where
        T: ProstMessage + Default + Topic + Send + 'static,
        R: Message + Send + Sync + 'static,
        F: Fn(T) -> R + Send + 'static,
    {
        self.export_with(topic, ty, qos, move |t| Ok(convert(t)))
    }

    /// The automatic form of [`Ros::export`]: deserialize into `R` by proto field name, through
    /// `T::DESCRIPTOR`. Zero lines of conversion when the names and types line up. An error if `T` has no descriptor.
    pub fn export_auto<T, R>(&self, topic: &str, ty: MessageTypeName, qos: Qos) -> Result<()>
    where
        T: ProstMessage + Default + Topic + Send + 'static,
        R: Message + Send + Sync + 'static,
    {
        let descriptor = auto::descriptor::<T>()?;
        self.export_with(topic, ty, qos, move |t: T| {
            auto::to_ros::<T, R>(&descriptor, &t)
        })
    }

    fn export_with<T, R, F>(
        &self,
        topic: &str,
        ty: MessageTypeName,
        qos: Qos,
        convert: F,
    ) -> Result<()>
    where
        T: ProstMessage + Default + Topic + Send + 'static,
        R: Message + Send + Sync + 'static,
        F: Fn(T) -> Result<R> + Send + 'static,
    {
        let name = parse_name(topic)?;
        let policies = qos_policies(qos);
        let publisher = {
            let mut node = lock(&self.node);
            let ros_topic = node.create_topic(&name, ty, &policies).map_err(err)?;
            node.create_publisher::<R>(&ros_topic, Some(policies))
                .map_err(err)?
        };
        let mut subscriber = self.cloudy.subscriber::<T>();
        if let History::KeepLast(n) = qos.history {
            subscriber = subscriber.latest(n);
        }
        let mut subscriber = subscriber.build()?;
        let topic = topic.to_string();
        self.spawn(async move {
            while let Some(value) = subscriber.recv().await {
                match convert(value) {
                    Ok(message) => {
                        // The failure type carries its message home (its contents are never inspected, so it needs no Debug).
                        if publisher.async_publish(message).await.is_err() {
                            tracing::warn!(%topic, "ros2: publish failed");
                        }
                    }
                    Err(e) => tracing::warn!(%topic, error = %e, "ros2: conversion failed"),
                }
            }
        });
        Ok(())
    }

    /// ROS → reiny: subscribe to the ROS topic `topic`, turn it into `T` with `convert`, and publish
    /// it on reiny (with the bridge's id as the source).
    pub fn import<R, T, F>(
        &self,
        topic: &str,
        ty: MessageTypeName,
        qos: Qos,
        convert: F,
    ) -> Result<()>
    where
        R: Message + Send + Sync + 'static,
        T: ProstMessage + Topic + Send + 'static,
        F: Fn(R) -> T + Send + 'static,
    {
        self.import_with(topic, ty, qos, move |r| Ok(convert(r)))
    }

    /// The automatic form of [`Ros::import`] (the inverse of [`Ros::export_auto`]).
    pub fn import_auto<R, T>(&self, topic: &str, ty: MessageTypeName, qos: Qos) -> Result<()>
    where
        R: Message + Send + Sync + 'static,
        T: ProstMessage + Default + Topic + Send + 'static,
    {
        let descriptor = auto::descriptor::<T>()?;
        self.import_with(topic, ty, qos, move |r: R| {
            auto::from_ros::<R, T>(&descriptor, &r)
        })
    }

    fn import_with<R, T, F>(
        &self,
        topic: &str,
        ty: MessageTypeName,
        qos: Qos,
        convert: F,
    ) -> Result<()>
    where
        R: Message + Send + Sync + 'static,
        T: ProstMessage + Topic + Send + 'static,
        F: Fn(R) -> Result<T> + Send + 'static,
    {
        let name = parse_name(topic)?;
        let policies = qos_policies(qos);
        let subscription = {
            let mut node = lock(&self.node);
            let ros_topic = node.create_topic(&name, ty, &policies).map_err(err)?;
            node.create_subscription::<R>(&ros_topic, Some(policies))
                .map_err(err)?
        };
        // A reiny publisher only has KeepLast(1) or KeepAll (an n-deep ring is the subscriber's).
        let history = match qos.history {
            History::KeepLast(1) => History::KeepLast(1),
            _ => History::KeepAll,
        };
        let publisher = self
            .cloudy
            .publisher::<T>()
            .qos(Qos { history, ..qos })
            .build()?;
        let topic = topic.to_string();
        self.spawn(async move {
            loop {
                match subscription.async_take().await {
                    Ok((message, _info)) => match convert(message) {
                        Ok(value) => {
                            if let Err(e) = publisher.send(value).await {
                                tracing::warn!(%topic, error = %e, "ros2: forward failed");
                            }
                        }
                        Err(e) => tracing::warn!(%topic, error = %e, "ros2: conversion failed"),
                    },
                    Err(e) => {
                        tracing::warn!(%topic, error = ?e, "ros2: take failed; route stops");
                        return;
                    }
                }
            }
        });
        Ok(())
    }

    /// A ROS client → a reiny service: serve the ROS service `name`, turn the request into `S` with
    /// `to_request`, call reiny's (any, same-domain) server, and answer with `to_response`.
    /// When reiny gives `NoReply` / `Timeout` / `reply_err`, nothing is answered on the ROS side (a ROS
    /// service has no notion of an error response).
    pub fn export_service<S, Q, P, FQ, FP>(
        &self,
        name: &str,
        ty: &ServiceTypeName,
        to_request: FQ,
        to_response: FP,
    ) -> Result<()>
    where
        S: Service + Send + 'static,
        S::Response: Send,
        Q: Message + Clone + Send + Sync + 'static,
        P: Message + Send + Sync + 'static,
        FQ: Fn(Q) -> S + Send + 'static,
        FP: Fn(S::Response) -> P + Send + 'static,
    {
        let service_name = parse_name(name)?;
        let server = lock(&self.node)
            .create_server::<AService<Q, P>>(
                ServiceMapping::Enhanced,
                &service_name,
                ty,
                service_qos(),
                service_qos(),
            )
            .map_err(err)?;
        let caller = self.cloudy.caller::<S>().build();
        let name = name.to_string();
        self.spawn(async move {
            loop {
                let (id, request) = match server.async_receive_request().await {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!(%name, error = ?e, "ros2: receive request failed; route stops");
                        return;
                    }
                };
                match caller.call(to_request(request)).await {
                    Ok(response) => {
                        if let Err(e) = server.async_send_response(id, to_response(response)).await {
                            tracing::warn!(%name, error = ?e, "ros2: send response failed");
                        }
                    }
                    Err(e) => tracing::warn!(%name, error = %e, "ros2: reiny call failed; no response"),
                }
            }
        });
        Ok(())
    }

    /// A reiny caller → a ROS service: serve the reiny service `S`, turn the request into a ROS request
    /// with `to_request`, call the ROS service `name`, and answer with `to_response`.
    /// A failure on the ROS side becomes a `reply_err`.
    pub fn import_service<S, Q, P, FQ, FP>(
        &self,
        name: &str,
        ty: &ServiceTypeName,
        to_request: FQ,
        to_response: FP,
    ) -> Result<()>
    where
        S: Service + Clone + Send + Sync + 'static,
        S::Response: Send,
        Q: Message + Clone + Send + Sync + 'static,
        P: Message + Send + Sync + 'static,
        FQ: Fn(S) -> Q + Send + 'static,
        FP: Fn(P) -> S::Response + Send + 'static,
    {
        let service_name = parse_name(name)?;
        let client = lock(&self.node)
            .create_client::<AService<Q, P>>(
                ServiceMapping::Enhanced,
                &service_name,
                ty,
                service_qos(),
                service_qos(),
            )
            .map_err(err)?;
        let mut server = self.cloudy.serve::<S>()?;
        let name = name.to_string();
        self.spawn(async move {
            while let Some(request) = server.recv().await {
                // A DDS client waits forever, so put a deadline on it (a request sent before the server is discovered is lost).
                let outcome = tokio::time::timeout(
                    ROS_CALL_TIMEOUT,
                    client.async_call_service(to_request(request.value.clone())),
                )
                .await;
                let result = match outcome {
                    Ok(Ok(response)) => request.reply(to_response(response)).await,
                    Ok(Err(e)) => request.reply_err(format!("ros2 {name}: {e:?}")).await,
                    Err(_) => request.reply_err(format!("ros2 {name}: timed out")).await,
                };
                if let Err(e) = result {
                    tracing::warn!(%name, error = %e, "ros2: reply failed");
                }
            }
        });
        Ok(())
    }

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        lock(&self.tasks).push(tokio::spawn(task));
    }
}

impl Drop for Ros<'_> {
    fn drop(&mut self) {
        for task in lock(&self.tasks).iter() {
            task.abort();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn err(e: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("ros2: {e:?}")
}

/// A ROS name such as `/joint_states`. A relative name gets a `/` in front.
fn parse_name(name: &str) -> Result<Name> {
    let absolute = if name.starts_with('/') {
        name.to_string()
    } else {
        format!("/{name}")
    };
    Name::parse(&absolute).map_err(|e| anyhow::anyhow!("ros2: bad name '{name}': {e:?}"))
}

/// reiny's [`Qos`] → DDS `QoS` (the ROS 2 column of §2.3). `priority` / `express` are dropped.
fn qos_policies(qos: Qos) -> QosPolicies {
    QosPolicyBuilder::new()
        .reliability(match qos.reliability {
            Reliability::BestEffort => policy::Reliability::BestEffort,
            Reliability::Reliable => policy::Reliability::Reliable {
                max_blocking_time: ros2_client::ros2::Duration::from_millis(RELIABLE_BLOCKING_MS),
            },
        })
        .durability(match qos.durability {
            Durability::Volatile => policy::Durability::Volatile,
            Durability::TransientLocal => policy::Durability::TransientLocal,
        })
        .history(match qos.history {
            History::KeepLast(n) => policy::History::KeepLast {
                depth: i32::try_from(n).unwrap_or(i32::MAX),
            },
            History::KeepAll => policy::History::KeepAll,
        })
        .build()
}

fn service_qos() -> QosPolicies {
    QosPolicyBuilder::new()
        .reliability(policy::Reliability::Reliable {
            max_blocking_time: ros2_client::ros2::Duration::from_millis(RELIABLE_BLOCKING_MS),
        })
        .history(policy::History::KeepLast {
            depth: SERVICE_DEPTH,
        })
        .build()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;

    /// ROS names are absolute. A relative one gets a `/` so `export("joint_states")` and
    /// `export("/joint_states")` reach the same topic, and a name ROS would reject fails here rather
    /// than at DDS discovery time, where nothing would ever arrive and nothing would say why.
    #[test]
    fn a_relative_name_is_made_absolute() {
        assert_eq!(
            parse_name("joint_states").unwrap().to_string(),
            "/joint_states"
        );
        assert_eq!(
            parse_name("/joint_states").unwrap().to_string(),
            "/joint_states"
        );
        assert_eq!(parse_name("a/b").unwrap().to_string(), "/a/b");

        for bad in [
            "",
            "/",
            "1leading_digit",
            "has space",
            "has-hyphen",
            "//double",
        ] {
            let err = parse_name(bad).expect_err(bad);
            assert!(err.to_string().contains(bad), "{err}");
        }
    }

    /// The `QoS` mapping is the documented contract with ROS 2 (`docs/design/0.5.0.md` §2.3), and it is
    /// what decides whether a ROS subscriber and a reiny publisher are compatible at all — DDS refuses
    /// to connect a `BestEffort` writer to a `Reliable` reader.
    #[test]
    fn qos_maps_onto_the_ros_2_profiles() {
        // SENSOR: BestEffort / Volatile / KeepLast(1) — ROS 2's SensorDataQoS.
        let sensor = qos_policies(Qos::SENSOR);
        assert!(!sensor.is_reliable());
        assert!(sensor.is_volatile());

        // COMMAND (= the default): Reliable / Volatile / KeepAll.
        let command = qos_policies(Qos::COMMAND);
        assert!(command.is_reliable());
        assert!(command.is_volatile());

        // STATE: Reliable / TransientLocal / KeepLast(1) — reiny's latched, ROS 2's transient local.
        let state = qos_policies(Qos::STATE);
        assert!(state.is_reliable());
        assert!(!state.is_volatile());
        assert_eq!(
            state.durability(),
            Some(policy::Durability::TransientLocal),
            "latched must arrive as transient local, or a late ROS subscriber gets nothing"
        );

        // The three profiles really are three different policies.
        assert_ne!(sensor, command);
        assert_ne!(command, state);
    }

    /// `History::KeepLast(n)` is a `usize` on reiny's side and an `i32` on DDS's. A depth past what
    /// DDS can express clamps instead of wrapping into a negative depth.
    #[test]
    fn a_history_depth_too_large_for_dds_clamps() {
        let huge = Qos {
            history: History::KeepLast(usize::MAX),
            ..Qos::DEFAULT
        };
        let expected = QosPolicyBuilder::new()
            .reliability(policy::Reliability::Reliable {
                max_blocking_time: ros2_client::ros2::Duration::from_millis(RELIABLE_BLOCKING_MS),
            })
            .durability(policy::Durability::Volatile)
            .history(policy::History::KeepLast { depth: i32::MAX })
            .build();
        assert_eq!(qos_policies(huge), expected);
    }

    /// A service's topics are Reliable whatever the caller asked for: a lost request or reply has no
    /// second chance, since ROS 2 services have no retry of their own.
    #[test]
    fn service_topics_are_always_reliable() {
        let qos = service_qos();
        assert!(qos.is_reliable());
        assert_eq!(qos, service_qos(), "it is a constant profile");
    }
}
