//! reiny ↔ ROS 2 bridge —— **型ごとの明示変換**。エンジンではなくライブラリ
//! (`docs/design/0.5.0.md` §4)。
//!
//! ROS 2 は `.msg` で型付けされるので protobuf の生バイトを流しても `RViz` は読めない。だから
//! 利用側が bridge launch を書き、型ごとにクロージャで変換する:
//!
//! ```ignore
//! use reiny_ros2::ros2_client::{MessageTypeName, ServiceTypeName};
//!
//! #[reiny::main]
//! async fn main(cloudy: Cloudy) -> reiny::Result<()> {
//!     let ros = reiny_ros2::Ros::new(&cloudy, "reiny_bridge")?;          // ROS_DOMAIN_ID は環境から
//!     ros.export::<RobotState, JointState>(                                 // reiny → ROS
//!         "/joint_states", MessageTypeName::new("sensor_msgs", "JointState"), Qos::SENSOR,
//!         |s| JointState { .. })?;
//!     ros.import::<Twist, CmdVel>(                                          // ROS → reiny
//!         "/cmd_vel", MessageTypeName::new("geometry_msgs", "Twist"), Qos::COMMAND,
//!         |t| CmdVel { .. })?;
//!     ros.export_service::<Calibrate, SetBoolReq, SetBoolRes, _, _>(       // ROS の client → reiny の service
//!         "/calibrate", &ServiceTypeName::new("std_srvs", "SetBool"), |q| .., |r| ..)?;
//!     cloudy.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! - ROS 2 のインストールは要らない —— [`ros2_client`] は pure Rust の DDS(RustDDS)で、
//!   Fast DDS / Connext の ROS 2 と喋る。
//! - ROS の型は [`ros2_client::Message`](serde の構造体)。標準 msg は `ros2-interfaces-<distro>`
//!   から、あるいは自前の struct 3 行。
//! - 同形の型には [`Ros::export_auto`] / [`Ros::import_auto`]: `Topic::DESCRIPTOR`(proto の
//!   descriptor)を経由してフィールド名で写す。0 行だが、規則に合わない型はクロージャ版で。
//! - ROS の domain(`ROS_DOMAIN_ID`)と reiny の domain は別物。bridge は両方を持つ。
//! - presence: `import` は bridge の id を source に publish する。ROS 側の graph は写さない。
//! - `QoS`: reliability / durability / history を 1:1 で写す。`priority` / `express` は捨てる。

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

/// `Reliable` の `max_blocking_time`。ROS 2 の rmw 既定に合わせた目安。
const RELIABLE_BLOCKING_MS: i64 = 100;
/// service の request / response topic の QoS(ROS 2 の `rmw_qos_profile_services_default` 相当)。
const SERVICE_DEPTH: i32 = 10;
/// ROS の service を呼ぶときの期限(reiny の `Caller` の既定と同じ 10 s)。
const ROS_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// ROS 2 側の入口。1 つの ROS node で、`export` / `import` を好きなだけ重ねる。
///
/// drop すると全経路と spinner が止まる。経路は `Cloudy` のシャットダウンでも止まる
/// (reiny 側の購読が `None` を返す)。
pub struct Ros<'c> {
    cloudy: &'c Cloudy,
    context: Context,
    node: Mutex<Node>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<'c> Ros<'c> {
    /// ROS node `node_name`(namespace `/`)を作り、spinner を tokio に載せる。
    /// DDS domain は `ROS_DOMAIN_ID`(無ければ 0)、`ROS_LOCALHOST_ONLY=1` ならループバックだけ
    /// (ROS 2 と同じ環境変数)。tokio runtime の中で呼ぶ。
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

    /// 自前の [`Context`](DDS domain / security 設定)で。
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

    /// DDS の [`Context`]。
    #[must_use]
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// ROS の [`Node`]。reiny-ros2 が包んでいないもの(parameters / actions / 任意 topic)への逃げ道。
    #[must_use]
    pub fn node(&self) -> &Mutex<Node> {
        &self.node
    }

    /// reiny → ROS: 型 `T` を購読し、`convert` で `R` にして ROS topic `topic` へ publish する。
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

    /// [`Ros::export`] の自動写像版: `T::DESCRIPTOR` を経由して proto のフィールド名で `R` に
    /// deserialize する。名前と型が揃っていれば変換は 0 行。`T` に descriptor が無ければエラー。
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
                        // 失敗の型はメッセージを抱えて返る(Debug 不要にするため中身は見ない)。
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

    /// ROS → reiny: ROS topic `topic` を購読し、`convert` で `T` にして reiny に publish する
    /// (source は bridge の id)。
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

    /// [`Ros::import`] の自動写像版([`Ros::export_auto`] の逆)。
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
        // reiny の publisher は KeepLast(1) か KeepAll しか持たない(n 件のリングは購読側)。
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

    /// ROS の client → reiny の service: ROS service `name` を serve し、request を `to_request`
    /// で `S` にして reiny の(同 domain の任意の)server を呼び、応答を `to_response` で返す。
    /// reiny 側が `NoReply` / `Timeout` / `reply_err` なら ROS には応答しない(ROS の service に
    /// エラー応答の概念が無い)。
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

    /// reiny の caller → ROS の service: reiny の service `S` を serve し、request を `to_request`
    /// で ROS の request にして ROS service `name` を呼び、応答を `to_response` で返す。
    /// ROS 側の失敗は `reply_err` になる。
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
                // DDS の client は応答を待ち続けるので期限を切る(server 未発見の request は消える)。
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

/// `/joint_states` のような ROS の名前。相対名は `/` を付ける。
fn parse_name(name: &str) -> Result<Name> {
    let absolute = if name.starts_with('/') {
        name.to_string()
    } else {
        format!("/{name}")
    };
    Name::parse(&absolute).map_err(|e| anyhow::anyhow!("ros2: bad name '{name}': {e:?}"))
}

/// reiny の [`Qos`] → DDS の QoS(§2.3 の ROS 2 列)。`priority` / `express` は捨てる。
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
