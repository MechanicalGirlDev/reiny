//! An e2e that runs without ROS: a "playing ROS" ros2-client node and a bridge (`Ros`) on a `Local`
//! bus, in one process, joined over `RustDDS` (the same DDS domain). Topics both ways, services both
//! ways. The DDS domain is derived from the process id, which is what keeps the traffic apart from a
//! parallel run or a neighbour on the network.
//!
//! Ignored on Windows debug builds: the Windows UDP code of mio 0.6, which rustdds 0.14 uses,
//! dereferences a null pointer, and Rust 1.96's UB check (debug only) aborts the event loop.
//! `cargo test -p reiny-ros2 --release -- --include-ignored` passes. CI (ubuntu) runs it as-is.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking

use std::sync::Arc;
use std::time::Duration;

use reiny::engine::Local;
use reiny::{Cloudy, Qos, RuntimeOptions, Service, Topic};
use reiny_ros2::Ros;
use reiny_ros2::ros2_client::ros2::{QosPolicyBuilder, policy};
use reiny_ros2::ros2_client::rustdds::DomainParticipantBuilder;
use reiny_ros2::ros2_client::{
    AService, Context, Message, MessageTypeName, Name, NodeName, NodeOptions, ServiceMapping,
    ServiceTypeName,
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

// --- the reiny-side types (prost) ---

#[derive(Clone, PartialEq, prost::Message)]
struct State {
    #[prost(double, tag = "1")]
    x: f64,
}
impl Topic for State {
    const TYPE: &'static str = "RosState";
}

#[derive(Clone, PartialEq, prost::Message)]
struct Cmd {
    #[prost(uint32, tag = "1")]
    v: u32,
}
impl Topic for Cmd {
    const TYPE: &'static str = "RosCmd";
}

#[derive(Clone, PartialEq, prost::Message)]
struct Add {
    #[prost(int32, tag = "1")]
    a: i32,
    #[prost(int32, tag = "2")]
    b: i32,
}
#[derive(Clone, PartialEq, prost::Message)]
struct Sum {
    #[prost(int32, tag = "1")]
    sum: i32,
}
impl Topic for Add {
    const TYPE: &'static str = "RosAdd";
}
impl Topic for Sum {
    const TYPE: &'static str = "RosSum";
}
impl Service for Add {
    type Response = Sum;
}

#[derive(Clone, PartialEq, prost::Message)]
struct Echo {
    #[prost(string, tag = "1")]
    text: String,
}
#[derive(Clone, PartialEq, prost::Message)]
struct Echoed {
    #[prost(string, tag = "1")]
    text: String,
}
impl Topic for Echo {
    const TYPE: &'static str = "RosEcho";
}
impl Topic for Echoed {
    const TYPE: &'static str = "RosEchoed";
}
impl Service for Echo {
    type Response = Echoed;
}

// --- the ROS-side types (serde = ros2-client's Message) ---

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct RosState {
    x: f64,
}
impl Message for RosState {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct RosCmd {
    v: u32,
}
impl Message for RosCmd {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct RosAddReq {
    a: i32,
    b: i32,
}
impl Message for RosAddReq {}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct RosAddRes {
    sum: i32,
}
impl Message for RosAddRes {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct RosEchoReq {
    text: String,
}
impl Message for RosEchoReq {}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct RosEchoRes {
    text: String,
}
impl Message for RosEchoRes {}

const WAIT: Duration = Duration::from_secs(10);

/// A DDS participant on `domain_id`. Which interfaces it may use is decided per platform, because
/// rustdds 0.14 is broken in the opposite direction on each:
///
/// - **Not Windows: every interface.** Loopback-only does discover the peer, but rustdds splits a
///   discovered endpoint's loopback locators into a bucket `DiscoveredReaderData` cannot carry, so
///   the discovery DB re-publishes that endpoint with an empty locator list and the writer is left
///   with no destination for user data ("No locators for `RtpsReaderProxy`"). On Linux `lo` carries no
///   multicast to fall back on, so nothing flows and this test hangs until its patience runs out.
/// - **Windows: loopback only.** An all-interfaces participant fails to build on a machine that has
///   virtual adapters which are not connected ("`UDPSender` construction fail: `AddrNotAvailable`"), and
///   loopback-only does deliver user data there.
fn context(domain_id: u16) -> Context {
    let builder = DomainParticipantBuilder::new(domain_id);
    #[cfg(windows)]
    let builder = builder.with_only_networks([std::net::Ipv4Addr::LOCALHOST]);
    Context::from_domain_participant(builder.build().expect("participant")).expect("context")
}

async fn open(bus: Local, id: &str) -> Cloudy {
    let mut opts = RuntimeOptions::new(id);
    opts.domain = "ros".to_string();
    opts.engine = Some(Arc::new(bus));
    opts.install_tracing = false;
    Cloudy::open(opts).await.expect("cloudy")
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // one pass end to end (so DDS discovery happens only once)
#[cfg_attr(
    all(windows, debug_assertions),
    ignore = "rustdds 0.14 (mio 0.6) aborts on Windows debug builds; run with --release --include-ignored"
)]
async fn bridge_round_trips_with_a_ros_node() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    // Derived from the pid (1..=200) so the domain does not clash with a parallel test or a neighbour.
    let domain_id = u16::try_from(std::process::id() % 200).unwrap() + 1;

    let bus = Local::new();
    let cloudy = open(bus.clone(), "bridge").await;
    let launch = open(bus, "g").await;

    // --- playing ROS ---
    let ros_ctx = context(domain_id);
    let mut ros_node = ros_ctx
        .new_node(
            NodeName::new("/", "ros_side").unwrap(),
            NodeOptions::new().enable_rosout(false),
        )
        .unwrap();
    let spinner = ros_node.spinner().unwrap();
    tokio::spawn(spinner.spin());
    let qos = QosPolicyBuilder::new()
        .reliability(policy::Reliability::Reliable {
            max_blocking_time: reiny_ros2::ros2_client::ros2::Duration::from_millis(100),
        })
        .history(policy::History::KeepLast { depth: 10 })
        .build();
    let state_topic = ros_node
        .create_topic(
            &Name::parse("/state").unwrap(),
            MessageTypeName::new("test_msgs", "State"),
            &qos,
        )
        .unwrap();
    let ros_states = ros_node
        .create_subscription::<RosState>(&state_topic, Some(qos.clone()))
        .unwrap();
    let cmd_topic = ros_node
        .create_topic(
            &Name::parse("/cmd").unwrap(),
            MessageTypeName::new("test_msgs", "Cmd"),
            &qos,
        )
        .unwrap();
    let ros_cmds = ros_node
        .create_publisher::<RosCmd>(&cmd_topic, Some(qos.clone()))
        .unwrap();
    let ros_add = ros_node
        .create_client::<AService<RosAddReq, RosAddRes>>(
            ServiceMapping::Enhanced,
            &Name::parse("/add").unwrap(),
            &ServiceTypeName::new("test_srvs", "Add"),
            qos.clone(),
            qos.clone(),
        )
        .unwrap();
    let ros_echo = ros_node
        .create_server::<AService<RosEchoReq, RosEchoRes>>(
            ServiceMapping::Enhanced,
            &Name::parse("/echo").unwrap(),
            &ServiceTypeName::new("test_srvs", "Echo"),
            qos.clone(),
            qos.clone(),
        )
        .unwrap();
    tokio::spawn(async move {
        while let Ok((id, req)) = ros_echo.async_receive_request().await {
            let text = format!("{}!", req.text);
            ros_echo
                .async_send_response(id, RosEchoRes { text })
                .await
                .unwrap();
        }
    });

    // --- the bridge (another Context on the same DDS domain) ---
    let ros = Ros::with_context(&cloudy, context(domain_id), "reiny_bridge").unwrap();
    ros.export::<State, RosState, _>(
        "/state",
        MessageTypeName::new("test_msgs", "State"),
        Qos::COMMAND,
        |s| RosState { x: s.x },
    )
    .unwrap();
    ros.import::<RosCmd, Cmd, _>(
        "/cmd",
        MessageTypeName::new("test_msgs", "Cmd"),
        Qos::COMMAND,
        |c| Cmd { v: c.v },
    )
    .unwrap();
    ros.export_service::<Add, RosAddReq, RosAddRes, _, _>(
        "/add",
        &ServiceTypeName::new("test_srvs", "Add"),
        |q| Add { a: q.a, b: q.b },
        |s| RosAddRes { sum: s.sum },
    )
    .unwrap();
    ros.import_service::<Echo, RosEchoReq, RosEchoRes, _, _>(
        "/echo",
        &ServiceTypeName::new("test_srvs", "Echo"),
        |e| RosEchoReq { text: e.text },
        |r| Echoed { text: r.text },
    )
    .unwrap();

    // --- the reiny-side launch ---
    let states = launch.publish::<State>().unwrap();
    let mut cmds = launch.subscribe::<Cmd>().unwrap();
    let mut add = launch.serve::<Add>().unwrap();
    tokio::spawn(async move {
        while let Some(req) = add.recv().await {
            let sum = req.value.a + req.value.b;
            req.reply(Sum { sum }).await.unwrap();
        }
    });

    // --- wait for DDS discovery (by looking at the graph, not by sleeping) ---
    timeout(WAIT, ros_states.wait_for_publisher(&ros_node))
        .await
        .expect("bridge publisher discovered");
    timeout(WAIT, ros_cmds.wait_for_subscription(&ros_node))
        .await
        .expect("bridge subscription discovered");
    timeout(WAIT, ros_add.wait_for_service(&ros_node))
        .await
        .expect("bridge service discovered");

    // reiny → ROS
    states.send(State { x: 1.5 }).await.unwrap();
    let (state, _) = timeout(WAIT, ros_states.async_take())
        .await
        .expect("state within patience")
        .unwrap();
    assert_eq!(state, RosState { x: 1.5 });

    // ROS → reiny (the source is the bridge's id)
    ros_cmds.async_publish(RosCmd { v: 7 }).await.unwrap();
    let envelope = timeout(WAIT, cmds.recv_envelope())
        .await
        .expect("cmd within patience")
        .unwrap();
    assert_eq!((envelope.value.v, envelope.source.as_str()), (7, "bridge"));

    // a ROS client → a reiny service
    let res = timeout(WAIT, ros_add.async_call_service(RosAddReq { a: 2, b: 3 }))
        .await
        .expect("add within patience")
        .unwrap();
    assert_eq!(res.sum, 5);

    // a reiny caller → a ROS service. Until the bridge's DDS client discovers the ROS server the
    // request is lost, so retry on a short deadline.
    let caller = launch
        .caller::<Echo>()
        .timeout(Duration::from_secs(2))
        .build();
    let mut echoed = None;
    for _ in 0..5 {
        if let Ok(r) = caller
            .call(Echo {
                text: "hi".to_string(),
            })
            .await
        {
            echoed = Some(r);
            break;
        }
    }
    assert_eq!(echoed.map(|e| e.text).as_deref(), Some("hi!"));
}
