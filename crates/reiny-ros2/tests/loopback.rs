//! ROS 無しで回る e2e: 同一プロセスに「ROS 役」の ros2-client node と、`Local` バス上の bridge
//! (`Ros`)を置き、RustDDS のループバック(同じ DDS domain)で繋ぐ。topic 双方向 + service 双方向。
//! DDS domain は process id から取り、並列実行で衝突しないようにする。
//!
//! Windows のデバッグビルドでは ignore: rustdds 0.14 が使う mio 0.6 の Windows UDP 実装に
//! null ポインタ参照があり、Rust 1.96 の UB 検査(debug のみ)で event loop が abort する。
//! `cargo test -p reiny-ros2 --release -- --include-ignored` なら通る。CI(ubuntu)は素で回る。

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // テストは panic で失敗を表現してよい

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

// --- reiny 側の型(prost) ---

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

// --- ROS 側の型(serde = ros2-client の Message) ---

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

/// ループバックだけの DDS participant。外向きのインタフェース(繋がっていない仮想アダプタを含む)
/// に触らないので、どの機械 / CI runner でも同一ホスト内の discovery が決定的に成立する。
fn loopback_context(domain_id: u16) -> Context {
    let participant = DomainParticipantBuilder::new(domain_id)
        .with_only_networks([std::net::Ipv4Addr::LOCALHOST])
        .build()
        .expect("participant");
    Context::from_domain_participant(participant).expect("context")
}

async fn open(bus: Local, id: &str) -> Cloudy {
    let mut opts = RuntimeOptions::new(id);
    opts.domain = "ros".to_string();
    opts.engine = Some(Arc::new(bus));
    opts.install_tracing = false;
    Cloudy::open(opts).await.expect("cloudy")
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // 1 本で通す(DDS の discovery を 1 回で済ませる)。
#[cfg_attr(
    all(windows, debug_assertions),
    ignore = "rustdds 0.14 (mio 0.6) aborts on Windows debug builds; run with --release --include-ignored"
)]
async fn bridge_round_trips_with_a_ros_node() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    // 並列テスト / 隣のプロセスと domain がぶつからないように pid から取る(1..=200)。
    let domain_id = u16::try_from(std::process::id() % 200).unwrap() + 1;

    let bus = Local::new();
    let cloudy = open(bus.clone(), "bridge").await;
    let launch = open(bus, "g").await;

    // --- ROS 役 ---
    let ros_ctx = loopback_context(domain_id);
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

    // --- bridge(同じ DDS domain の別 Context)---
    let ros = Ros::with_context(&cloudy, loopback_context(domain_id), "reiny_bridge").unwrap();
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

    // --- reiny 側の launch ---
    let states = launch.publish::<State>().unwrap();
    let mut cmds = launch.subscribe::<Cmd>().unwrap();
    let mut add = launch.serve::<Add>().unwrap();
    tokio::spawn(async move {
        while let Some(req) = add.recv().await {
            let sum = req.value.a + req.value.b;
            req.reply(Sum { sum }).await.unwrap();
        }
    });

    // --- DDS の discovery を待つ(固定 sleep ではなく graph を見る)---
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

    // ROS → reiny(source は bridge の id)
    ros_cmds.async_publish(RosCmd { v: 7 }).await.unwrap();
    let envelope = timeout(WAIT, cmds.recv_envelope())
        .await
        .expect("cmd within patience")
        .unwrap();
    assert_eq!((envelope.value.v, envelope.source.as_str()), (7, "bridge"));

    // ROS の client → reiny の service
    let res = timeout(WAIT, ros_add.async_call_service(RosAddReq { a: 2, b: 3 }))
        .await
        .expect("add within patience")
        .unwrap();
    assert_eq!(res.sum, 5);

    // reiny の caller → ROS の service。bridge の DDS client が ROS の server を見つけるまでは
    // request が消えるので、短い期限で撃ち直す。
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
