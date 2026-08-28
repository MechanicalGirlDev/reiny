//! Services(0.4.0)の e2e: server 1 + client 2 を実際に zenoh で繋ぎ、往復・宛先指定・
//! `reply_err` → `Remote`・返さずに drop → `NoReply`・保留 → `Timeout`・指紋違い・
//! `servers()` / `watch_servers()`・**latched publish と serve の同居**を 1 本で通す。
//!
//! `e2e.rs` と同じ理由で `src/` に置く(private な `Cloudy::new` が要る)。ポートは e2e の
//! 37447 / `bag_e2e` の 37448 の次。

use std::time::Duration;

use tokio::time::timeout;

use crate::{CallError, Cloudy, PresenceEvent, Service, Topic, shutdown::Shutdown};

#[derive(Clone, PartialEq, prost::Message)]
struct Add {
    #[prost(int64, tag = "1")]
    a: i64,
    #[prost(int64, tag = "2")]
    b: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Sum {
    #[prost(int64, tag = "1")]
    sum: i64,
}

impl Topic for Add {
    const TYPE: &'static str = "ReinyRpcAdd";
    const SCHEMA: Option<u64> = Some(0xAAAA_AAAA_AAAA_AAAA);
}

impl Topic for Sum {
    const TYPE: &'static str = "ReinyRpcSum";
    const SCHEMA: Option<u64> = Some(0x5555_5555_5555_5555);
}

/// 手書き `impl Service` —— 第三者型が 1 行で参加できることの回帰でもある。
impl Service for Add {
    type Response = Sum;
}

/// 同じトピックだが **指紋が違う** request。別プロジェクトの同名型を再現する。
#[derive(Clone, PartialEq, prost::Message)]
struct AddV2 {
    #[prost(int64, tag = "1")]
    a: i64,
    #[prost(int64, tag = "2")]
    b: i64,
}

impl Topic for AddV2 {
    const TYPE: &'static str = "ReinyRpcAdd";
    const SCHEMA: Option<u64> = Some(0xBBBB_BBBB_BBBB_BBBB);
}

impl Service for AddV2 {
    type Response = Sum;
}

/// latched publish と serve を **同じ型**でやる grain の再現。
#[derive(Clone, PartialEq, prost::Message)]
struct Cfg {
    #[prost(uint32, tag = "1")]
    rev: u32,
}

impl Topic for Cfg {
    const TYPE: &'static str = "ReinyRpcCfg";
}

impl Service for Cfg {
    type Response = Cfg;
}

const ENDPOINT: &str = "tcp/127.0.0.1:37449";

async fn session(listen: bool) -> zenoh::Session {
    let mut config = zenoh::Config::default();
    let key = if listen {
        "listen/endpoints"
    } else {
        "connect/endpoints"
    };
    for (k, v) in [
        (key, format!("[\"{ENDPOINT}\"]")),
        ("scouting/multicast/enabled", "false".to_string()),
    ] {
        config
            .insert_json5(k, &v)
            .unwrap_or_else(|e| panic!("zenoh config {k}: {e}"));
    }
    zenoh::open(config)
        .await
        .unwrap_or_else(|e| panic!("opening zenoh session: {e}"))
}

async fn cloudy(session: zenoh::Session, id: &str) -> Cloudy {
    Cloudy::new(
        std::sync::Arc::new(crate::engine::Zenoh::from_session(session)),
        id.to_string(),
        "lab".to_string(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await
    .expect("cloudy")
}

const SETTLE: Duration = Duration::from_millis(600);
const PATIENCE: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // 1 本で通す(ポートを増やさない)ので長い。
async fn services_round_trip_presence_and_latched_coexistence() {
    let server = cloudy(session(true).await, "srv").await;
    let client = cloudy(session(false).await, "cli").await;
    let other = cloudy(session(false).await, "cli2").await;
    crate::e2e::wait_peers(server.session().expect("zenoh engine"), 2).await;

    // --- server: b の値で振る舞いを変える(正常 / reply_err / 返さず drop / 保留) ---
    let mut srv = server.serve::<Add>().expect("serve");
    let server_task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Some(req) = srv.recv().await {
            let Add { a, b } = req.value;
            match b {
                b if b < 0 => req.reply_err("negative b").await.expect("reply_err"),
                0 => drop(req),
                7 => held.push(req),
                _ => req.reply(Sum { sum: a + b }).await.expect("reply"),
            }
        }
        // srv(= liveliness token)はここで drop される。
    });
    tokio::time::sleep(SETTLE).await;

    // --- presence: server は servers() に出て、publishers() には混ざらない ---
    assert_eq!(client.servers::<Add>().await.expect("servers"), ["srv"]);
    assert!(
        client
            .publishers::<Add>()
            .await
            .expect("publishers")
            .is_empty(),
        "@service トークンは publisher の一覧に見えてはいけない"
    );

    // --- 往復(任意の server / 宛先指定) ---
    let sum = timeout(PATIENCE, client.call::<Add>(Add { a: 2, b: 3 }))
        .await
        .expect("call should return")
        .expect("call");
    assert_eq!(sum.sum, 5);
    let caller = client
        .caller::<Add>()
        .to("srv")
        .timeout(Duration::from_secs(2))
        .build();
    assert_eq!(
        caller.call(Add { a: 10, b: 1 }).await.expect("call").sum,
        11
    );

    // --- 居ない宛先 / reply_err / 返さずに drop / 保留 ---
    let nobody = client
        .caller::<Add>()
        .to("ghost")
        .timeout(Duration::from_secs(2))
        .build();
    assert!(matches!(
        nobody.call(Add { a: 1, b: 1 }).await,
        Err(CallError::NoReply)
    ));
    assert!(matches!(
        caller.call(Add { a: 1, b: -1 }).await,
        Err(CallError::Remote(m)) if m == "negative b"
    ));
    assert!(matches!(
        caller.call(Add { a: 1, b: 0 }).await,
        Err(CallError::NoReply)
    ));
    let impatient = client
        .caller::<Add>()
        .to("srv")
        .timeout(Duration::from_millis(500))
        .build();
    let held = impatient.call(Add { a: 1, b: 7 }).await;
    assert!(matches!(held, Err(CallError::Timeout)), "{held:?}");

    // --- 指紋違いの request は黙って捨てず、エラーで応える(NoReply に見せない) ---
    let mismatched = other
        .caller::<AddV2>()
        .to("srv")
        .timeout(Duration::from_secs(2))
        .build();
    assert!(matches!(
        mismatched.call(AddV2 { a: 1, b: 1 }).await,
        Err(CallError::Remote(m)) if m.contains("fingerprint")
    ));

    // --- watch_servers: Joined → (server 終了) → Left ---
    let mut watch = client.watch_servers::<Add>().expect("watch_servers");
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("join event"),
        Some(PresenceEvent::Joined("srv".to_string()))
    );
    server.shutdown_now();
    server_task.await.expect("server task");
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("leave event"),
        Some(PresenceEvent::Left("srv".to_string()))
    );
    assert!(client.servers::<Add>().await.expect("servers").is_empty());

    // --- 同じ型を latched publish しつつ serve する grain ---
    let cfg_pub = other
        .publisher::<Cfg>()
        .latched()
        .build()
        .expect("latched publisher");
    cfg_pub.send(Cfg { rev: 1 }).await.expect("send");
    let mut cfg_srv = other.serve::<Cfg>().expect("serve cfg");
    let cfg_task = tokio::spawn(async move {
        while let Some(req) = cfg_srv.recv().await {
            let rev = req.value.rev + 100;
            req.reply(Cfg { rev }).await.expect("reply");
        }
    });
    tokio::time::sleep(SETTLE).await;
    // latched 購読者(payload 無しの get)には直近値だけが届き、service は黙っている。
    let mut late = client
        .subscriber::<Cfg>()
        .latched()
        .build()
        .expect("latched subscriber");
    let got = timeout(PATIENCE, late.recv())
        .await
        .expect("latched value")
        .expect("stream");
    assert_eq!(got.rev, 1);
    assert!(
        timeout(Duration::from_millis(500), late.recv())
            .await
            .is_err(),
        "service の queryable が latched の get に応えてはいけない"
    );
    // 呼び出し(payload 有り)には service だけが応え、latched の直近値は混ざらない。
    let resp = client
        .caller::<Cfg>()
        .to("cli2")
        .timeout(Duration::from_secs(2))
        .build()
        .call(Cfg { rev: 5 })
        .await
        .expect("call cfg");
    assert_eq!(resp.rev, 105, "latched の直近値(1)が応答に化けている");

    other.shutdown_now();
    cfg_task.await.expect("cfg task");
    drop(cfg_pub);
}
