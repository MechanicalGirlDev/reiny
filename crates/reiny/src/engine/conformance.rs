//! The `Engine` conformance test — one function, run against every engine.
//!
//! It pins that the five operations plus the logic above them (latest(n) / cancel-safety / latched /
//! a service round trip / presence / fingerprints) look the same whichever engine is underneath.
//! Someone adding an engine only has to pass this one: it is public behind the `conformance` feature,
//! so another crate's tests can call `reiny::engine::conformance::{cloudy, exercise}`. `Local` runs
//! in-process, `Zenoh` on loopback 37453 (apart from `e2e.rs`'s 37447 and `rpc_e2e.rs`'s 37449).

use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use super::{Engine, Key};
use crate::{CallError, Cloudy, PresenceEvent, Qos, Service, Topic, shutdown::Shutdown};

#[derive(Clone, PartialEq, prost::Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

impl Topic for Probe {
    const TYPE: &'static str = "ConfProbe";
}

#[derive(Clone, PartialEq, prost::Message)]
struct StampedV1 {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StampedV2 {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

impl Topic for StampedV1 {
    const TYPE: &'static str = "ConfStamped";
    const SCHEMA: Option<u64> = Some(0x1111);
}

impl Topic for StampedV2 {
    const TYPE: &'static str = "ConfStamped";
    const SCHEMA: Option<u64> = Some(0x2222);
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
    const TYPE: &'static str = "ConfAdd";
}

impl Topic for Sum {
    const TYPE: &'static str = "ConfSum";
}

impl Service for Add {
    type Response = Sum;
}

const DOMAIN: &str = "conf";
const SETTLE: Duration = Duration::from_millis(600);
const PATIENCE: Duration = Duration::from_secs(5);

/// Build the launch `id` on top of `engine` (in the domain `conf`).
pub async fn cloudy(engine: Arc<dyn Engine>, id: &str) -> Cloudy {
    Cloudy::new(
        engine,
        id.to_string(),
        DOMAIN.to_string(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await
    .expect("cloudy")
}

/// The conformance body. `a` / `b` are two launches (ids `a` / `b`) on the same bus. It panics on failure.
#[allow(clippy::too_many_lines)] // one pass end to end (run once per engine), hence long
pub async fn exercise(a: Cloudy, b: Cloudy) {
    tokio::time::sleep(SETTLE).await;

    // --- launch presence: Cloudy::new raised @launch ---
    let launches = a
        .engine()
        .alive(&Key::launch(DOMAIN, None), PATIENCE)
        .await
        .expect("alive");
    let mut ids: Vec<String> = launches.into_iter().filter_map(|k| k.source).collect();
    ids.sort();
    assert_eq!(ids, ["a", "b"]);

    // --- publish → subscribe, with the source attached ---
    let probe = a.publish::<Probe>().expect("publisher");
    let mut plain = b.subscribe::<Probe>().expect("subscriber");
    tokio::time::sleep(SETTLE).await;
    probe.send(Probe { seq: 1 }).await.expect("send");
    let envelope = timeout(PATIENCE, plain.recv_envelope())
        .await
        .expect("sample within patience")
        .expect("stream open");
    assert_eq!(envelope.value.seq, 1);
    assert_eq!(envelope.source, "a");

    // --- cancel-safety: a recv dropped half-way loses no later sample ---
    assert!(
        timeout(Duration::from_millis(200), plain.recv())
            .await
            .is_err()
    );
    probe.send(Probe { seq: 2 }).await.expect("send");
    assert_eq!(
        timeout(PATIENCE, plain.recv())
            .await
            .expect("sample")
            .map(|p| p.seq),
        Some(2)
    );

    // --- latest(2): the oldest is dropped on overflow ---
    let mut ring = b.subscriber::<Probe>().latest(2).build().expect("ring");
    tokio::time::sleep(SETTLE).await;
    for seq in 10..15 {
        probe.send(Probe { seq }).await.expect("send");
    }
    tokio::time::sleep(SETTLE).await;
    assert_eq!(ring.recv().await.map(|p| p.seq), Some(13));
    assert_eq!(ring.recv().await.map(|p| p.seq), Some(14));

    // --- and it says so: dropping the oldest is silent apart from these counters ---
    let ring_stats = ring.stats();
    assert_eq!(
        (ring_stats.received, ring_stats.dropped, ring_stats.blocked),
        (5, 3, 0),
        "5 samples into a ring of 2"
    );

    // --- subscriber presence (`@sub`): who is listening, publisher or no publisher ---
    // `Sum` has neither a publisher nor another subscriber here, so the answer is unambiguous.
    assert!(
        a.subscribers::<Sum>()
            .await
            .expect("subscribers")
            .is_empty()
    );
    let mut watch_subs = a.watch_subscribers::<Sum>().expect("watch subscribers");
    let listener = b.subscribe::<Sum>().expect("subscriber");
    assert_eq!(
        timeout(PATIENCE, watch_subs.recv()).await.expect("join"),
        Some(PresenceEvent::Joined("b".to_string()))
    );
    assert_eq!(a.subscribers::<Sum>().await.expect("subscribers"), ["b"]);
    // A listener is not a publisher: `@sub` must not leak into the type's own key.
    assert!(a.publishers::<Sum>().await.expect("publishers").is_empty());
    drop(listener);
    assert_eq!(
        timeout(PATIENCE, watch_subs.recv()).await.expect("leave"),
        Some(PresenceEvent::Left("b".to_string()))
    );

    // --- presence: publishers() and watch (history → Joined, drop → Left) ---
    assert_eq!(b.publishers::<Probe>().await.expect("publishers"), ["a"]);
    let mut watch = b.watch_publishers::<Probe>().expect("watch");
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("join"),
        Some(PresenceEvent::Joined("a".to_string()))
    );
    drop(probe);
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("leave"),
        Some(PresenceEvent::Left("a".to_string()))
    );
    assert!(
        b.publishers::<Probe>()
            .await
            .expect("publishers")
            .is_empty()
    );

    // --- latched (Qos::STATE): a late subscriber receives the most recent value ---
    let state = a
        .publisher::<Probe>()
        .qos(Qos::STATE)
        .build()
        .expect("state publisher");
    state.send(Probe { seq: 42 }).await.expect("send");
    tokio::time::sleep(SETTLE).await;
    let mut late = b
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("latched subscriber");
    let envelope = timeout(PATIENCE, late.recv_envelope())
        .await
        .expect("latched value within patience")
        .expect("stream open");
    assert_eq!((envelope.value.seq, envelope.source.as_str()), (42, "a"));

    // --- fingerprints: same topic, different shape, nothing arrives ---
    let stamped = a.publish::<StampedV1>().expect("stamped publisher");
    let mut same = b.subscribe::<StampedV1>().expect("same");
    let mut different = b.subscribe::<StampedV2>().expect("different");
    tokio::time::sleep(SETTLE).await;
    stamped.send(StampedV1 { seq: 7 }).await.expect("send");
    assert_eq!(
        timeout(PATIENCE, same.recv())
            .await
            .expect("same")
            .map(|s| s.seq),
        Some(7)
    );
    assert!(
        timeout(Duration::from_millis(500), different.recv())
            .await
            .is_err(),
        "mismatching fingerprint must be dropped"
    );

    // --- services: round trip / reply_err / a destination that is not there / dropped / held → Timeout ---
    let mut server = a.serve::<Add>().expect("serve");
    let server_task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Some(req) = server.recv().await {
            let Add { a, b } = req.value;
            match b {
                b if b < 0 => req.reply_err("negative b").await.expect("reply_err"),
                0 => drop(req),
                7 => held.push(req),
                _ => req.reply(Sum { sum: a + b }).await.expect("reply"),
            }
        }
    });
    tokio::time::sleep(SETTLE).await;
    assert_eq!(b.servers::<Add>().await.expect("servers"), ["a"]);
    let sum = timeout(PATIENCE, b.call::<Add>(Add { a: 2, b: 3 }))
        .await
        .expect("call returns")
        .expect("call");
    assert_eq!(sum.sum, 5);
    let caller = b
        .caller::<Add>()
        .to("a")
        .timeout(Duration::from_millis(500))
        .build();
    assert!(matches!(
        caller.call(Add { a: 1, b: -1 }).await,
        Err(CallError::Remote(m)) if m == "negative b"
    ));
    assert!(matches!(
        caller.call(Add { a: 1, b: 0 }).await,
        Err(CallError::NoReply)
    ));
    assert!(matches!(
        caller.call(Add { a: 1, b: 7 }).await,
        Err(CallError::Timeout)
    ));
    let nobody = b
        .caller::<Add>()
        .to("ghost")
        .timeout(Duration::from_millis(500))
        .build();
    assert!(matches!(
        nobody.call(Add { a: 1, b: 1 }).await,
        Err(CallError::NoReply)
    ));

    // --- shutdown: recv returns None and the loop falls out ---
    b.shutdown_now();
    assert_eq!(timeout(PATIENCE, plain.recv()).await.expect("recv"), None);
    a.shutdown_now();
    timeout(PATIENCE, server_task)
        .await
        .expect("server task ends on shutdown")
        .expect("server task");
}

#[cfg(test)]
#[tokio::test(flavor = "multi_thread")]
async fn local() {
    let bus = super::Local::new();
    let a = cloudy(Arc::new(bus.clone()), "a").await;
    let b = cloudy(Arc::new(bus), "b").await;
    exercise(a, b).await;
}

#[cfg(all(test, feature = "zenoh"))]
#[tokio::test(flavor = "multi_thread")]
async fn zenoh() {
    use super::Zenoh;

    const ENDPOINT: &str = "tcp/127.0.0.1:37453";

    async fn session(listen: bool) -> zenoh::Session {
        let mut config = zenoh::Config::default();
        let key = if listen {
            "listen/endpoints"
        } else {
            "connect/endpoints"
        };
        for (k, v) in [
            ("scouting/multicast/enabled", "false".to_string()),
            (key, format!("[\"{ENDPOINT}\"]")),
        ] {
            config
                .insert_json5(k, &v)
                .unwrap_or_else(|e| panic!("zenoh config {k}: {e}"));
        }
        zenoh::open(config)
            .await
            .unwrap_or_else(|e| panic!("opening zenoh session: {e}"))
    }

    let a = cloudy(Arc::new(Zenoh::from_session(session(true).await)), "a").await;
    let b = cloudy(Arc::new(Zenoh::from_session(session(false).await)), "b").await;
    crate::e2e::wait_peers(a.session().expect("zenoh engine"), 1).await;
    exercise(a, b).await;
}
