//! Three real zenoh sessions, exercising the three things 0.3 added — presence / latched / domain
//! isolation — end to end.
//!
//! They live in `src/` rather than `tests/` because `Cloudy::new` is private (growing the public API
//! for a test's sake would be the wrong trade). Multicast scouting is off and the sessions are wired
//! deterministically over one loopback TCP link, so nothing depends on CI's network.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use zenoh::Wait;

use crate::engine::Zenoh;
use crate::{Cloudy, Descriptor, History, PresenceEvent, Qos, Topic, shutdown::Shutdown};

/// A wire type just for these tests. `impl Topic` is hand-written because that is precisely reiny's
/// promise — "a third party joins with their own type" — so this doubles as a regression test for it.
#[derive(Clone, PartialEq, prost::Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

impl Topic for Probe {
    const TYPE: &'static str = "ReinyE2eProbe";
}

/// The same type on the same topic but with **different schema fingerprints**. It recreates by hand
/// what `reiny-build` does when another project has a same-named type.
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
    const TYPE: &'static str = "ReinyE2eStamped";
    const SCHEMA: Option<u64> = Some(0x1111_1111_1111_1111);
}

impl Topic for StampedV2 {
    const TYPE: &'static str = "ReinyE2eStamped";
    const SCHEMA: Option<u64> = Some(0x2222_2222_2222_2222);
}

/// A type that announces a descriptor. The bytes need not be a real descriptor set — reiny never
/// interprets them, it just serves them **verbatim** at `@schema` (interpreting is `reiny bag`'s job).
#[derive(Clone, PartialEq, prost::Message)]
struct Described {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

impl Topic for Described {
    const TYPE: &'static str = "ReinyE2eDescribed";
    const DESCRIPTOR: Option<Descriptor> = Some(Descriptor {
        message: "e2e.Described",
        file_set: b"not-a-real-descriptor-set",
    });
}

/// The loopback port. These tests **run in parallel**, so every test needs its own
/// (`rpc_e2e.rs` uses 37449 — overlapping makes a listen fail and takes another test down with it).
const ENDPOINT: &str = "tcp/127.0.0.1:37447";
const ISLAND_A: &str = "tcp/127.0.0.1:37451";
const ISLAND_B: &str = "tcp/127.0.0.1:37452";

/// A peer session with multicast off. One side `listen`s; the others `connect`.
async fn session(listen: bool) -> zenoh::Session {
    let key = if listen {
        "listen/endpoints"
    } else {
        "connect/endpoints"
    };
    open_session(&[(key, format!("[\"{ENDPOINT}\"]"))]).await
}

/// A peer session with multicast off, opened with extra configuration layered on.
async fn open_session(entries: &[(&str, String)]) -> zenoh::Session {
    let mut config = zenoh::Config::default();
    for (k, v) in [("scouting/multicast/enabled", "false".to_string())]
        .iter()
        .chain(entries)
    {
        config
            .insert_json5(k, v)
            .unwrap_or_else(|e| panic!("zenoh config {k}: {e}"));
    }
    zenoh::open(config)
        .await
        .unwrap_or_else(|e| panic!("opening zenoh session: {e}"))
}

async fn cloudy(session: zenoh::Session, id: &str, domain: &str) -> Cloudy {
    Cloudy::new(
        Arc::new(Zenoh::from_session(session)),
        id.to_string(),
        domain.to_string(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await
    .expect("cloudy")
}

const SETTLE: Duration = Duration::from_millis(600);
const PATIENCE: Duration = Duration::from_secs(5);

/// Wait until the listening session has `peers` links. A fixed sleep is not enough under a loaded
/// parallel run — a get or a declaration fired before the link exists never reaches the other side.
pub(crate) async fn wait_peers(session: &zenoh::Session, peers: usize) {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let linked = session.info().peers_zid().await.count();
        if linked >= peers {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {linked}/{peers} peers linked within patience"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn presence_latched_and_domain_isolation() {
    let alpha = cloudy(session(true).await, "alpha", "lab").await;
    let beta = cloudy(session(false).await, "beta", "lab").await;
    let gamma = cloudy(session(false).await, "gamma", "other").await;
    // Until the sessions are connected (beta / gamma attached to alpha).
    wait_peers(alpha.session().expect("zenoh engine"), 2).await;

    // --- Qos: a publisher's KeepLast(n > 1) is stopped at build (not silently rounded to 1) ---
    let err = alpha
        .publisher::<Probe>()
        .qos(Qos {
            history: History::KeepLast(3),
            ..Qos::DEFAULT
        })
        .build()
        .err()
        .expect("KeepLast(3) must be rejected");
    assert!(err.to_string().contains("KeepLast(3)"), "{err}");

    // --- latched (= `Qos::STATE`): send once, up front, and never re-send ---
    let publisher = alpha
        .publisher::<Probe>()
        .qos(Qos::STATE)
        .build()
        .expect("latched publisher");
    publisher.send(Probe { seq: 7 }).await.expect("send");
    tokio::time::sleep(SETTLE).await;

    // --- a late subscriber still receives that one sample ---
    let mut late = beta
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("latched subscriber");
    let envelope = timeout(PATIENCE, late.recv_envelope())
        .await
        .expect("latched value should arrive")
        .expect("stream should not end");
    assert_eq!(envelope.value.seq, 7);
    assert_eq!(envelope.source, "alpha", "the source id comes off the key");

    // --- presence: a live publisher is visible by id ---
    let live = beta.publishers::<Probe>().await.expect("publishers()");
    assert_eq!(live, ["alpha"]);

    // --- domain isolation: on the same fabric, a different domain sees nothing ---
    let other = gamma.publishers::<Probe>().await.expect("publishers()");
    assert!(
        other.is_empty(),
        "must not be visible across domains: {other:?}"
    );
    let mut outsider = gamma
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("subscriber");
    assert!(
        timeout(Duration::from_millis(800), outsider.recv())
            .await
            .is_err(),
        "a latched value must not cross domains"
    );

    // --- leaving: dropping the publisher drops the liveliness token too ---
    let mut watch = beta.watch_publishers::<Probe>().expect("watch_publishers");
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("join event"),
        Some(PresenceEvent::Joined("alpha".to_string())),
        "history=true, so an already-declared publisher arrives first"
    );
    drop(publisher);
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("leave event"),
        Some(PresenceEvent::Left("alpha".to_string())),
    );

    // Once the publisher is gone it leaves publishers() too.
    let live = beta.publishers::<Probe>().await.expect("publishers()");
    assert!(live.is_empty(), "still there after the drop: {live:?}");

    // --- schema fingerprints: same topic, different shape, nothing arrives ---
    let stamped = alpha
        .publisher::<StampedV1>()
        .build()
        .expect("stamped publisher");
    let mut same = beta
        .subscriber::<StampedV1>()
        .build()
        .expect("matching subscriber");
    let mut different = beta
        .subscriber::<StampedV2>()
        .build()
        .expect("mismatching subscriber");
    tokio::time::sleep(SETTLE).await;
    stamped.send(StampedV1 { seq: 42 }).await.expect("send");

    let got = timeout(PATIENCE, same.recv())
        .await
        .expect("the same fingerprint arrives")
        .expect("stream should not end");
    assert_eq!(got.seq, 42);
    assert!(
        timeout(Duration::from_millis(800), different.recv())
            .await
            .is_err(),
        "a sample with a different fingerprint must be dropped (protobuf is permissive: it would decode)"
    );

    // --- @schema: a publisher with a DESCRIPTOR announces it beside its own key ---
    let described = alpha
        .publisher::<Described>()
        .build()
        .expect("described publisher");
    tokio::time::sleep(SETTLE).await;
    let replies = beta
        .session()
        .expect("zenoh engine")
        .get("reiny/lab/*/*/@schema/*")
        .wait()
        .expect("schema get");
    let named: Vec<(String, Vec<u8>)> = replies
        .iter()
        .filter_map(|r| {
            r.result()
                .ok()
                .map(|s| (s.key_expr().to_string(), s.payload().to_bytes().to_vec()))
        })
        .collect();
    assert_eq!(
        named,
        [(
            "reiny/lab/alpha/ReinyE2eDescribed/@schema/e2e.Described".to_string(),
            b"not-a-real-descriptor-set".to_vec()
        )]
    );
    // verbatim: querying the whole domain with `**` still does not pick up @schema (the latched
    // queryable answers that same get, which is what makes "invisible" demonstrable).
    let broad = beta
        .session()
        .expect("zenoh engine")
        .get("reiny/lab/**")
        .wait()
        .expect("broad get");
    let leaked: Vec<String> = broad
        .iter()
        .filter_map(|r| r.result().ok().map(|s| s.key_expr().to_string()))
        .filter(|k| k.contains("@schema"))
        .collect();
    assert!(leaked.is_empty(), "@schema is visible to **: {leaked:?}");
    drop(described);

    // --- latest(n): letting samples pile up unread drops the oldest (the default Fifo keeps all 5) ---
    let burst = alpha.publisher::<Probe>().build().expect("burst publisher");
    let mut ring = beta
        .subscriber::<Probe>()
        .latest(2)
        .build()
        .expect("ring subscriber");
    let mut fifo = beta.subscriber::<Probe>().build().expect("fifo subscriber");
    tokio::time::sleep(SETTLE).await;
    for seq in 1..=5 {
        burst.send(Probe { seq }).await.expect("send");
    }
    tokio::time::sleep(SETTLE).await;
    let mut kept = Vec::new();
    while let Ok(Some(m)) = timeout(Duration::from_millis(300), ring.recv()).await {
        kept.push(m.seq);
    }
    assert_eq!(kept, [4, 5], "latest(2) returns the two newest, in order");
    let mut all = Vec::new();
    while let Ok(Some(m)) = timeout(Duration::from_millis(300), fifo.recv()).await {
        all.push(m.seq);
    }
    assert_eq!(all, [1, 2, 3, 4, 5], "the default Fifo drops nothing");

    // --- cancel-safety: dropping recv on a timeout over and over still yields the sample that arrived ---
    for sub in [&mut ring, &mut fifo] {
        for _ in 0..3 {
            assert!(
                timeout(Duration::from_millis(50), sub.recv())
                    .await
                    .is_err(),
                "nothing was published, so it must expire"
            );
        }
    }
    burst.send(Probe { seq: 99 }).await.expect("send");
    tokio::time::sleep(SETTLE).await;
    for sub in [&mut ring, &mut fifo] {
        // After it arrives, provoke a "start taking it out, then drop" with an already-expired recv.
        let got = match timeout(Duration::ZERO, sub.recv()).await {
            Ok(v) => v,
            Err(_) => timeout(PATIENCE, sub.recv())
                .await
                .expect("the sample survives a cancelled recv"),
        };
        assert_eq!(got.map(|m| m.seq), Some(99));
    }
    drop(burst);
}

/// A latched publisher on the far side of a link that **comes up later**. 0.3.0 hung here forever: a
/// `get` only sees the routing table of the instant it is fired, so it never reached the queryable of a
/// peer that was not connected yet, and since the publisher does not re-send, the value never came
/// (in the field, a bag recorder joined as a fourth peer and physics connected to it first, hitting
///
/// Here the publisher and the subscriber are brought up **isolated from each other** (each only
/// listens, neither connects); the send and the subscription are done first, and only then does a
/// third session connecting to both create the link. The live path can no longer carry anything, so
/// only the "ask again on presence" path can deliver the value.
#[tokio::test(flavor = "multi_thread")]
async fn latched_survives_a_link_that_comes_up_late() {
    // 1) The publisher side: isolated, sends its latched value exactly once.
    let alpha = cloudy(
        open_session(&[("listen/endpoints", format!("[\"{ISLAND_A}\"]"))]).await,
        "alpha",
        "lab",
    )
    .await;
    let publisher = alpha
        .publisher::<Probe>()
        .latched()
        .build()
        .expect("latched publisher");
    publisher.send(Probe { seq: 11 }).await.expect("send");

    // 2) The subscriber side: connected to nobody, so a query at this point is bound to miss.
    let beta = cloudy(
        open_session(&[("listen/endpoints", format!("[\"{ISLAND_B}\"]"))]).await,
        "beta",
        "lab",
    )
    .await;
    let mut sub = beta
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("latched subscriber");

    // 3) The third session, connected to both. Only now does alpha's declaration reach beta.
    let _bridge = open_session(&[(
        "connect/endpoints",
        format!("[\"{ISLAND_A}\", \"{ISLAND_B}\"]"),
    )])
    .await;

    let envelope = timeout(PATIENCE, sub.recv_envelope())
        .await
        .expect("the latched value arrives once the link is up")
        .expect("stream should not end");
    assert_eq!(envelope.value.seq, 11);
    assert_eq!(envelope.source, "alpha");
}
