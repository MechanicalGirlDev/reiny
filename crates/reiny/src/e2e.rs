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

use crate::engine::{
    BoxFuture, Callback, Caps, Engine, Guard, Key, Presence, QueryCallback, QueryParams,
    RawPublisher, RawReplies, Sample, Zenoh,
};
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
/// The rendezvous point of the late-link test. Both sides retry-connect to it; it is opened only
/// after both have declared, so the link is genuinely late.
const LATE_LINK_HUB: &str = "tcp/127.0.0.1:37451";

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

const PATIENCE: Duration = Duration::from_secs(5);

/// All callers have already constructed their Cloudy instances. History subscribes atomically to
/// their launch-token arrivals, including the local launch, instead of polling transport counts.
pub(crate) async fn wait_peers(session: &zenoh::Session, peers: usize) {
    let launches = session
        .liveliness()
        .declare_subscriber("reiny/**/@launch")
        .history(true)
        .wait()
        .expect("launch readiness observer");
    timeout(PATIENCE, async {
        let mut seen = std::collections::HashSet::new();
        while seen.len() < peers + 1 {
            let event = launches
                .recv_async()
                .await
                .expect("launch observer remains open");
            if event.kind() == zenoh::sample::SampleKind::Put {
                seen.insert(event.key_expr().to_string());
            }
        }
    })
    .await
    .expect("all peer launch declarations reach this session");
}

fn subscriber_signal(session: &zenoh::Session, key: String) -> (Guard, flume::Receiver<()>) {
    let observer = session.declare_publisher(key).wait().expect("observer");
    let (tx, rx) = flume::unbounded();
    let listener = observer
        .matching_listener()
        .callback(move |status| {
            if status.matching() {
                let _ = tx.send(());
            }
        })
        .wait()
        .expect("matching listener");
    (Box::new((observer, listener)), rx)
}

fn queryable_signal(session: &zenoh::Session, key: String) -> (Guard, flume::Receiver<()>) {
    let observer = session.declare_querier(key).wait().expect("observer");
    let (tx, rx) = flume::unbounded();
    let listener = observer
        .matching_listener()
        .callback(move |status| {
            if status.matching() {
                let _ = tx.send(());
            }
        })
        .wait()
        .expect("matching listener");
    (Box::new((observer, listener)), rx)
}

async fn matched_signal(matched: &flume::Receiver<()>) {
    timeout(PATIENCE, matched.recv_async())
        .await
        .expect("remote declaration reaches the observer")
        .expect("matching listener remains open");
}

/// Observe completion of the real SDK buffer callback, not just a parallel native subscription.
/// In particular, a ring must have received the entire burst before we start draining it.
struct Observed {
    zenoh: Zenoh,
    buffered: flume::Sender<Sample>,
}

impl Engine for Observed {
    fn caps(&self) -> Caps {
        self.zenoh.caps()
    }

    fn publisher(&self, key: &Key, qos: &Qos) -> crate::Result<Box<dyn RawPublisher>> {
        self.zenoh.publisher(key, qos)
    }

    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> crate::Result<Guard> {
        let buffered = self.buffered.clone();
        self.zenoh.subscribe(
            key,
            Box::new(move |sample| {
                on_sample(sample.clone());
                let _ = buffered.send(sample);
            }),
        )
    }

    fn declare_alive(&self, key: &Key) -> crate::Result<Guard> {
        self.zenoh.declare_alive(key)
    }

    fn alive(&self, key: &Key, duration: Duration) -> BoxFuture<'_, crate::Result<Vec<Key>>> {
        self.zenoh.alive(key, duration)
    }

    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> crate::Result<Guard> {
        self.zenoh.watch_alive(key, on_event)
    }

    fn respond(&self, key: &Key, on_query: QueryCallback) -> crate::Result<Guard> {
        self.zenoh.respond(key, on_query)
    }

    fn query(&self, key: &Key, params: QueryParams) -> crate::Result<Box<dyn RawReplies>> {
        self.zenoh.query(key, params)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.zenoh.as_any()
    }
}

async fn buffered_samples(buffered: &flume::Receiver<Sample>, ty: &str, count: usize) {
    timeout(PATIENCE, async {
        for _ in 0..count {
            let sample = buffered
                .recv_async()
                .await
                .expect("buffer observer remains open");
            assert_eq!(sample.key.ty.as_deref(), Some(ty));
        }
    })
    .await
    .expect("all samples reach the SDK buffers");
}

#[tokio::test(flavor = "multi_thread")]
async fn presence_latched_and_domain_isolation() {
    let alpha = cloudy(session(true).await, "alpha", "lab").await;
    let (buffered_tx, buffered) = flume::unbounded();
    let beta = Cloudy::new(
        Arc::new(Observed {
            zenoh: Zenoh::from_session(session(false).await),
            buffered: buffered_tx,
        }),
        "beta".to_string(),
        "lab".to_string(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await
    .expect("cloudy");
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
    let (_latched_observer, latched_ready) = queryable_signal(
        beta.session().unwrap(),
        "reiny/lab/alpha/ReinyE2eProbe".to_string(),
    );
    let publisher = alpha
        .publisher::<Probe>()
        .qos(Qos::STATE)
        .build()
        .expect("latched publisher");
    publisher.send(Probe { seq: 7 }).await.expect("send");
    matched_signal(&latched_ready).await;

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
    drop(late);

    // --- presence: a live publisher is visible by id ---
    let live = beta.publishers::<Probe>().await.expect("publishers()");
    assert_eq!(live, ["alpha"]);

    domain_isolation(&alpha, &gamma, &publisher).await;

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

    schema_fingerprints(&alpha, &beta, &buffered).await;
    schema_discovery(&alpha, &beta).await;
    receive_buffers(&alpha, &beta, &buffered).await;
}

async fn domain_isolation(alpha: &Cloudy, gamma: &Cloudy, publisher: &crate::Publisher<Probe>) {
    // --- domain isolation: on the same fabric, a different domain sees nothing ---
    let other = gamma.publishers::<Probe>().await.expect("publishers()");
    assert!(
        other.is_empty(),
        "must not be visible across domains: {other:?}"
    );
    let (_isolation_observer, isolated_ready) = subscriber_signal(
        alpha.session().unwrap(),
        "reiny/other/delta/ReinyE2eProbe".to_string(),
    );
    let mut outsider = gamma
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("subscriber");
    matched_signal(&isolated_ready).await;
    // Use the same physical sender for both domains, with an ordered valid sentinel.
    let delta = cloudy(alpha.session().unwrap().clone(), "delta", "other").await;
    let sentinel = delta
        .publisher::<Probe>()
        .latched()
        .build()
        .expect("sentinel");
    publisher.send(Probe { seq: 8 }).await.expect("lab sample");
    sentinel
        .send(Probe { seq: 9 })
        .await
        .expect("other-domain sentinel");
    let isolated = timeout(PATIENCE, outsider.recv_envelope())
        .await
        .expect("other-domain delivery")
        .expect("open outsider");
    assert_eq!((isolated.value.seq, isolated.source.as_str()), (9, "delta"));
}

async fn schema_fingerprints(alpha: &Cloudy, beta: &Cloudy, buffered: &flume::Receiver<Sample>) {
    // --- schema fingerprints: same topic, different shape, nothing arrives ---
    let (_stamped_observer, stamped_ready) = subscriber_signal(
        alpha.session().unwrap(),
        "reiny/lab/alpha/ReinyE2eStamped".to_string(),
    );
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
    matched_signal(&stamped_ready).await;
    stamped.send(StampedV1 { seq: 42 }).await.expect("send");
    buffered_samples(buffered, StampedV1::TYPE, 2).await;

    let got = timeout(PATIENCE, same.recv())
        .await
        .expect("the same fingerprint arrives")
        .expect("stream should not end");
    assert_eq!(got.seq, 42);
    // Both real SDK callbacks completed above. Polling once must consume the
    // mismatching frame without producing a value, rather than waiting for silence.
    let mut rejected = std::pin::pin!(different.recv());
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(rejected.as_mut(), cx).is_pending(),
            "a mismatching fingerprint must not decode"
        );
        std::task::Poll::Ready(())
    })
    .await;
}

async fn schema_discovery(alpha: &Cloudy, beta: &Cloudy) {
    // --- @schema: a publisher with a DESCRIPTOR announces it beside its own key ---
    let (_schema_observer, schema_ready) = queryable_signal(
        beta.session().unwrap(),
        "reiny/lab/alpha/ReinyE2eDescribed/@schema/e2e.Described".to_string(),
    );
    let described = alpha
        .publisher::<Described>()
        .build()
        .expect("described publisher");
    matched_signal(&schema_ready).await;
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
}

async fn receive_buffers(alpha: &Cloudy, beta: &Cloudy, buffered: &flume::Receiver<Sample>) {
    // --- latest(n): letting samples pile up unread drops the oldest (the default Fifo keeps all 5) ---
    let (_burst_observer, burst_ready) = subscriber_signal(
        alpha.session().unwrap(),
        "reiny/lab/alpha/ReinyE2eProbe".to_string(),
    );
    let burst = alpha.publisher::<Probe>().build().expect("burst publisher");
    let mut ring = beta
        .subscriber::<Probe>()
        .latest(2)
        .build()
        .expect("ring subscriber");
    let mut fifo = beta.subscriber::<Probe>().build().expect("fifo subscriber");
    matched_signal(&burst_ready).await;
    for seq in 1..=5 {
        burst.send(Probe { seq }).await.expect("send");
    }
    buffered_samples(buffered, Probe::TYPE, 10).await;
    assert_eq!(ring.stats().received, 5);
    assert_eq!(ring.stats().dropped, 3);
    assert_eq!(fifo.stats().received, 5);
    let mut kept = Vec::new();
    for _ in 0..2 {
        let m = timeout(PATIENCE, ring.recv())
            .await
            .expect("ring sample")
            .expect("open");
        kept.push(m.seq);
    }
    assert_eq!(kept, [4, 5], "latest(2) returns the two newest, in order");
    let mut all = Vec::new();
    for _ in 0..5 {
        let m = timeout(PATIENCE, fifo.recv())
            .await
            .expect("fifo sample")
            .expect("open");
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
    buffered_samples(buffered, Probe::TYPE, 2).await;
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
/// Here the publisher and the subscriber are brought up **isolated from each other** (both retry a
/// hub that is not up yet); the send and the subscription are done first, and only then is the hub
/// opened and the link created. The live path can no longer carry anything, so only the "ask again
/// on presence" path can deliver the value.
#[tokio::test(flavor = "multi_thread")]
async fn latched_survives_a_link_that_comes_up_late() {
    // Both sides connect *out* to a hub that is not up yet: retrying in the background is what makes
    // the link late. (Bridging them with a third session used to work through gossip; since zenoh
    // 1.10 gossip advertises `get_locators_noloopback()`, so two peers listening on 127.0.0.1 never
    // learn about each other and nothing here would ever link up.)
    // The `#retry_...` suffix is zenoh's per-endpoint connect config: retry briskly so the link
    // forms well inside `PATIENCE` once the hub appears.
    let island = [(
        "connect/endpoints",
        format!("[\"{LATE_LINK_HUB}#retry_period_init_ms=50;retry_period_max_ms=200\"]"),
    )];

    // 1) The publisher side: isolated, sends its latched value exactly once.
    let alpha = cloudy(open_session(&island).await, "alpha", "lab").await;
    let publisher = alpha
        .publisher::<Probe>()
        .latched()
        .build()
        .expect("latched publisher");
    publisher.send(Probe { seq: 11 }).await.expect("send");

    // 2) The subscriber side: connected to nobody, so a query at this point is bound to miss.
    let beta = cloudy(open_session(&island).await, "beta", "lab").await;
    let mut sub = beta
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("latched subscriber");

    // 3) The hub the two have been retrying against. Only now does alpha's declaration reach beta.
    let _hub = open_session(&[("listen/endpoints", format!("[\"{LATE_LINK_HUB}\"]"))]).await;

    let envelope = timeout(PATIENCE, sub.recv_envelope())
        .await
        .expect("the latched value arrives once the link is up")
        .expect("stream should not end");
    assert_eq!(envelope.value.seq, 11);
    assert_eq!(envelope.source, "alpha");
}
