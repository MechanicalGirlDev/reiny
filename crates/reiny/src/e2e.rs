//! 実際に zenoh セッションを 3 本張って、0.3 で足した 3 つ —— presence / latched /
//! domain 隔離 —— を通しで確かめる。
//!
//! `tests/` ではなく `src/` に置くのは、`Cloudy::new` が private だから(公開 API を
//! テストのために増やさない)。マルチキャスト探索は切り、ループバック TCP 1 本で
//! 決定的に繋ぐ —— CI のネットワークに依存させないため。

use std::time::Duration;

use tokio::time::timeout;

use crate::{Cloudy, PresenceEvent, Topic, shutdown::Shutdown};

/// このテスト専用の wire 型。`impl Topic` を手書きしているのは、それが
/// 「第三者が自分の型で参加できる」という reiny の売りそのものだから(回帰も兼ねる)。
#[derive(Clone, PartialEq, prost::Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

impl Topic for Probe {
    const TYPE: &'static str = "ReinyE2eProbe";
}

/// 他のテスト実行と衝突しないよう、この 1 本だけが使うループバックポート。
const ENDPOINT: &str = "tcp/127.0.0.1:37447";

/// マルチキャストを切った peer セッション。`listen` 側が 1 本、他は `connect` する。
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

fn cloudy(session: zenoh::Session, id: &str, domain: &str) -> Cloudy {
    Cloudy::new(
        session,
        id.to_string(),
        domain.to_string(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
}

const SETTLE: Duration = Duration::from_millis(600);
const PATIENCE: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn presence_latched_and_domain_isolation() {
    let alpha = cloudy(session(true).await, "alpha", "lab");
    let beta = cloudy(session(false).await, "beta", "lab");
    let gamma = cloudy(session(false).await, "gamma", "other");
    // セッション同士が繋がるまでの間。
    tokio::time::sleep(SETTLE).await;

    // --- latched: 先に 1 回だけ送っておく(定期再送はしない) ---
    let publisher = alpha
        .publisher::<Probe>()
        .latched()
        .build()
        .expect("latched publisher");
    publisher.send(Probe { seq: 7 }).await.expect("send");
    tokio::time::sleep(SETTLE).await;

    // --- 遅れて来た購読者が、その 1 回を受け取れる ---
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
    assert_eq!(envelope.source, "alpha", "送信元 id がキーから取れている");

    // --- presence: 生きている publisher が id で見える ---
    let live = beta.publishers::<Probe>().await.expect("publishers()");
    assert_eq!(live, ["alpha"]);

    // --- domain 隔離: 同じ fabric に居ても domain が違えば何も見えない ---
    let other = gamma.publishers::<Probe>().await.expect("publishers()");
    assert!(other.is_empty(), "domain 越しに見えてはいけない: {other:?}");
    let mut outsider = gamma
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("subscriber");
    assert!(
        timeout(Duration::from_millis(800), outsider.recv())
            .await
            .is_err(),
        "domain 越しに latched 値が届いてはいけない"
    );

    // --- 離脱: publisher を drop すると liveliness トークンも落ちる ---
    let mut watch = beta.watch_publishers::<Probe>().expect("watch_publishers");
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("join event"),
        Some(PresenceEvent::Joined("alpha".to_string())),
        "history=true なので宣言済みの publisher が最初に流れる"
    );
    drop(publisher);
    assert_eq!(
        timeout(PATIENCE, watch.recv()).await.expect("leave event"),
        Some(PresenceEvent::Left("alpha".to_string())),
    );

    // publisher が消えれば publishers() からも消える。
    let live = beta.publishers::<Probe>().await.expect("publishers()");
    assert!(live.is_empty(), "drop 後も残っている: {live:?}");
}
