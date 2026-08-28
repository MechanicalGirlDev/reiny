//! 実際に zenoh セッションを 3 本張って、0.3 で足した 3 つ —— presence / latched /
//! domain 隔離 —— を通しで確かめる。
//!
//! `tests/` ではなく `src/` に置くのは、`Cloudy::new` が private だから(公開 API を
//! テストのために増やさない)。マルチキャスト探索は切り、ループバック TCP 1 本で
//! 決定的に繋ぐ —— CI のネットワークに依存させないため。

use std::time::Duration;

use tokio::time::timeout;

use zenoh::Wait;

use crate::{Cloudy, Descriptor, PresenceEvent, Topic, shutdown::Shutdown};

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

/// 同じ型・同じトピックだが **スキーマ指紋が違う** 2 つ。`reiny-build` が別プロジェクトの
/// 同名型に別の指紋を振る状況を、手書きで再現している。
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

/// descriptor を名乗る型。中身は本物の descriptor set でなくてよい —— reiny はバイト列を
/// 解釈せず、`@schema` で**そのまま**返すだけだから(解釈するのは `reiny bag` 側)。
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

    // --- スキーマ指紋: 同じトピックでも形が違えば届かない ---
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
        .expect("同じ指紋なら届く")
        .expect("stream should not end");
    assert_eq!(got.seq, 42);
    assert!(
        timeout(Duration::from_millis(800), different.recv())
            .await
            .is_err(),
        "指紋が違うサンプルは捨てられるべき(protobuf は寛容なので decode は通ってしまう)"
    );

    // --- @schema: DESCRIPTOR を持つ publisher は自分のキーの脇で descriptor を名乗る ---
    let described = alpha
        .publisher::<Described>()
        .build()
        .expect("described publisher");
    tokio::time::sleep(SETTLE).await;
    let replies = beta
        .session()
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
    // verbatim: `**` で domain 全体を問い合わせても @schema は混ざらない(latched の
    // queryable は同じ get に応えるので、これが「見えない」ことの実証になる)。
    let broad = beta
        .session()
        .get("reiny/lab/**")
        .wait()
        .expect("broad get");
    let leaked: Vec<String> = broad
        .iter()
        .filter_map(|r| r.result().ok().map(|s| s.key_expr().to_string()))
        .filter(|k| k.contains("@schema"))
        .collect();
    assert!(leaked.is_empty(), "@schema が ** に見えている: {leaked:?}");
    drop(described);

    // --- latest(n): 読まずに溜めると最古から落ちる(既定の Fifo なら 5 件とも残る) ---
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
    assert_eq!(kept, [4, 5], "latest(2) は最新 2 件だけを順に返す");
    let mut all = Vec::new();
    while let Ok(Some(m)) = timeout(Duration::from_millis(300), fifo.recv()).await {
        all.push(m.seq);
    }
    assert_eq!(all, [1, 2, 3, 4, 5], "既定の Fifo は落とさない");

    // --- cancel-safety: timeout で recv を捨て続けても、届いた sample は次の recv が返す ---
    for sub in [&mut ring, &mut fifo] {
        for _ in 0..3 {
            assert!(
                timeout(Duration::from_millis(50), sub.recv())
                    .await
                    .is_err(),
                "何も流れていないので期限切れのはず"
            );
        }
    }
    burst.send(Probe { seq: 99 }).await.expect("send");
    tokio::time::sleep(SETTLE).await;
    for sub in [&mut ring, &mut fifo] {
        // 届いた後に、即時期限切れの recv で「取り出しかけて捨てる」を起こしてから読む。
        let got = match timeout(Duration::ZERO, sub.recv()).await {
            Ok(v) => v,
            Err(_) => timeout(PATIENCE, sub.recv())
                .await
                .expect("cancel された recv の後でも sample は残っている"),
        };
        assert_eq!(got.map(|m| m.seq), Some(99));
    }
    drop(burst);
}
