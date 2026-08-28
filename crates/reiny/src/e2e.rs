//! 実際に zenoh セッションを 3 本張って、0.3 で足した 3 つ —— presence / latched /
//! domain 隔離 —— を通しで確かめる。
//!
//! `tests/` ではなく `src/` に置くのは、`Cloudy::new` が private だから(公開 API を
//! テストのために増やさない)。マルチキャスト探索は切り、ループバック TCP 1 本で
//! 決定的に繋ぐ —— CI のネットワークに依存させないため。

use std::time::Duration;

use tokio::time::timeout;

use zenoh::Wait;

use crate::{Cloudy, Descriptor, History, PresenceEvent, Qos, Topic, shutdown::Shutdown};

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

/// ループバックポート。テストは**並行に走る**ので、テストごとに別ポートを取ること
/// (`rpc_e2e.rs` も 37449 を使っている —— 重ねると listen が失敗して別のテストが落ちる)。
const ENDPOINT: &str = "tcp/127.0.0.1:37447";
const ISLAND_A: &str = "tcp/127.0.0.1:37451";
const ISLAND_B: &str = "tcp/127.0.0.1:37452";

/// マルチキャストを切った peer セッション。`listen` 側が 1 本、他は `connect` する。
async fn session(listen: bool) -> zenoh::Session {
    let key = if listen {
        "listen/endpoints"
    } else {
        "connect/endpoints"
    };
    open_session(&[(key, format!("[\"{ENDPOINT}\"]"))]).await
}

/// マルチキャストを切った peer セッションを、追加の設定を重ねて開く。
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

fn cloudy(session: zenoh::Session, id: &str, domain: &str) -> Cloudy {
    Cloudy::new(
        session,
        id.to_string(),
        domain.to_string(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .expect("cloudy")
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

    // --- Qos: publisher の KeepLast(n > 1) は build で止まる(黙って 1 に丸めない) ---
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

    // --- latched(= `Qos::STATE`): 先に 1 回だけ送っておく(定期再送はしない) ---
    let publisher = alpha
        .publisher::<Probe>()
        .qos(Qos::STATE)
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

/// **リンクが後から張れる**先に latched publisher が居る場合。0.3.0 はここで恒久ハングした:
/// `get` は撃った瞬間のルーティング表しか見ないので、まだ繋がっていない相手の queryable には
/// 届かず、publisher は再送しないので二度と値が来なかった(実機では bag レコーダが 4 本目の
/// peer として入り、physics が先にそちらと繋がって「起動完了」した途端に踏んだ)。
///
/// ここでは publisher と購読者を**互いに孤立したまま**立ち上げ(それぞれ listen するだけで
/// connect しない)、送信も購読宣言も済ませてから、両方へ繋ぐ 3 本目のセッションで初めて
/// リンクを作る。ライブ経路はもう流れないので、presence を合図に問い合わせ直す経路だけが
/// 値を運べる。
#[tokio::test(flavor = "multi_thread")]
async fn latched_survives_a_link_that_comes_up_late() {
    // 1) publisher 側: 孤立したまま latched を 1 回だけ送る。
    let alpha = cloudy(
        open_session(&[("listen/endpoints", format!("[\"{ISLAND_A}\"]"))]).await,
        "alpha",
        "lab",
    );
    let publisher = alpha
        .publisher::<Probe>()
        .latched()
        .build()
        .expect("latched publisher");
    publisher.send(Probe { seq: 11 }).await.expect("send");

    // 2) 購読側: まだ誰とも繋がっていないので、この時点の問い合わせは必ず空振りする。
    let beta = cloudy(
        open_session(&[("listen/endpoints", format!("[\"{ISLAND_B}\"]"))]).await,
        "beta",
        "lab",
    );
    let mut sub = beta
        .subscriber::<Probe>()
        .latched()
        .build()
        .expect("latched subscriber");

    // 3) 両方へ繋ぐ 3 本目。ここで初めて alpha の宣言が beta へ届く。
    let _bridge = open_session(&[(
        "connect/endpoints",
        format!("[\"{ISLAND_A}\", \"{ISLAND_B}\"]"),
    )])
    .await;

    let envelope = timeout(PATIENCE, sub.recv_envelope())
        .await
        .expect("リンクが張れた後に latched 値が届くこと")
        .expect("stream should not end");
    assert_eq!(envelope.value.seq, 11);
    assert_eq!(envelope.source, "alpha");
}
