//! `Cloudy` をリンクの上に置く: MCU 役(素の `Host`、型を宣言)と、bridge モードの `Host` を
//! `LinkEngine` で包んだ `Cloudy` を `tokio::io::duplex` で繋ぐ。presence / Data / latched /
//! service を両方向に通す。ハードウェア無し。

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // テストは panic で失敗を表現してよい

use std::sync::Arc;
use std::time::Duration;

use reiny::{CallError, Cloudy, PresenceEvent, RuntimeOptions, Service, Topic};
use reiny_link::transport::Stream;
use reiny_link::{Host, HostEvent, HostLink, LinkEngine, wire};
use tokio::time::timeout;

#[derive(Clone, PartialEq, prost::Message)]
struct Pos {
    #[prost(int32, tag = "1")]
    x: i32,
}
impl Topic for Pos {
    const TYPE: &'static str = "EngPos";
    const SCHEMA: Option<u64> = Some(0x55);
}

#[derive(Clone, PartialEq, prost::Message)]
struct Cmd {
    #[prost(uint32, tag = "1")]
    v: u32,
}
impl Topic for Cmd {
    const TYPE: &'static str = "EngCmd";
}

#[derive(Clone, PartialEq, prost::Message)]
struct Add {
    #[prost(int32, tag = "1")]
    a: i32,
    #[prost(int32, tag = "2")]
    b: i32,
}
impl Topic for Add {
    const TYPE: &'static str = "EngAdd";
}
#[derive(Clone, PartialEq, prost::Message)]
struct Sum {
    #[prost(int32, tag = "1")]
    s: i32,
}
impl Topic for Sum {
    const TYPE: &'static str = "EngSum";
}
impl Service for Add {
    type Response = Sum;
}

/// MCU が呼び、ホストが serve する。
#[derive(Clone, PartialEq, prost::Message)]
struct Echo {
    #[prost(string, tag = "1")]
    text: String,
}
impl Topic for Echo {
    const TYPE: &'static str = "EngEcho";
}
impl Service for Echo {
    type Response = Echo;
}

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // 1 本で通す(リンクは 1 対なので分けても速くならない)。
async fn cloudy_over_a_link() {
    let (mcu_end, host_end) = tokio::io::duplex(4096);

    // MCU 役: Pos を latched で publish、Cmd を subscribe、Add を serve、Echo を呼ぶ。
    let mut mcu_link = HostLink::host("mcu").unwrap();
    mcu_link.publishes_latched::<Pos>().unwrap();
    mcu_link.subscribes::<Cmd>().unwrap();
    mcu_link.serves::<Add>().unwrap();
    mcu_link.calls::<Echo>().unwrap();
    let mcu = Arc::new(Host::spawn(mcu_link, Stream(mcu_end)));

    // ホスト役: bridge モードの Host を Engine にして Cloudy を載せる。
    let engine = LinkEngine::spawn(
        Host::spawn(
            HostLink::host("host").unwrap().as_bridge(),
            Stream(host_end),
        ),
        "lab",
    );
    let mut opts = RuntimeOptions::new("host");
    opts.domain = "lab".to_string();
    opts.engine = Some(Arc::new(engine));
    opts.install_tracing = false;
    let cloudy = Cloudy::open(opts).await.expect("cloudy over link");

    // MCU 側のイベントを 1 本の task で捌く: Data は channel へ、Add には答える。
    let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel::<HostEvent>();
    let (conn_tx, mut conn_rx) = tokio::sync::mpsc::unbounded_channel::<HostEvent>();
    {
        let mcu = Arc::clone(&mcu);
        tokio::spawn(async move {
            while let Some(ev) = mcu.recv().await {
                match ev {
                    HostEvent::Request { hash, seq, payload }
                        if hash == wire::type_hash("EngAdd") =>
                    {
                        let add = <Add as prost::Message>::decode(payload.as_slice()).unwrap();
                        mcu.reply::<Add>(seq, &Sum { s: add.a + add.b }).unwrap();
                    }
                    HostEvent::Data { .. } => {
                        let _ = data_tx.send(ev);
                    }
                    HostEvent::Connected { .. } | HostEvent::Disconnected => {
                        let _ = conn_tx.send(ev);
                    }
                    HostEvent::Request { .. } => {}
                }
            }
        });
    }

    // 握手: MCU から見た相手は型を名乗らない bridge。
    let Some(HostEvent::Connected { id, types }) = timeout(WAIT, conn_rx.recv()).await.unwrap()
    else {
        panic!("mcu should connect")
    };
    assert_eq!((id.as_str(), types.len()), ("host", 0));

    // presence: MCU の Hello がそのまま publishers / servers / watch に見える。
    let mut watch = cloudy.watch_publishers::<Pos>().unwrap();
    assert_eq!(
        timeout(WAIT, watch.recv()).await.unwrap(),
        Some(PresenceEvent::Joined("mcu".to_string()))
    );
    assert_eq!(cloudy.publishers::<Pos>().await.unwrap(), ["mcu"]);
    assert_eq!(cloudy.servers::<Add>().await.unwrap(), ["mcu"]);
    assert!(cloudy.publishers::<Cmd>().await.unwrap().is_empty());

    // MCU → Cloudy: Data(指紋は Hello 経由で attachment に載る)。
    let mut sub = cloudy.subscribe::<Pos>().unwrap();
    assert!(mcu.send(&Pos { x: 1 }).unwrap());
    let envelope = timeout(WAIT, sub.recv_envelope()).await.unwrap().unwrap();
    assert_eq!((envelope.value.x, envelope.source.as_str()), (1, "mcu"));

    // latched: 後から来た購読者は engine が覚えている直近値を受け取る。
    let mut late = cloudy.subscriber::<Pos>().latched().build().unwrap();
    assert_eq!(
        timeout(WAIT, late.recv()).await.unwrap().map(|p| p.x),
        Some(1)
    );

    // Cloudy → MCU: publish は subscribe されている型だけ届く。
    let cmd = cloudy.publish::<Cmd>().unwrap();
    cmd.send(Cmd { v: 7 }).await.unwrap();
    let ev = timeout(WAIT, data_rx.recv()).await.unwrap().unwrap();
    assert_eq!(ev.decode::<Cmd>(), Some(Cmd { v: 7 }));

    // Cloudy → MCU: service。
    let sum = timeout(WAIT, cloudy.call::<Add>(Add { a: 2, b: 3 }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sum.s, 5);
    let nobody = cloudy
        .caller::<Add>()
        .to("ghost")
        .timeout(Duration::from_millis(500))
        .build();
    assert!(matches!(
        nobody.call(Add { a: 1, b: 1 }).await,
        Err(CallError::NoReply)
    ));

    // MCU → Cloudy: service(bridge は serve していない型の request も受けて responder へ)。
    let mut server = cloudy.serve::<Echo>().unwrap();
    let server_task = tokio::spawn(async move {
        let req = server.recv().await.unwrap();
        let text = req.value.text.clone();
        req.reply(Echo {
            text: format!("{text}!"),
        })
        .await
        .unwrap();
    });
    let echoed = timeout(
        WAIT,
        mcu.call::<Echo>(
            &Echo {
                text: "hi".to_string(),
            },
            WAIT,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(echoed.text, "hi!");
    server_task.await.unwrap();
    // responder が居ない型の request はエラーで返る(ハングしない)。
    let err = mcu
        .call::<Echo>(
            &Echo {
                text: "x".to_string(),
            },
            WAIT,
        )
        .await;
    assert!(
        matches!(err, Err(reiny_link::CallError::Remote(_))),
        "{err:?}"
    );
}
