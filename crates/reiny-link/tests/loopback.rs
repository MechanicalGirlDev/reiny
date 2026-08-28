//! ホスト同士を stream(`tokio::io::duplex`)と UDP(ループバック)で繋ぎ、`Host` の
//! 接続 / Data / call を通しで確かめる。ハードウェア無し。UDP は OS に空きポートを選ばせるので
//! 並列実行でも衝突しない。

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // テストは panic で失敗を表現してよい

use std::time::Duration;

use reiny_link::transport::{Stream, Transport, Udp};
use reiny_link::{CallError, Host, HostEvent, HostLink, Service, Topic, wire};
use tokio::time::timeout;

#[derive(Clone, PartialEq, prost::Message)]
struct State {
    #[prost(uint32, tag = "1")]
    seq: u32,
}
impl Topic for State {
    const TYPE: &'static str = "LoopState";
}

#[derive(Clone, PartialEq, prost::Message)]
struct Add {
    #[prost(int32, tag = "1")]
    a: i32,
    #[prost(int32, tag = "2")]
    b: i32,
}
impl Topic for Add {
    const TYPE: &'static str = "LoopAdd";
}
#[derive(Clone, PartialEq, prost::Message)]
struct Sum {
    #[prost(int32, tag = "1")]
    s: i32,
}
impl Topic for Sum {
    const TYPE: &'static str = "LoopSum";
}
impl Service for Add {
    type Response = Sum;
}

const WAIT: Duration = Duration::from_secs(5);

/// 「MCU 役」: `State` を publish、`Add` を serve。
fn mcu_link() -> HostLink {
    let mut l = HostLink::host("mcu").unwrap();
    l.publishes::<State>().unwrap();
    l.serves::<Add>().unwrap();
    l
}

/// 「PC 役」: `State` を subscribe、`Add` を呼ぶ。
fn pc_link() -> HostLink {
    let mut l = HostLink::host("pc").unwrap();
    l.subscribes::<State>().unwrap();
    l
}

async fn expect_connected(host: &mut Host, peer: &str) -> Vec<reiny_link::PeerType> {
    match timeout(WAIT, host.recv()).await.expect("connect in time") {
        Some(HostEvent::Connected { id, types }) => {
            assert_eq!(id, peer);
            types
        }
        other => panic!("expected Connected, got {other:?}"),
    }
}

/// 両端を繋いで一通り流す。transport の種類に依らない本体。
async fn exercise<A: Transport + 'static, B: Transport + 'static>(mcu_t: A, pc_t: B) {
    let mut mcu = Host::spawn(mcu_link(), mcu_t);
    let mut pc = Host::spawn(pc_link(), pc_t);

    // 接続: 双方に Connected、PC は MCU の型名まで見える。
    let types = expect_connected(&mut pc, "mcu").await;
    expect_connected(&mut mcu, "pc").await;
    assert!(
        types
            .iter()
            .any(|t| t.name == "LoopState" && t.flags & wire::flags::PUB != 0)
    );
    assert!(
        types
            .iter()
            .any(|t| t.name == "LoopAdd" && t.flags & wire::flags::SERVE != 0)
    );
    assert!(pc.is_connected() && mcu.is_connected());
    assert_eq!(pc.peer().map(|p| p.0), Some("mcu".to_string()));

    // Data: MCU → PC。
    for seq in 1..=3u32 {
        assert_eq!(mcu.send(&State { seq }), Ok(true));
    }
    for seq in 1..=3u32 {
        let ev = timeout(WAIT, pc.recv()).await.unwrap().unwrap();
        assert_eq!(ev.decode::<State>(), Some(State { seq }), "{ev:?}");
    }
    // PC は State を publish していない / MCU は購読していない。
    assert_eq!(
        pc.send(&State { seq: 9 }),
        Err(reiny_link::Error::NotDeclared)
    );

    // call: PC → MCU → PC。MCU 役は recv で Request を受けて reply する。相手が生きている限り
    // recv は None を返さないので、server 側は client の合図で抜ける(join で同居させる)。
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();
    let server = async {
        loop {
            let ev = tokio::select! {
                _ = &mut done_rx => break,
                ev = mcu.recv() => ev,
            };
            let Some(ev) = ev else { break };
            if let HostEvent::Request { seq, .. } = &ev {
                let add = ev.decode::<Add>().unwrap();
                if add.b < 0 {
                    mcu.reply_err::<Add>(*seq, "negative").unwrap();
                } else {
                    mcu.reply::<Add>(*seq, &Sum { s: add.a + add.b }).unwrap();
                }
            }
        }
    };
    let client = async {
        let sum = pc.call(&Add { a: 2, b: 3 }, WAIT).await.unwrap();
        assert_eq!(sum, Sum { s: 5 });
        match pc.call(&Add { a: 1, b: -1 }, WAIT).await {
            Err(CallError::Remote(m)) => assert_eq!(m, "negative"),
            other => panic!("expected Remote, got {other:?}"),
        }
        let _ = done_tx.send(());
    };
    tokio::join!(server, client);
    // MCU は Add を呼べない(PC は serve していない)。
    match mcu.call(&Add { a: 0, b: 0 }, WAIT).await {
        Err(CallError::NoPeerService) => {}
        other => panic!("expected NoPeerService, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_duplex() {
    let (a, b) = tokio::io::duplex(4096);
    exercise(Stream(a), Stream(b)).await;
}

#[tokio::test]
async fn udp_loopback() {
    let mcu = Udp::bind("127.0.0.1:0").await.unwrap();
    let pc = Udp::bind("127.0.0.1:0").await.unwrap();
    // MCU 役だけが相手を知っている。PC 役は最初の datagram で覚える。
    let pc_addr = pc.local_addr().unwrap();
    exercise(mcu.with_peer(pc_addr), pc).await;
}

#[tokio::test]
async fn stream_eof_closes_host() {
    let (a, b) = tokio::io::duplex(4096);
    let host = Host::spawn(pc_link(), Stream(a));
    drop(b);
    // 相手が消えた stream は EOF → ドライバが終わり、recv は None。
    assert_eq!(timeout(WAIT, host.recv()).await.unwrap(), None);
    match host.call(&Add { a: 0, b: 0 }, WAIT).await {
        Err(CallError::NoPeerService) => {}
        other => panic!("expected NoPeerService, got {other:?}"),
    }
}
