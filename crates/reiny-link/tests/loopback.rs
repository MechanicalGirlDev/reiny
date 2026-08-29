//! Two hosts wired to each other over a stream (`tokio::io::duplex`) and over UDP (loopback), to
//! check `Host`'s connect / Data / call end to end. No hardware. UDP lets the OS pick a free port,
//! so these do not collide when run in parallel.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking

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

/// Playing the MCU: publishes `State`, serves `Add`.
fn mcu_link() -> HostLink {
    let mut l = HostLink::host("mcu").unwrap();
    l.publishes::<State>().unwrap();
    l.serves::<Add>().unwrap();
    l
}

/// Playing the PC: subscribes to `State`, calls `Add`.
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

/// Wire both ends together and run through the whole repertoire. The body is the same whichever
/// transport it is given.
async fn exercise<A: Transport + 'static, B: Transport + 'static>(mcu_t: A, pc_t: B) {
    let mut mcu = Host::spawn(mcu_link(), mcu_t);
    let mut pc = Host::spawn(pc_link(), pc_t);

    // Connect: both sides see Connected, and the PC can see the MCU's type names.
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

    // Data: MCU → PC.
    for seq in 1..=3u32 {
        assert_eq!(mcu.send(&State { seq }), Ok(true));
    }
    for seq in 1..=3u32 {
        let ev = timeout(WAIT, pc.recv()).await.unwrap().unwrap();
        assert_eq!(ev.decode::<State>(), Some(State { seq }), "{ev:?}");
    }
    // The PC does not publish State, and the MCU does not subscribe to it.
    assert_eq!(
        pc.send(&State { seq: 9 }),
        Err(reiny_link::Error::NotDeclared)
    );

    // call: PC → MCU → PC. The MCU side takes the Request off recv and replies. recv never returns
    // None while the peer is alive, so the server half exits on the client's signal (the two are
    // joined).
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
    // The MCU cannot call Add (the PC does not serve it).
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
    // Only the MCU side knows the peer; the PC side learns it from the first datagram.
    let pc_addr = pc.local_addr().unwrap();
    exercise(mcu.with_peer(pc_addr), pc).await;
}

/// A peer that is connected but never answers has to end as `Timeout`, and the abandoned pending
/// entry must not swallow a later, unrelated call's reply.
#[tokio::test]
async fn call_times_out_when_the_peer_never_replies() {
    let (a, b) = tokio::io::duplex(4096);
    // The MCU serves Add but nobody ever drains its events, so no reply is ever produced.
    let _mcu = Host::spawn(mcu_link(), Stream(a));
    let mut pc = Host::spawn(pc_link(), Stream(b));
    expect_connected(&mut pc, "mcu").await;

    let started = std::time::Instant::now();
    match pc
        .call(&Add { a: 1, b: 2 }, Duration::from_millis(300))
        .await
    {
        Err(CallError::Timeout) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }
    assert!(started.elapsed() < WAIT, "timeout took far too long");
    // Still usable afterwards: the timed-out call was cleaned up, not left wedged.
    assert!(pc.is_connected());
}

/// Losing the peer surfaces as `Disconnected` after `timeout_ms` of silence. The knobs are set short
/// so the test does not sit through the 3 s default.
#[tokio::test]
async fn silence_disconnects_the_host() {
    use reiny_link::LinkConfig;

    use tokio::io::AsyncWriteExt;

    let (mut a, b) = tokio::io::duplex(4096);
    let config = LinkConfig {
        ping_interval_ms: 50,
        timeout_ms: 200,
    };
    // Say hello once by hand and then go quiet, keeping the stream open. Aborting a `Host` would
    // drop its transport, and the PC would see EOF instead of silence — a different code path.
    let mut mcu = mcu_link().with_config(config);
    mcu.tick(0);
    let mut buf = vec![0u8; 4096];
    let n = mcu.drain(&mut buf);
    a.write_all(&buf[..n]).await.unwrap();

    let pc = Host::spawn(pc_link().with_config(config), Stream(b));
    match timeout(WAIT, pc.recv()).await.unwrap() {
        Some(HostEvent::Connected { id, .. }) => assert_eq!(id, "mcu"),
        other => panic!("expected Connected, got {other:?}"),
    }
    match timeout(WAIT, pc.recv()).await.unwrap() {
        Some(HostEvent::Disconnected) => {}
        other => panic!("expected Disconnected, got {other:?}"),
    }
    assert!(!pc.is_connected());
    assert_eq!(pc.peer(), None);
    // Nothing is routable at a peer that is gone.
    match pc
        .call(&Add { a: 1, b: 1 }, Duration::from_millis(200))
        .await
    {
        Err(CallError::NoPeerService) => {}
        other => panic!("expected NoPeerService, got {other:?}"),
    }
}

/// A pinned UDP peer is a filter, not a hint: datagrams from any other address are dropped rather
/// than being fed into the link, where they would look like a corrupt or a competing stream.
#[tokio::test]
async fn udp_ignores_datagrams_from_other_addresses() {
    let mcu = Udp::bind("127.0.0.1:0").await.unwrap();
    let pc = Udp::bind("127.0.0.1:0").await.unwrap();
    let pc_addr = pc.local_addr().unwrap();
    let mcu_addr = mcu.local_addr().unwrap();

    let mcu = Host::spawn(mcu_link(), mcu.with_peer(pc_addr));
    let mut pc = Host::spawn(pc_link(), pc.with_peer(mcu_addr));
    expect_connected(&mut pc, "mcu").await;

    // A third party shouting at the PC's port. `\0` is a frame delimiter, so this would be handed to
    // the link as (broken) frames if the filter were not there.
    let stranger = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for _ in 0..4 {
        stranger
            .send_to(b"\x01\x02\x00\x03\x00", pc_addr)
            .await
            .unwrap();
    }

    // Real traffic still arrives, and the link never counted a bad frame.
    assert_eq!(mcu.send(&State { seq: 42 }), Ok(true));
    let ev = timeout(WAIT, pc.recv()).await.unwrap().unwrap();
    assert_eq!(ev.decode::<State>(), Some(State { seq: 42 }), "{ev:?}");
    let stats = pc.link().lock().unwrap().stats();
    assert_eq!(stats.bad_frames, 0, "the stranger's bytes reached the link");
}

#[tokio::test]
async fn stream_eof_closes_host() {
    let (a, b) = tokio::io::duplex(4096);
    let host = Host::spawn(pc_link(), Stream(a));
    drop(b);
    // A stream whose peer is gone hits EOF → the driver ends and recv returns None.
    assert_eq!(timeout(WAIT, host.recv()).await.unwrap(), None);
    match host.call(&Add { a: 0, b: 0 }, WAIT).await {
        Err(CallError::NoPeerService) => {}
        other => panic!("expected NoPeerService, got {other:?}"),
    }
}
