//! Transport adapter tests over a duplex stream. Applications use named static ports;
//! the host side exercises the low-level Engine boundary without an unbound Cloudy.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking

use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use reiny::engine::{Engine, Guard, Key, Presence, QueryParams, SERVICE_CHUNK, SUB_CHUNK, Sample};
use reiny::{Service, Topic};
use reiny_link::transport::Stream;
use reiny_link::{
    Host, HostEvent, HostLink, LinkEngine, PortDirection, PortType, StaticPort, wire,
};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::timeout;

#[derive(Clone, PartialEq, prost::Message)]
struct Pos {
    #[prost(int32, tag = "1")]
    x: i32,
}
impl Topic for Pos {
    const TYPE: &'static str = "EngPos";
    const SCHEMA: u64 = 0x55;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

#[derive(Clone, PartialEq, prost::Message)]
struct Cmd {
    #[prost(uint32, tag = "1")]
    v: u32,
}
impl Topic for Cmd {
    const TYPE: &'static str = "EngCmd";
    const SCHEMA: u64 = 0x66;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
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
    const SCHEMA: u64 = 0x77;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}
#[derive(Clone, PartialEq, prost::Message)]
struct Sum {
    #[prost(int32, tag = "1")]
    s: i32,
}
impl Topic for Sum {
    const TYPE: &'static str = "EngSum";
    const SCHEMA: u64 = 0x88;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}
impl Service for Add {
    type Response = Sum;
}

/// Called by the MCU and served by the host.
#[derive(Clone, PartialEq, prost::Message)]
struct Echo {
    #[prost(string, tag = "1")]
    text: String,
}
impl Topic for Echo {
    const TYPE: &'static str = "EngEcho";
    const SCHEMA: u64 = 0x99;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}
impl Service for Echo {
    type Response = Echo;
}

const PORTS: &[StaticPort] = &[
    StaticPort {
        name: "position",
        direction: PortDirection::Output,
        ty: Pos::TYPE,
        schema: Pos::SCHEMA,
        response: None,
    },
    StaticPort {
        name: "command",
        direction: PortDirection::Input,
        ty: Cmd::TYPE,
        schema: Cmd::SCHEMA,
        response: None,
    },
    StaticPort {
        name: "addition",
        direction: PortDirection::Service,
        ty: Add::TYPE,
        schema: Add::SCHEMA,
        response: Some(PortType {
            ty: Sum::TYPE,
            schema: Sum::SCHEMA,
        }),
    },
    StaticPort {
        name: "echo",
        direction: PortDirection::Client,
        ty: Echo::TYPE,
        schema: Echo::SCHEMA,
        response: Some(PortType {
            ty: Echo::TYPE,
            schema: Echo::SCHEMA,
        }),
    },
];

const WAIT: Duration = Duration::from_secs(5);

fn fingerprint<T: Topic>() -> Vec<u8> {
    T::SCHEMA.to_le_bytes().to_vec()
}

fn watch(engine: &LinkEngine, key: &Key) -> (Guard, UnboundedReceiver<Presence>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let guard = engine
        .watch_alive(
            key,
            Box::new(move |event| {
                let _ = tx.send(event);
            }),
        )
        .unwrap();
    (guard, rx)
}

fn subscribe(engine: &LinkEngine, key: &Key) -> (Guard, UnboundedReceiver<Sample>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let guard = engine
        .subscribe(
            key,
            Box::new(move |sample| {
                let _ = tx.send(sample);
            }),
        )
        .unwrap();
    (guard, rx)
}

async fn link_to_engine(link: HostLink, id: &str) -> (Arc<Host>, LinkEngine) {
    let peer = link.id().to_string();
    let (mcu_end, host_end) = tokio::io::duplex(4096);
    let mcu = Arc::new(Host::spawn(link, Stream(mcu_end)));
    let engine = LinkEngine::spawn(Host::bridge(id, Stream(host_end)).unwrap(), "lab");
    let launch = Key::launch("lab", Some(&peer));
    let (_watch, mut events) = watch(&engine, &launch);
    assert_eq!(
        timeout(WAIT, events.recv()).await.unwrap(),
        Some(Presence::Joined(launch))
    );
    assert!(matches!(
        timeout(WAIT, mcu.recv()).await.unwrap(),
        Some(HostEvent::Connected { .. })
    ));
    (mcu, engine)
}

fn request<T: Topic + Message>(value: &T) -> QueryParams {
    QueryParams {
        payload: Some(value.encode_to_vec()),
        attachment: Some(fingerprint::<T>()),
        timeout: WAIT,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn outgoing_data_and_requests_require_matching_fingerprints() {
    let mut link = HostLink::host("mcu", PORTS).unwrap();
    link.subscribes::<Cmd>("command").unwrap();
    link.serves::<Add>("addition").unwrap();
    let (mcu, engine) = link_to_engine(link, "fingerprints").await;
    assert!(engine.caps().attachment);
    let sub = Key::topic("lab", Some("mcu"), Cmd::TYPE).with_chunk(SUB_CHUNK);
    let (_watch, mut events) = watch(&engine, &sub);
    assert_eq!(
        timeout(WAIT, events.recv()).await.unwrap(),
        Some(Presence::Joined(sub))
    );
    let publisher = engine
        .publisher(
            &Key::topic("lab", Some("host"), Cmd::TYPE),
            &reiny::Qos::default(),
        )
        .unwrap();
    for attachment in [
        None,
        Some(vec![0; 7]),
        Some((Cmd::SCHEMA + 1).to_le_bytes().to_vec()),
    ] {
        assert!(
            publisher
                .put(Cmd { v: 1 }.encode_to_vec(), attachment.clone())
                .is_err()
        );
        assert!(
            engine
                .query(
                    &Key::topic("lab", Some("mcu"), Add::TYPE),
                    QueryParams {
                        payload: Some(Add { a: 1, b: 2 }.encode_to_vec()),
                        attachment,
                        timeout: WAIT,
                    }
                )
                .is_err()
        );
    }
    publisher
        .put(Cmd { v: 2 }.encode_to_vec(), Some(fingerprint::<Cmd>()))
        .unwrap();
    let event = timeout(WAIT, mcu.recv()).await.unwrap().unwrap();
    assert_eq!(event.decode::<Cmd>(), Some(Cmd { v: 2 }));
    assert_eq!(
        engine.host().send("command", &Cmd { v: 3 }),
        Err(reiny_link::Error::UnknownPort)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn hierarchical_peer_namespace_keeps_type_identity_and_exact_routing() {
    const PEER: &str = "deployment/arm/mcu";
    let mut link = HostLink::host(PEER, PORTS).unwrap();
    link.publishes::<Pos>("position").unwrap();
    link.subscribes::<Cmd>("command").unwrap();
    link.serves::<Add>("addition").unwrap();
    let (mcu, engine) = link_to_engine(link, "deployment/host/bridge").await;
    let position = Key::topic("lab", Some(PEER), Pos::TYPE);
    let (_watch, mut events) = watch(&engine, &position);
    assert_eq!(
        timeout(WAIT, events.recv()).await.unwrap(),
        Some(Presence::Joined(position.clone()))
    );
    for key in [
        position.clone(),
        Key::topic("lab", Some(PEER), Cmd::TYPE).with_chunk(SUB_CHUNK),
        Key::topic("lab", Some(PEER), Add::TYPE).with_chunk(SERVICE_CHUNK),
    ] {
        assert_eq!(engine.alive(&key, WAIT).await.unwrap(), [key]);
    }
    let (_exact, mut exact) = subscribe(&engine, &position);
    let (_recursive, mut recursive) = subscribe(&engine, &Key::topic("lab", None, Pos::TYPE));
    assert!(mcu.send("position", &Pos { x: 9 }).unwrap());
    for sample in [
        timeout(WAIT, exact.recv()).await.unwrap().unwrap(),
        timeout(WAIT, recursive.recv()).await.unwrap().unwrap(),
    ] {
        assert_eq!(sample.key, position);
        assert_eq!(Pos::decode(sample.payload.as_slice()).unwrap().x, 9);
        assert_eq!(sample.attachment, Some(fingerprint::<Pos>()));
    }
    let (data_tx, mut data_rx) = mpsc::unbounded_channel();
    let peer_task = {
        let mcu = Arc::clone(&mcu);
        tokio::spawn(async move {
            while let Some(event) = mcu.recv().await {
                match event {
                    HostEvent::Request {
                        hash, seq, payload, ..
                    } => {
                        assert_eq!(hash, wire::type_hash(Add::TYPE));
                        let add = Add::decode(payload.as_slice()).unwrap();
                        mcu.reply::<Add>("addition", seq, &Sum { s: add.a + add.b })
                            .unwrap();
                    }
                    HostEvent::Data { .. } => data_tx.send(event).unwrap(),
                    HostEvent::Connected { .. } | HostEvent::Disconnected => {}
                }
            }
        })
    };
    let cmd = engine
        .publisher(
            &Key::topic("lab", Some("host"), Cmd::TYPE),
            &reiny::Qos::default(),
        )
        .unwrap();
    cmd.put(Cmd { v: 11 }.encode_to_vec(), Some(fingerprint::<Cmd>()))
        .unwrap();
    let event = timeout(WAIT, data_rx.recv()).await.unwrap().unwrap();
    assert_eq!(event.decode::<Cmd>(), Some(Cmd { v: 11 }));
    let exact_key = Key::topic("lab", Some(PEER), Add::TYPE);
    for key in [
        Key::topic("lab", Some("deployment/arm"), Add::TYPE),
        Key::topic("other", Some(PEER), Add::TYPE),
        exact_key.with_chunk("@schema/probe.Add"),
    ] {
        let mut replies = engine.query(&key, request(&Add { a: 2, b: 3 })).unwrap();
        assert!(
            timeout(WAIT, replies.next()).await.unwrap().is_none(),
            "{key}"
        );
    }
    let mut replies = engine
        .query(&exact_key, request(&Add { a: 2, b: 3 }))
        .unwrap();
    let sum = timeout(WAIT, replies.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(sum.key, exact_key);
    assert_eq!(sum.attachment, Some(fingerprint::<Sum>()));
    assert_eq!(Sum::decode(sum.payload.as_slice()).unwrap().s, 5);
    peer_task.abort();
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // exercises the bidirectional transport adapter as one pair
async fn engine_over_declared_ports() {
    let mut link = HostLink::host("mcu", PORTS).unwrap();
    link.publishes_latched::<Pos>("position").unwrap();
    link.subscribes::<Cmd>("command").unwrap();
    link.serves::<Add>("addition").unwrap();
    link.calls::<Echo>("echo").unwrap();
    let (mcu, engine) = link_to_engine(link, "host").await;
    assert!(mcu.link().lock().unwrap().peer_is_bridge());
    assert_eq!(mcu.peer().unwrap().1, Vec::<reiny_link::PeerType>::new());
    let position = Key::topic("lab", Some("mcu"), Pos::TYPE);
    let (_watch, mut events) = watch(&engine, &position);
    assert_eq!(
        timeout(WAIT, events.recv()).await.unwrap(),
        Some(Presence::Joined(position.clone()))
    );
    assert_eq!(
        engine.alive(&position, WAIT).await.unwrap().as_slice(),
        core::slice::from_ref(&position)
    );
    assert_eq!(
        engine
            .alive(
                &Key::topic("lab", Some("mcu"), Add::TYPE).with_chunk(SERVICE_CHUNK),
                WAIT
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        engine
            .alive(&Key::topic("lab", Some("mcu"), Cmd::TYPE), WAIT)
            .await
            .unwrap(),
        Vec::<Key>::new()
    );
    assert_eq!(
        engine
            .alive(
                &Key::topic("lab", Some("mcu"), Cmd::TYPE).with_chunk(SUB_CHUNK),
                WAIT
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        engine
            .alive(&position.with_chunk(SUB_CHUNK), WAIT)
            .await
            .unwrap(),
        Vec::<Key>::new()
    );
    let (_sub, mut samples) = subscribe(&engine, &position);
    assert!(mcu.send("position", &Pos { x: 1 }).unwrap());
    let sample = timeout(WAIT, samples.recv()).await.unwrap().unwrap();
    assert_eq!(sample.key, position);
    assert_eq!(Pos::decode(sample.payload.as_slice()).unwrap().x, 1);
    assert_eq!(sample.attachment, Some(fingerprint::<Pos>()));
    let mut latched = engine
        .query(
            &position,
            QueryParams {
                payload: None,
                attachment: None,
                timeout: WAIT,
            },
        )
        .unwrap();
    let sample = timeout(WAIT, latched.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(Pos::decode(sample.payload.as_slice()).unwrap().x, 1);
    let cmd = engine
        .publisher(
            &Key::topic("lab", Some("host"), Cmd::TYPE),
            &reiny::Qos::default(),
        )
        .unwrap();
    cmd.put(Cmd { v: 7 }.encode_to_vec(), Some(fingerprint::<Cmd>()))
        .unwrap();
    let event = timeout(WAIT, mcu.recv()).await.unwrap().unwrap();
    assert_eq!(event.decode::<Cmd>(), Some(Cmd { v: 7 }));
    let service = Key::topic("lab", Some("mcu"), Add::TYPE);
    let mut replies = engine
        .query(&service, request(&Add { a: 2, b: 3 }))
        .unwrap();
    let event = timeout(WAIT, mcu.recv()).await.unwrap().unwrap();
    let HostEvent::Request { seq, .. } = &event else {
        panic!("expected request")
    };
    let add = event.decode::<Add>().unwrap();
    mcu.reply::<Add>("addition", *seq, &Sum { s: add.a + add.b })
        .unwrap();
    let sample = timeout(WAIT, replies.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(sample.attachment, Some(fingerprint::<Sum>()));
    assert_eq!(Sum::decode(sample.payload.as_slice()).unwrap().s, 5);
    let mut nobody = engine
        .query(
            &Key::topic("lab", Some("ghost"), Add::TYPE),
            request(&Add { a: 1, b: 1 }),
        )
        .unwrap();
    assert!(timeout(WAIT, nobody.next()).await.unwrap().is_none());
    let echo_key = Key::topic("lab", Some("host"), Echo::TYPE);
    let response_key = echo_key.clone();
    let responder = engine
        .respond(
            &echo_key,
            Box::new(move |query| {
                assert_eq!(query.attachment(), Some(fingerprint::<Echo>().as_slice()));
                let echo = Echo::decode(query.payload().unwrap()).unwrap();
                query
                    .reply(
                        &response_key,
                        Echo {
                            text: format!("{}!", echo.text),
                        }
                        .encode_to_vec(),
                        Some(fingerprint::<Echo>()),
                    )
                    .unwrap();
            }),
        )
        .unwrap();
    let echoed = timeout(
        WAIT,
        mcu.call::<Echo>("echo", &Echo { text: "hi".into() }, WAIT),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(echoed.text, "hi!");
    drop(responder);
    let error = mcu
        .call::<Echo>("echo", &Echo { text: "x".into() }, WAIT)
        .await;
    assert!(
        matches!(error, Err(reiny_link::CallError::Remote(_))),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_loss_retracts_presence() {
    let mut link = HostLink::host("mcu", PORTS).unwrap();
    link.publishes::<Pos>("position").unwrap();
    link.serves::<Add>("addition").unwrap();
    let (mcu, engine) = link_to_engine(link, "loss").await;
    let position = Key::topic("lab", Some("mcu"), Pos::TYPE);
    let (_watch, mut events) = watch(&engine, &position);
    assert_eq!(
        timeout(WAIT, events.recv()).await.unwrap(),
        Some(Presence::Joined(position.clone()))
    );
    assert_eq!(
        engine.alive(&position, WAIT).await.unwrap().as_slice(),
        core::slice::from_ref(&position)
    );
    drop(mcu);
    assert_eq!(
        timeout(WAIT, events.recv()).await.unwrap(),
        Some(Presence::Left(position.clone()))
    );
    assert_eq!(
        engine.alive(&position, WAIT).await.unwrap(),
        Vec::<Key>::new()
    );
    assert_eq!(
        engine
            .alive(
                &Key::topic("lab", Some("mcu"), Add::TYPE).with_chunk(SERVICE_CHUNK),
                WAIT
            )
            .await
            .unwrap(),
        Vec::<Key>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn request_type_the_peer_never_announced_is_refused() {
    let link = HostLink::host("mcu", PORTS).unwrap();
    let (mcu, engine) = link_to_engine(link, "unannounced").await;
    let _server = engine
        .respond(
            &Key::topic("lab", Some("host"), Echo::TYPE),
            Box::new(|_| panic!("undeclared client reached transport")),
        )
        .unwrap();
    let error = mcu
        .call::<Echo>("echo", &Echo { text: "hi".into() }, WAIT)
        .await;
    assert!(
        matches!(
            error,
            Err(reiny_link::CallError::Link(reiny_link::Error::NotDeclared))
        ),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_request_answers_with_an_error() {
    let mut link = HostLink::host("mcu", PORTS).unwrap();
    link.calls::<Echo>("echo").unwrap();
    let (mcu, engine) = link_to_engine(link, "dropped").await;
    let _server = engine
        .respond(&Key::topic("lab", Some("host"), Echo::TYPE), Box::new(drop))
        .unwrap();
    let started = std::time::Instant::now();
    let error = mcu
        .call::<Echo>("echo", &Echo { text: "hi".into() }, WAIT)
        .await;
    match error {
        Err(reiny_link::CallError::Remote(message)) => assert_eq!(message, "no reply"),
        other => panic!("expected Remote(no reply), got {other:?}"),
    }
    assert!(started.elapsed() < WAIT, "waited for the full timeout");
}
