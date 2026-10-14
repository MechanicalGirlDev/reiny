//! Real transport coverage for prepared foreign module endpoints.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use prost::Message as _;
use reiny::MessageType;
use std::io::Write;

#[derive(Clone, PartialEq, prost::Message)]
struct Ping {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}
impl MessageType for Ping {
    const TYPE_NAME: &'static str = "Ping";
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
    const SCHEMA_FINGERPRINT: u64 = u64::MAX;
}

fn prepared(id: &str, source: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::with_suffix(".json").unwrap();
    write!(
        file,
        r#"{{"version":2,"namespace":"{id}","endpoint_namespace":"{id}",
        "inputs":{{"incoming":{{"contract":{{"type":"Ping"}},"sources":["{source}"]}}}},
        "outputs":{{"outgoing":{{"contract":{{"type":"Ping"}}}}}}}}"#
    )
    .unwrap();
    file
}
fn path(file: &tempfile::NamedTempFile) -> String {
    file.path().to_str().unwrap().into()
}
fn connect(bus: &LocalBus, id: &str, source: &str, domain: &str) -> Arc<Session> {
    let bindings = prepared(id, source);
    bus.connect(id.into(), domain.into(), path(&bindings))
        .unwrap()
}
fn native(session: &Session, bus: &LocalBus) -> ModuleContext {
    let bindings = prepared("rust", "foreign");
    let mut options = RuntimeOptions::new("rust");
    options.domain = "test".into();
    options.engine = Some(bus.engine.clone());
    options.module_bindings_path = Some(bindings.path().to_owned());
    options.install_tracing = false;
    session
        .runtime
        .block_on(ModuleContext::open(options))
        .unwrap()
}

#[test]
fn ffi_receives_typed_rust_protobuf_with_provenance() {
    let bus = LocalBus::new();
    let foreign = connect(&bus, "foreign", "rust", "test");
    let native = native(&foreign, &bus);
    let publisher = native.output::<Ping>("outgoing").unwrap();
    let subscriber = foreign
        .subscriber("incoming", "Ping", Ping::SCHEMA_FINGERPRINT)
        .unwrap();
    let ping = Ping {
        data: vec![0, 1, 255, 0],
    };
    foreign
        .runtime
        .block_on(publisher.send(ping.clone()))
        .unwrap();
    let message = subscriber.receive(1000).unwrap().unwrap();
    assert_eq!(Ping::decode(message.payload.as_slice()).unwrap(), ping);
    assert_eq!(message.source, "rust");
    assert_eq!(message.schema, Some(u64::MAX));
}

#[test]
fn rust_receives_ffi_encoded_protobuf() {
    let bus = LocalBus::new();
    let foreign = connect(&bus, "foreign", "rust", "test");
    let native = native(&foreign, &bus);
    let mut subscriber = native.input::<Ping>("incoming").unwrap();
    let publisher = foreign
        .publisher("outgoing", "Ping", Ping::SCHEMA_FINGERPRINT)
        .unwrap();
    let ping = Ping {
        data: vec![0, 128, 255],
    };
    publisher.send(ping.encode_to_vec()).unwrap();
    let message = foreign.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), subscriber.recv_envelope())
            .await
            .unwrap()
            .unwrap()
    });
    assert_eq!(message.value, ping);
    assert_eq!(message.source, "foreign");
}

#[test]
fn subscription_filters_prepared_source_domain_and_schema() {
    use reiny::engine::Engine as _;
    let bus = LocalBus::new();
    let a = connect(&bus, "robot/left", "robot/right", "test");
    let b = connect(&bus, "robot/right", "robot/left", "test");
    let other = connect(&bus, "robot/right", "robot/left", "other");
    let subscriber = a.subscriber("incoming", "Ping", 42).unwrap();
    let wrong_source = a.publisher("outgoing", "Ping", 42).unwrap();
    let wrong_domain = other.publisher("outgoing", "Ping", 42).unwrap();
    let wrong_schema = b.publisher("outgoing", "Ping", 41).unwrap();
    wrong_source.send(vec![1]).unwrap();
    wrong_domain.send(vec![2]).unwrap();
    wrong_schema.send(vec![3]).unwrap();
    let missing_schema = bus
        .engine
        .publisher(
            &reiny::engine::Key::topic("test", Some("robot/right"), "Ping"),
            &reiny::Qos::DEFAULT,
        )
        .unwrap();
    missing_schema.put(vec![5], None).unwrap();
    // The same source sends correct metadata through a separate domain-matched module instance.
    drop(wrong_schema);
    drop(b);
    let correct = connect(&bus, "robot/right", "robot/left", "test");
    let expected = correct.publisher("outgoing", "Ping", 42).unwrap();
    expected.send(vec![4]).unwrap();
    let message = subscriber.receive(1000).unwrap().unwrap();
    assert_eq!(message.payload, vec![4]);
    assert_eq!(message.source, "robot/right");
    assert_eq!(message.schema, Some(42));
}

#[test]
fn readiness_requires_live_complete_named_ports() {
    let bus = LocalBus::new();
    let session = connect(&bus, "sender", "sender", "test");
    assert!(session.ready().is_err());
    let publisher = session.publisher("outgoing", "Ping", 42).unwrap();
    assert!(session.ready().is_err());
    let _subscriber = session.subscriber("incoming", "Ping", 42).unwrap();
    session.ready().unwrap();
    assert!(session.publisher("outgoing", "Ping", 42).is_err());
    drop(publisher);
    assert!(session.ready().is_err());
}

#[test]
fn shutdown_cancels_receive_without_consuming_queued_messages() {
    let bus = LocalBus::new();
    let session = connect(&bus, "sender", "sender", "test");
    let publisher = session.publisher("outgoing", "Ping", 42).unwrap();
    let subscriber = session.subscriber("incoming", "Ping", 42).unwrap();
    publisher.send(vec![0]).unwrap();
    session.shutdown();
    assert_eq!(subscriber.receive(1000).unwrap(), None);
}

#[test]
fn foreign_arguments_require_prepared_paths_named_ports_and_exact_metadata() {
    let bus = LocalBus::new();
    assert!(
        bus.connect("sender".into(), "test".into(), "bindings.json".into())
            .is_err()
    );
    let session = connect(&bus, "sender", "sender", "test");
    for invalid in ["", "missing", "a/b", "*", "a b", "@launch"] {
        assert!(session.publisher(invalid, "Ping", 42).is_err());
        assert!(session.subscriber(invalid, "Ping", 42).is_err());
    }
    assert!(session.publisher("outgoing", "other.Ping", 42).is_err());
    assert!(session.subscriber("incoming", "Other", 42).is_err());
    let _publisher = session.publisher("outgoing", "Ping", 42).unwrap();
    assert!(session.publisher("outgoing", "Ping", 42).is_err());
}

#[test]
fn children_keep_session_and_bus_alive() {
    let (publisher, subscriber) = {
        let bus = LocalBus::new();
        let session = connect(&bus, "sender", "sender", "test");
        (
            session.publisher("outgoing", "Ping", 42).unwrap(),
            session.subscriber("incoming", "Ping", 42).unwrap(),
        )
    };
    publisher.send(Vec::new()).unwrap();
    assert_eq!(
        subscriber.receive(1000).unwrap().unwrap().payload,
        Vec::<u8>::new()
    );
}

#[test]
fn dynamic_outputs_refuse_duplicate_exact_transport_types() {
    let bus = LocalBus::new();
    let mut bindings = tempfile::NamedTempFile::with_suffix(".json").unwrap();
    bindings
        .write_all(
            br#"{"version":2,"namespace":"sender","endpoint_namespace":"sender",
        "inputs":{},"outputs":{"first":{"contract":{"type":"a.Ping"}},
        "second":{"contract":{"type":"a.Ping"}}}}"#,
        )
        .unwrap();
    // Reject ambiguous wire addresses before opening the session or any publisher.
    assert!(
        bus.connect("sender".into(), "test".into(), path(&bindings))
            .is_err()
    );
}

#[test]
fn distinct_packages_use_distinct_dynamic_wire_addresses() {
    // Given two same-named protobuf messages in different packages.
    let bus = LocalBus::new();
    let mut bindings = tempfile::NamedTempFile::with_suffix(".json").unwrap();
    bindings.write_all(br#"{"version":2,"namespace":"sender","endpoint_namespace":"sender",
        "inputs":{"first":{"contract":{"type":"a.Ping"},"sources":["sender"]},
        "second":{"contract":{"type":"b.Ping"},"sources":["sender"]}},
        "outputs":{"first":{"contract":{"type":"a.Ping"}},"second":{"contract":{"type":"b.Ping"}}}}"#).unwrap();
    let session = bus
        .connect("sender".into(), "test".into(), path(&bindings))
        .unwrap();
    let first = session.publisher("first", "a.Ping", 42).unwrap();
    let second = session.publisher("second", "b.Ping", 42).unwrap();
    let first_input = session.subscriber("first", "a.Ping", 42).unwrap();
    let second_input = session.subscriber("second", "b.Ping", 42).unwrap();
    // When both packages publish under the same source and fingerprint.
    first.send(vec![1]).unwrap();
    second.send(vec![2]).unwrap();
    // Then each exact type address receives only its own payload.
    assert_eq!(first_input.receive(1000).unwrap().unwrap().payload, vec![1]);
    assert_eq!(
        second_input.receive(1000).unwrap().unwrap().payload,
        vec![2]
    );
    session.ready().unwrap();
}

#[test]
fn dynamic_endpoints_apply_retention_replay_and_latest_buffer() {
    use reiny::engine::Engine as _;
    let bus = LocalBus::new();
    let mut bindings = tempfile::NamedTempFile::with_suffix(".json").unwrap();
    bindings.write_all(br#"{"version":2,"namespace":"sender","endpoint_namespace":"sender",
        "inputs":{"incoming":{"contract":{"type":"test.Ping","replay":"last","buffer":{"latest":1}},"sources":["sender"]}},
        "outputs":{"outgoing":{"contract":{"type":"test.Ping","retention":"last","qos":"sensor"}}}}"#).unwrap();
    let session = bus
        .connect("sender".into(), "test".into(), path(&bindings))
        .unwrap();
    let publisher = session.publisher("outgoing", "test.Ping", 42).unwrap();
    publisher.send(vec![1]).unwrap();
    let subscriber = session.subscriber("incoming", "test.Ping", 42).unwrap();
    assert_eq!(subscriber.receive(1000).unwrap().unwrap().payload, vec![1]);
    publisher.send(vec![2]).unwrap();
    publisher.send(vec![3]).unwrap();
    // An ordered presence query is a dispatcher barrier, not a timing delay.
    session
        .runtime
        .block_on(bus.engine.alive(
            &reiny::engine::Key::topic("test", Some("sender"), "test.Ping"),
            Duration::from_secs(1),
        ))
        .unwrap();
    assert_eq!(subscriber.receive(1000).unwrap().unwrap().payload, vec![3]);
    session.ready().unwrap();
}

#[test]
fn publisher_presence_ends_when_last_handle_is_dropped() {
    use reiny::engine::Engine as _;
    let bus = LocalBus::new();
    let session = connect(&bus, "sender", "sender", "test");
    let publisher = session.publisher("outgoing", "Ping", 42).unwrap();
    let key = reiny::engine::Key::topic("test", Some("sender"), "Ping");
    assert_eq!(
        session
            .runtime
            .block_on(bus.engine.alive(&key, Duration::from_secs(1)))
            .unwrap()
            .len(),
        1
    );
    drop(publisher);
    assert_eq!(
        session
            .runtime
            .block_on(bus.engine.alive(&key, Duration::from_secs(1)))
            .unwrap(),
        []
    );
}

#[test]
#[cfg(feature = "zenoh")]
fn network_sessions_exchange_protobuf_using_configuration_files() {
    use reiny::zenoh::Wait;
    let mut listener_config = tempfile::NamedTempFile::with_suffix(".json5").unwrap();
    listener_config.write_all(br#"{mode:"peer",listen:{endpoints:["tcp/127.0.0.1:0"]},scouting:{multicast:{enabled:false}}}"#).unwrap();
    let listener =
        reiny::zenoh::open(reiny::zenoh::Config::from_file(listener_config.path()).unwrap())
            .wait()
            .unwrap();
    let locator = listener
        .info()
        .locators()
        .wait()
        .into_iter()
        .next()
        .unwrap();
    let mut connector_config = tempfile::NamedTempFile::with_suffix(".json5").unwrap();
    write!(connector_config, r#"{{mode:"client",connect:{{endpoints:["{locator}"]}},scouting:{{multicast:{{enabled:false}}}}}}"#).unwrap();
    let receiver_bindings = prepared("network-receiver", "network-sender");
    let sender_bindings = prepared("network-sender", "network-receiver");
    let receiver = Session::open(
        "network-receiver".into(),
        "ffi-test".into(),
        Some(path(&connector_config)),
        path(&receiver_bindings),
    )
    .unwrap();
    let sender = Session::open(
        "network-sender".into(),
        "ffi-test".into(),
        Some(path(&connector_config)),
        path(&sender_bindings),
    )
    .unwrap();
    let observer = listener
        .declare_publisher("reiny/ffi-test/network-sender/Ping")
        .wait()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let _matching = observer
        .matching_listener()
        .callback(move |status| {
            if status.matching() {
                let _ = tx.try_send(());
            }
        })
        .wait()
        .unwrap();
    let subscriber = receiver
        .subscriber("incoming", "Ping", Ping::SCHEMA_FINGERPRINT)
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let publisher = sender
        .publisher("outgoing", "Ping", Ping::SCHEMA_FINGERPRINT)
        .unwrap();
    let ping = Ping {
        data: vec![0, 128, 255],
    };
    publisher.send(ping.encode_to_vec()).unwrap();
    let message = subscriber.receive(5000).unwrap().unwrap();
    assert_eq!(Ping::decode(message.payload.as_slice()).unwrap(), ping);
    assert_eq!(message.source, "network-sender");
    assert_eq!(message.schema, Some(Ping::SCHEMA_FINGERPRINT));
}
