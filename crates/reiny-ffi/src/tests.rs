//! Real engine regression coverage for the shared foreign-language facade.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use prost::Message as _;
use reiny::Topic;

use super::*;

#[derive(Clone, PartialEq, prost::Message)]
struct Ping {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

impl Topic for Ping {
    const TYPE: &'static str = "Ping";
    const SCHEMA: Option<u64> = Some(u64::MAX);
}

#[test]
fn ffi_receives_typed_rust_protobuf_with_provenance() {
    // Given: a typed Rust launch and a foreign subscriber on the same real engine.
    let bus = LocalBus::new();
    let foreign = bus.connect("foreign".into(), "test".into()).unwrap();
    let mut options = RuntimeOptions::new("rust");
    options.domain = "test".into();
    options.engine = Some(bus.engine.clone());
    options.install_tracing = false;
    let native = foreign.runtime.block_on(Cloudy::open(options)).unwrap();
    let publisher = native.publish::<Ping>().unwrap();
    let subscriber = foreign.subscriber("Ping".into(), None).unwrap();
    let ping = Ping {
        data: vec![0, 1, 255, 0],
    };

    // When: Rust publishes a normal typed message.
    foreign
        .runtime
        .block_on(publisher.send(ping.clone()))
        .unwrap();
    let message = subscriber.receive(1000).unwrap().unwrap();

    // Then: the foreign API preserves the Protobuf bytes, fingerprint, and source.
    assert_eq!(Ping::decode(message.payload.as_slice()).unwrap(), ping);
    assert_eq!(message.source, "rust");
    assert_eq!(message.schema, Some(u64::MAX));
}

#[test]
fn rust_receives_ffi_encoded_protobuf() {
    // Given: a Rust subscriber and a foreign publisher.
    let bus = LocalBus::new();
    let foreign = bus.connect("foreign".into(), "test".into()).unwrap();
    let mut options = RuntimeOptions::new("rust");
    options.domain = "test".into();
    options.engine = Some(bus.engine.clone());
    options.install_tracing = false;
    let native = foreign.runtime.block_on(Cloudy::open(options)).unwrap();
    let mut subscriber = native.subscribe::<Ping>().unwrap();
    let publisher = foreign.publisher("Ping".into(), Ping::SCHEMA).unwrap();
    let ping = Ping {
        data: vec![0, 128, 255],
    };

    // When: a foreign launch publishes serialized Protobuf.
    publisher.send(ping.encode_to_vec()).unwrap();
    let message = foreign.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), subscriber.recv_envelope())
            .await
            .unwrap()
            .unwrap()
    });

    // Then: Rust's typed path decodes it without any alternate transport.
    assert_eq!(message.value, ping);
    assert_eq!(message.source, "foreign");
}

#[test]
fn subscription_filters_source_and_domain() {
    // Given: declarations for several sources and domains.
    let bus = LocalBus::new();
    let a = bus.connect("a".into(), "test".into()).unwrap();
    let b = bus.connect("b".into(), "test".into()).unwrap();
    let other = bus.connect("a".into(), "other".into()).unwrap();
    let subscription = a.subscriber("Ping".into(), Some("b".into())).unwrap();
    let wrong_source = a.publisher("Ping".into(), None).unwrap();
    let wrong_domain = other.publisher("Ping".into(), None).unwrap();
    let expected = b.publisher("Ping".into(), None).unwrap();

    // When: filtered messages precede the matching one in the ordered local dispatcher.
    wrong_source.send(vec![1]).unwrap();
    wrong_domain.send(vec![2]).unwrap();
    expected.send(vec![3]).unwrap();

    // Then: only the expected source in the expected domain reaches the FIFO.
    assert_eq!(
        subscription.receive(1000).unwrap().unwrap().payload,
        vec![3]
    );
}

#[test]
fn publisher_presence_ends_when_last_handle_is_dropped() {
    // Given: a publisher with its normal liveliness declaration.
    let bus = LocalBus::new();
    let session = bus.connect("sender".into(), "test".into()).unwrap();
    let publisher = session.publisher("Ping".into(), None).unwrap();
    assert_eq!(session.publishers("Ping".into(), 1000).unwrap(), ["sender"]);

    // When: the last publisher reference is released.
    drop(publisher);

    // Then: an ordered presence query observes its undeclaration.
    assert_eq!(
        session.publishers("Ping".into(), 1000).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn shutdown_cancels_receive_without_consuming_queued_messages() {
    // Given: a queued sample on a subscription.
    let bus = LocalBus::new();
    let session = bus.connect("sender".into(), "test".into()).unwrap();
    let publisher = session.publisher("Ping".into(), None).unwrap();
    let subscription = session.subscriber("Ping".into(), None).unwrap();
    publisher.send(vec![0]).unwrap();

    // When: the session requests shutdown.
    session.shutdown();

    // Then: shutdown wins even if data is already queued.
    assert_eq!(subscription.receive(1000).unwrap(), None);
}

#[test]
fn foreign_arguments_reject_invalid_key_segments() {
    // Given: a normal connected session.
    let bus = LocalBus::new();
    let session = bus.connect("sender".into(), "test".into()).unwrap();

    // When/Then: malformed foreign names cannot address unintended keys.
    for invalid in ["", "a/b", "*", "a b", "@launch", "a?"] {
        assert!(session.publisher(invalid.into(), None).is_err());
        assert!(
            session
                .subscriber("Ping".into(), Some(invalid.into()))
                .is_err()
        );
        assert!(bus.connect(invalid.into(), "test".into()).is_err());
        assert!(bus.connect("sender".into(), invalid.into()).is_err());
    }
}

#[test]
fn children_keep_session_and_bus_alive() {
    // Given: children whose language-side parent references have disappeared.
    let (publisher, subscription) = {
        let bus = LocalBus::new();
        let session = bus.connect("sender".into(), "test".into()).unwrap();
        (
            session.publisher("Ping".into(), None).unwrap(),
            session.subscriber("Ping".into(), None).unwrap(),
        )
    };

    // When: the child publisher sends an empty binary payload.
    publisher.send(Vec::new()).unwrap();

    // Then: the underlying runtime and engine remain available.
    assert_eq!(
        subscription.receive(1000).unwrap().unwrap().payload,
        Vec::<u8>::new()
    );
}

#[test]
#[cfg(feature = "zenoh")]
fn network_sessions_exchange_protobuf_using_configuration_files() {
    use reiny::zenoh::Wait;
    use std::io::Write;

    // Given: two real network sessions, multicast disabled and an ephemeral listener.
    let mut listener_config = tempfile::NamedTempFile::with_suffix(".json5").unwrap();
    listener_config
        .write_all(
            br#"{
                mode: "peer",
                listen: { endpoints: ["tcp/127.0.0.1:0"] },
                scouting: { multicast: { enabled: false } }
            }"#,
        )
        .unwrap();
    let receiver = Session::open(
        "network-receiver".into(),
        "ffi-test".into(),
        Some(listener_config.path().to_str().unwrap().into()),
    )
    .unwrap();
    let locator = receiver
        .cloudy
        .session()
        .unwrap()
        .info()
        .locators()
        .wait()
        .into_iter()
        .next()
        .unwrap();
    let mut connector_config = tempfile::NamedTempFile::with_suffix(".json5").unwrap();
    write!(
        connector_config,
        r#"{{
            mode: "client",
            connect: {{ endpoints: ["{locator}"] }},
            scouting: {{ multicast: {{ enabled: false }} }}
        }}"#
    )
    .unwrap();
    let sender = Session::open(
        "network-sender".into(),
        "ffi-test".into(),
        Some(connector_config.path().to_str().unwrap().into()),
    )
    .unwrap();
    let native = sender.cloudy.session().unwrap();
    let observer = native
        .declare_publisher("reiny/ffi-test/network-sender/Ping")
        .wait()
        .unwrap();
    // Subscribe to the exact readiness event before declaring the foreign subscription.
    let (matched_tx, matched_rx) = std::sync::mpsc::sync_channel(1);
    let _matching = observer
        .matching_listener()
        .callback(move |status| {
            if status.matching() {
                matched_tx.try_send(()).unwrap();
            }
        })
        .wait()
        .unwrap();
    let subscription = receiver
        .subscriber("Ping".into(), Some("network-sender".into()))
        .unwrap();
    matched_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let publisher = sender.publisher("Ping".into(), Ping::SCHEMA).unwrap();
    let ping = Ping {
        data: vec![0, 128, 255],
    };

    // When: the foreign publisher sends across a real TCP session.
    publisher.send(ping.encode_to_vec()).unwrap();

    // Then: the foreign subscriber receives the same bytes and metadata.
    let message = subscription.receive(5000).unwrap().unwrap();
    assert_eq!(Ping::decode(message.payload.as_slice()).unwrap(), ping);
    assert_eq!(message.source, "network-sender");
    assert_eq!(message.schema, Ping::SCHEMA);
}
