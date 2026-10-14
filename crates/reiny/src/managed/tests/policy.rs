use super::*;
use crate::bindings::{QosProfile, ReceiveBuffer, Replay, Retention};
use crate::engine::{
    BoxFuture, Callback, Caps, Guard, QueryCallback, RawPublisher, RawReplies, Sample,
};
use crate::{Durability, Result};
use std::any::Any;
use std::sync::Mutex;

#[test]
fn qos_profiles_serialize_exact_tags_and_reject_legacy_reliable() {
    assert_eq!(QosProfile::default(), QosProfile::Default);
    let default: EndpointContract =
        serde_json::from_str(r#"{"type":"test.Probe"}"#).expect("default contract");
    assert_eq!(default.qos, QosProfile::Default);
    for (tag, profile) in [
        ("default", QosProfile::Default),
        ("command", QosProfile::Command),
        ("sensor", QosProfile::Sensor),
    ] {
        let encoded = format!("\"{tag}\"");
        assert_eq!(
            serde_json::from_str::<QosProfile>(&encoded).expect("profile"),
            profile
        );
        assert_eq!(
            serde_json::to_string(&profile).expect("serialized profile"),
            encoded
        );
    }
    assert!(serde_json::from_str::<QosProfile>("\"reliable\"").is_err());
    assert!(
        serde_json::from_str::<EndpointContract>(r#"{"type":"test.Probe","qos":"reliable"}"#)
            .is_err()
    );
    let mut rpc = contract("test.Request");
    rpc.kind = crate::bindings::PortKind::Rpc;
    rpc.response = Some("test.Response".into());
    assert!(rpc.validate(false).is_ok());
    for profile in [QosProfile::Command, QosProfile::Sensor] {
        rpc.qos = profile;
        assert!(rpc.validate(false).is_err());
    }
}

#[derive(Default)]
struct Declarations {
    subscriptions: Vec<Key>,
    publishers: Vec<(Key, Qos)>,
    publisher_events: Vec<(Key, &'static str)>,
}

// Instrument the real ordered Local engine; every operation still reaches the bus.
#[derive(Default)]
struct Observed {
    local: Local,
    declarations: Mutex<Declarations>,
}

impl Engine for Observed {
    fn caps(&self) -> Caps {
        self.local.caps()
    }
    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        {
            let mut declarations = self.declarations.lock().expect("declarations");
            declarations.publishers.push((key.clone(), *qos));
            declarations.publisher_events.push((key.clone(), "sender"));
        }
        self.local.publisher(key, qos)
    }
    fn subscribe(&self, key: &Key, callback: Callback<Sample>) -> Result<Guard> {
        self.declarations
            .lock()
            .expect("declarations")
            .subscriptions
            .push(key.clone());
        self.local.subscribe(key, callback)
    }
    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        self.declarations
            .lock()
            .expect("declarations")
            .publisher_events
            .push((key.clone(), "presence"));
        self.local.declare_alive(key)
    }
    fn alive(&self, key: &Key, duration: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        self.local.alive(key, duration)
    }
    fn watch_alive(&self, key: &Key, callback: Callback<Presence>) -> Result<Guard> {
        self.local.watch_alive(key, callback)
    }
    fn respond(&self, key: &Key, callback: QueryCallback) -> Result<Guard> {
        self.declarations
            .lock()
            .expect("declarations")
            .publisher_events
            .push((key.clone(), "queryable"));
        self.local.respond(key, callback)
    }
    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        self.local.query(key, params)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[tokio::test]
async fn typed_and_dynamic_publishers_share_qos_retention_and_presence_order() {
    for dynamic in [false, true] {
        for retention in [Retention::Volatile, Retention::Last] {
            let bus = Arc::new(Observed::default());
            let mut binding = bindings(SENDER);
            let mut output = contract("test.Probe");
            output.retention = retention;
            binding
                .outputs
                .insert("outgoing".into(), OutputBinding { contract: output });
            let sender = ModuleContext::open(options(bus.clone(), binding))
                .await
                .expect("sender");
            let key = Key::output(DOMAIN, SENDER, "outgoing", Probe::TYPE_NAME);
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let _subscription = bus
                .subscribe(
                    &key,
                    Box::new(move |sample| {
                        tx.send(sample).expect("live receiver");
                    }),
                )
                .expect("subscription");
            let typed = if dynamic {
                None
            } else {
                Some(sender.output::<Probe>("outgoing").expect("typed"))
            };
            let raw = if dynamic {
                Some(
                    sender
                        .raw_output("outgoing", "test.Probe", Probe::SCHEMA_FINGERPRINT)
                        .expect("dynamic"),
                )
            } else {
                None
            };
            for value in [7, 8] {
                let message = Probe { value };
                if let Some(publisher) = &typed {
                    publisher.send(message).await.expect("typed send");
                } else {
                    raw.as_ref()
                        .expect("dynamic publisher")
                        .send(message.encode_to_vec())
                        .expect("dynamic send");
                }
                let sample = timeout(PATIENCE, rx.recv())
                    .await
                    .expect("live sample")
                    .expect("open");
                assert_eq!(sample.payload, Probe { value }.encode_to_vec());
                assert_eq!(
                    sample.attachment,
                    Some(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec())
                );
            }
            {
                let declarations = bus.declarations.lock().expect("declarations");
                let mut qos = Qos::DEFAULT;
                qos.durability = if retention == Retention::Last {
                    Durability::TransientLocal
                } else {
                    Durability::Volatile
                };
                assert_eq!(declarations.publishers, vec![(key.clone(), qos)]);
                let events: Vec<_> = declarations
                    .publisher_events
                    .iter()
                    .filter(|(event_key, _)| event_key == &key)
                    .map(|(_, event)| *event)
                    .collect();
                assert_eq!(
                    events,
                    if retention == Retention::Last {
                        vec!["sender", "queryable", "presence"]
                    } else {
                        vec!["sender", "presence"]
                    }
                );
            }
            assert_retained_reply(bus.as_ref(), &key, retention).await;
        }
    }
}

#[tokio::test]
async fn typed_and_dynamic_reject_explicit_congestion_before_side_effects() {
    for dynamic in [false, true] {
        for profile in [QosProfile::Command, QosProfile::Sensor] {
            for retention in [Retention::Volatile, Retention::Last] {
                let bus = Arc::new(Observed::default());
                let mut binding = bindings(SENDER);
                let mut output = contract("test.Probe");
                output.qos = profile;
                output.retention = retention;
                binding
                    .outputs
                    .insert("outgoing".into(), OutputBinding { contract: output });
                let sender = ModuleContext::open(options(bus.clone(), binding))
                    .await
                    .expect("sender");
                let before = bus
                    .declarations
                    .lock()
                    .expect("declarations")
                    .publisher_events
                    .clone();
                let result = if dynamic {
                    sender
                        .raw_output("outgoing", "test.Probe", Probe::SCHEMA_FINGERPRINT)
                        .map(|_| ())
                } else {
                    sender.output::<Probe>("outgoing").map(|_| ())
                };
                assert!(
                    result
                        .expect_err("Local rejects explicit congestion policies")
                        .to_string()
                        .contains("congestion")
                );
                assert_eq!(
                    bus.declarations
                        .lock()
                        .expect("declarations")
                        .publisher_events,
                    before,
                    "reject before declaring sender, retention, or presence"
                );
            }
        }
    }
}

async fn assert_retained_reply(bus: &Observed, key: &Key, retention: Retention) {
    let mut replies = bus
        .query(
            key,
            QueryParams {
                payload: None,
                attachment: None,
                timeout: PATIENCE,
            },
        )
        .expect("retained query");
    let reply = timeout(PATIENCE, replies.next())
        .await
        .expect("query complete");
    if retention == Retention::Last {
        let sample = reply.expect("retained reply").expect("successful reply");
        assert_eq!(sample.payload, Probe { value: 8 }.encode_to_vec());
        assert_eq!(
            sample.attachment,
            Some(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec())
        );
        assert!(
            timeout(PATIENCE, replies.next())
                .await
                .expect("query complete")
                .is_none()
        );
    } else {
        assert!(reply.is_none());
    }
}

#[tokio::test]
async fn fan_in_declares_exact_subscriptions_and_one_shared_latest_buffer() {
    let bus = Arc::new(Observed::default());
    let sources = [SENDER, "deployment/second"];
    let mut binding = bindings(RECEIVER);
    let mut incoming = input(SENDER);
    incoming.sources = sources.map(|source| endpoint(source, "outgoing")).to_vec();
    incoming.contract.buffer = ReceiveBuffer::Latest(2);
    binding.inputs.insert("incoming".into(), incoming);
    let receiver = ModuleContext::open(options(bus.clone(), binding))
        .await
        .expect("receiver");
    let mut subscriber = receiver.input::<Probe>("incoming").expect("fan in");
    assert_eq!(
        bus.declarations.lock().expect("declarations").subscriptions,
        sources.map(|source| Key::output(DOMAIN, source, "outgoing", Probe::TYPE_NAME)),
    );
    let first = producer(bus.clone(), sources[0]).await;
    let second = producer(bus.clone(), sources[1]).await;
    let outsider = producer(bus.clone(), "deployment/second/child").await;
    let first = first.output::<Probe>("outgoing").expect("first");
    let second = second.output::<Probe>("outgoing").expect("second");
    let outsider = outsider.output::<Probe>("outgoing").expect("outsider");
    first.send(Probe { value: 1 }).await.expect("send");
    second.send(Probe { value: 2 }).await.expect("send");
    outsider.send(Probe { value: 99 }).await.expect("send");
    first.send(Probe { value: 3 }).await.expect("send");
    // Local processes this query after all preceding puts; no timing-dependent queue inspection.
    bus.alive(&Key::launch(DOMAIN, None), PATIENCE)
        .await
        .expect("ordered barrier");
    assert_eq!(
        subscriber.stats(),
        crate::SubscriberStats {
            received: 3,
            dropped: 1,
            blocked: 0
        }
    );
    let a = timeout(PATIENCE, subscriber.recv_envelope())
        .await
        .expect("receive")
        .expect("sample");
    let b = timeout(PATIENCE, subscriber.recv_envelope())
        .await
        .expect("receive")
        .expect("sample");
    assert_eq!((a.source.as_str(), a.value.value), (sources[1], 2));
    assert_eq!((b.source.as_str(), b.value.value), (sources[0], 3));
}

#[tokio::test]
async fn manifest_controls_publisher_qos_retention_and_each_source_replay() {
    let bus = Arc::new(Observed::default());
    let sources = [SENDER, "deployment/second"];
    let mut publishers = Vec::new();
    for (source, qos) in sources
        .into_iter()
        .zip([QosProfile::Default, QosProfile::Default])
    {
        let mut binding = bindings(source);
        let mut output = contract("test.Probe");
        output.qos = qos;
        output.retention = Retention::Last;
        binding
            .outputs
            .insert("outgoing".into(), OutputBinding { contract: output });
        let publisher = ModuleContext::open(options(bus.clone(), binding))
            .await
            .expect("publisher");
        publishers.push(publisher.output::<Probe>("outgoing").expect("output"));
    }
    {
        let declarations = &bus.declarations.lock().expect("declarations").publishers;
        assert_eq!(
            declarations[0].1,
            Qos {
                durability: Durability::TransientLocal,
                ..Qos::DEFAULT
            }
        );
        assert_eq!(
            declarations[1].1,
            Qos {
                durability: Durability::TransientLocal,
                ..Qos::DEFAULT
            }
        );
    }
    publishers[0]
        .send(Probe { value: 4 })
        .await
        .expect("retained");
    publishers[1]
        .send(Probe { value: 5 })
        .await
        .expect("retained");
    let mut binding = bindings(RECEIVER);
    let mut incoming = input(SENDER);
    incoming.sources = sources.map(|source| endpoint(source, "outgoing")).to_vec();
    incoming.contract.replay = Replay::Last;
    binding.inputs.insert("incoming".into(), incoming);
    let receiver = ModuleContext::open(options(bus.clone(), binding))
        .await
        .expect("receiver");
    let mut subscriber = receiver.input::<Probe>("incoming").expect("replay");
    let mut replayed = BTreeMap::new();
    for _ in sources {
        let sample = timeout(PATIENCE, subscriber.recv_envelope())
            .await
            .expect("replay")
            .expect("sample");
        replayed.insert(sample.source, sample.value.value);
    }
    assert_eq!(
        replayed,
        BTreeMap::from([(sources[0].into(), 4), (sources[1].into(), 5)])
    );
}

#[tokio::test]
async fn raw_access_is_rejected_before_any_transport_declaration() {
    let bus = Arc::new(Observed::default());
    let cloudy = consumer(bus.clone()).await;
    assert!(cloudy.publish::<Probe>().is_err());
    assert!(cloudy.subscribe::<Probe>().is_err());
    assert!(cloudy.publisher::<Probe>().latched().build().is_err());
    assert!(cloudy.subscriber::<Probe>().from(SENDER).build().is_err());
    assert!(cloudy.raw_subscriber(Probe::TYPE_NAME).build().is_err());
    assert!(cloudy.serve::<super::rpc::RequestProbe>().is_err());
    assert!(
        cloudy
            .caller::<super::rpc::RequestProbe>()
            .to(SENDER)
            .build()
            .is_err()
    );
    let second = cloudy
        .with_engine(Arc::new(Local::new()))
        .await
        .expect("bound second engine");
    assert!(second.input::<Probe>("undefined").is_err());
    assert!(second.output::<Probe>("undefined").is_err());
    #[cfg(feature = "zenoh")]
    assert!(cloudy.session().is_none());
    {
        let declarations = bus.declarations.lock().expect("declarations");
        assert_eq!(declarations.subscriptions.len(), 0);
        assert_eq!(declarations.publishers.len(), 0);
    }
    assert_eq!(
        bus.alive(&Key::all(DOMAIN), PATIENCE)
            .await
            .expect("tokens")
            .len(),
        0
    );
}

#[tokio::test]
async fn managed_engine_cannot_be_reopened_standalone_or_used_by_old_builders() {
    let bus = Arc::new(Local::new());
    let mut standalone = RuntimeOptions::new("before");
    standalone.domain = DOMAIN.into();
    standalone.module_report_path = None;
    standalone.engine = Some(bus.clone());
    assert!(ModuleContext::open(standalone).await.is_err());
    let managed = consumer(bus.clone()).await;
    let mut reopening = RuntimeOptions::new("escape");
    reopening.domain = DOMAIN.into();
    reopening.engine = Some(bus.clone());
    reopening.module_report_path = None;
    assert!(ModuleContext::open(reopening).await.is_err());
    assert_eq!(
        bus.alive(&Key::launch(DOMAIN, Some("escape")), PATIENCE)
            .await
            .expect("launches")
            .len(),
        0
    );
    drop(managed);
    let mut reopening = RuntimeOptions::new("escape");
    reopening.module_report_path = None;
    reopening.engine = Some(bus.clone());
    assert!(ModuleContext::open(reopening).await.is_err());
}
