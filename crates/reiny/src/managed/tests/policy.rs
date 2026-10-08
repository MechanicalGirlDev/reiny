use super::*;
use crate::bindings::{QosProfile, ReceiveBuffer, Replay, Retention};
use crate::engine::{
    BoxFuture, Callback, Caps, Guard, QueryCallback, RawPublisher, RawReplies, Sample,
};
use crate::{Durability, Result};
use std::any::Any;
use std::sync::Mutex;

#[derive(Default)]
struct Declarations {
    subscriptions: Vec<Key>,
    publishers: Vec<(Key, Qos)>,
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
        self.declarations
            .lock()
            .expect("declarations")
            .publishers
            .push((key.clone(), *qos));
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
        self.local.declare_alive(key)
    }
    fn alive(&self, key: &Key, duration: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        self.local.alive(key, duration)
    }
    fn watch_alive(&self, key: &Key, callback: Callback<Presence>) -> Result<Guard> {
        self.local.watch_alive(key, callback)
    }
    fn respond(&self, key: &Key, callback: QueryCallback) -> Result<Guard> {
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
async fn fan_in_declares_exact_subscriptions_and_one_shared_latest_buffer() {
    let bus = Arc::new(Observed::default());
    let sources = [SENDER, "deployment/second"];
    let mut binding = bindings(RECEIVER);
    let mut incoming = input(SENDER);
    incoming.sources = sources.map(str::to_string).to_vec();
    incoming.contract.buffer = ReceiveBuffer::Latest(2);
    binding.inputs.insert("incoming".into(), incoming);
    let receiver = Cloudy::open(options(bus.clone(), binding))
        .await
        .expect("receiver");
    let mut subscriber = receiver.input::<Probe>("incoming").expect("fan in");
    assert_eq!(
        bus.declarations.lock().expect("declarations").subscriptions,
        sources.map(|source| Key::topic(DOMAIN, Some(source), Probe::TYPE)),
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
        .zip([QosProfile::Sensor, QosProfile::Reliable])
    {
        let mut binding = bindings(source);
        let mut output = contract("test.Probe");
        output.qos = qos;
        output.retention = Retention::Last;
        binding
            .outputs
            .insert("outgoing".into(), OutputBinding { contract: output });
        let publisher = Cloudy::open(options(bus.clone(), binding))
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
                ..Qos::SENSOR
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
    incoming.sources = sources.map(str::to_string).to_vec();
    incoming.contract.replay = Replay::Last;
    binding.inputs.insert("incoming".into(), incoming);
    let receiver = Cloudy::open(options(bus.clone(), binding))
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
    assert!(cloudy.raw_subscriber(Probe::TYPE).build().is_err());
    assert!(cloudy.serve::<super::rpc::RequestProbe>().is_err());
    assert!(
        cloudy
            .caller::<super::rpc::RequestProbe>()
            .to(SENDER)
            .build()
            .is_err()
    );
    assert!(cloudy.engine().is_err());
    assert!(cloudy.with_engine(Arc::new(Local::new())).await.is_err());
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
    let standalone = Cloudy::open(standalone).await.expect("initial standalone");
    let builder = standalone.publisher::<Probe>();
    let managed = consumer(bus.clone()).await;
    assert!(builder.build().is_err());
    assert!(standalone.engine().is_err());
    let mut reopening = RuntimeOptions::new("escape");
    reopening.domain = DOMAIN.into();
    reopening.engine = Some(bus.clone());
    reopening.module_report_path = None;
    assert!(Cloudy::open(reopening).await.is_err());
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
    assert!(Cloudy::open(reopening).await.is_err());
}
