use super::*;
use crate::engine::{
    BoxFuture, Callback, Caps, Guard, QueryCallback, RawPublisher, RawQuery, RawReplies,
    ReplyResult, Sample,
};
use crate::{Qos, Result};
use std::any::Any;

// An external-style Engine implementing only the original required primitives.
// The wrappers deliberately expose no native overrides on any returned handle.
struct ByteOnly(Local);
struct BytePublisher(Box<dyn RawPublisher>);
struct ByteQuery(Box<dyn RawQuery>);
struct ByteReplies(Box<dyn RawReplies>);

impl RawPublisher for BytePublisher {
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()> {
        self.0.put(payload, attachment)
    }
}

impl RawQuery for ByteQuery {
    fn key(&self) -> &Key {
        self.0.key()
    }
    fn payload(&self) -> Option<&[u8]> {
        self.0.payload()
    }
    fn attachment(&self) -> Option<&[u8]> {
        self.0.attachment()
    }
    fn reply(
        self: Box<Self>,
        key: &Key,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> Result<()> {
        self.0.reply(key, payload, attachment)
    }
    fn reply_err(self: Box<Self>, message: Vec<u8>) -> Result<()> {
        self.0.reply_err(message)
    }
}

impl RawReplies for ByteReplies {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        self.0.next()
    }
}

impl Engine for ByteOnly {
    fn caps(&self) -> Caps {
        self.0.caps()
    }
    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        Ok(Box::new(BytePublisher(self.0.publisher(key, qos)?)))
    }
    fn subscribe(&self, key: &Key, callback: Callback<Sample>) -> Result<Guard> {
        self.0.subscribe(key, callback)
    }
    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        self.0.declare_alive(key)
    }
    fn alive(&self, key: &Key, timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        self.0.alive(key, timeout)
    }
    fn watch_alive(&self, key: &Key, callback: Callback<Presence>) -> Result<Guard> {
        self.0.watch_alive(key, callback)
    }
    fn respond(&self, key: &Key, callback: QueryCallback) -> Result<Guard> {
        self.0
            .respond(key, Box::new(move |q| callback(Box::new(ByteQuery(q)))))
    }
    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        Ok(Box::new(ByteReplies(self.0.query(key, params)?)))
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[tokio::test]
async fn external_byte_only_engine_defaults_support_shared_live_and_replay() {
    let bus = Arc::new(ByteOnly(Local::new()));
    let sender = sender(bus.clone(), true).await;
    let receiver = consumer(bus.clone()).await;
    let output = sender.output::<Counted>("outgoing").expect("output");
    let mut live = receiver.input::<Counted>("incoming").expect("input");
    let value = Arc::new(Counted::new(17));
    output.send_shared(Arc::clone(&value)).await.expect("send");
    let actual = timeout(PATIENCE, live.recv_shared())
        .await
        .expect("live")
        .expect("value");
    assert!(!Arc::ptr_eq(&value, &actual));
    assert_eq!((actual.value(), actual.decoded_fields), (17, 1));
    let late = super::receiver(bus, true, ReceiveBuffer::Fifo).await;
    let mut replay = late.input::<Counted>("first").expect("replay");
    let actual = timeout(PATIENCE, replay.recv_shared())
        .await
        .expect("replay")
        .expect("value");
    assert_eq!((actual.value(), actual.decoded_fields), (17, 1));
    assert_eq!(value.encodes(), 1);
}

#[tokio::test]
async fn legacy_send_keeps_send_time_snapshot_for_live_raw_and_retained_receivers() {
    let bus = Arc::new(Local::new());
    let sender = sender(bus.clone(), true).await;
    let receiver = receiver(bus.clone(), false, ReceiveBuffer::Fifo).await;
    let output = sender.output::<Counted>("outgoing").expect("output");
    let mut live = receiver.input::<Counted>("first").expect("live");
    let mut raw = receiver
        .raw_input(
            "raw",
            Counted::<0>::TYPE_NAME,
            Counted::<0>::SCHEMA_FINGERPRINT,
        )
        .expect("raw");
    let message = Counted::<0>::new(7);
    let mutable_alias = Arc::clone(&message.value);
    let encodes = Arc::clone(&message.encodes);

    output.send(message).await.expect("snapshot");
    mutable_alias.store(99, Ordering::Relaxed);

    let actual = timeout(PATIENCE, live.recv_shared())
        .await
        .expect("live")
        .expect("value");
    let raw = timeout(PATIENCE, raw.recv())
        .await
        .expect("raw")
        .expect("value");
    assert_eq!((actual.value(), actual.decoded_fields), (7, 1));
    assert_eq!(raw.payload, Probe { value: 7 }.encode_to_vec());
    let late = super::receiver(bus, true, ReceiveBuffer::Fifo).await;
    let mut replay = late.input::<Counted>("first").expect("replay");
    let actual = timeout(PATIENCE, replay.recv_shared())
        .await
        .expect("replay")
        .expect("value");
    assert_eq!((actual.value(), actual.decoded_fields), (7, 1));
    assert_eq!(encodes.load(Ordering::Relaxed), 1);
}
