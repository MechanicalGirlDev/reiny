//! Zenoh typed subscriber

use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;
use tracing::{debug, warn};
use zenoh::Session;
use zenoh::handlers::FifoChannelHandler;
use zenoh::pubsub::Subscriber;
use zenoh::sample::Sample;

use crate::CommError;

use super::HosSession;

/// A typed Zenoh subscriber for protobuf messages
pub struct ZenohSubscriber<T: Message + Default> {
    subscriber: Subscriber<FifoChannelHandler<Sample>>,
    topic: String,
    // Keep session alive
    _session: Arc<Session>,
    _marker: PhantomData<T>,
}

impl<T: Message + Default> ZenohSubscriber<T> {
    /// Create a new subscriber for the given topic
    pub async fn new(session: &HosSession, topic: impl Into<String>) -> Result<Self, CommError> {
        let topic = topic.into();
        let subscriber = session.inner().declare_subscriber(topic.clone()).await?;
        Ok(Self {
            subscriber,
            topic,
            _session: session.inner().clone(),
            _marker: PhantomData,
        })
    }

    /// Receive a message asynchronously.
    ///
    /// Returns `None` only when the subscriber's channel is **closed**. A sample
    /// that fails to decode (e.g. a malformed or wrong-schema payload) is warned
    /// and skipped, and reception continues with the next sample — a single bad
    /// sample must not be mistaken for a closed channel. This matters for callers
    /// that treat `None` as a terminal condition (e.g. an event-driven control
    /// loop driven solely by this stream).
    pub async fn recv_async(&self) -> Option<T> {
        loop {
            match self.subscriber.recv_async().await {
                Ok(sample) => {
                    if let Some(msg) = self.decode_sample(&sample) {
                        return Some(msg);
                    }
                    // decode failure already warned in `decode_sample`; skip it.
                }
                Err(_) => return None, // channel closed
            }
        }
    }

    /// Try to receive a message without blocking
    ///
    /// Returns None if no message is available.
    pub fn try_recv(&self) -> Option<T> {
        match self.subscriber.try_recv() {
            Ok(Some(sample)) => self.decode_sample(&sample),
            _ => None,
        }
    }

    /// Receive a message with blocking
    ///
    /// Returns None if the subscriber is closed.
    pub fn recv(&self) -> Option<T> {
        match self.subscriber.recv() {
            Ok(sample) => self.decode_sample(&sample),
            Err(_) => None,
        }
    }

    fn decode_sample(&self, sample: &Sample) -> Option<T> {
        let bytes = sample.payload().to_bytes();
        debug!("Received {} bytes from {}", bytes.len(), self.topic);
        match T::decode(bytes.as_ref()) {
            Ok(msg) => Some(msg),
            Err(e) => {
                warn!("Failed to decode message from {}: {}", self.topic, e);
                None
            }
        }
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }
}
