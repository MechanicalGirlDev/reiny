//! Zenoh typed publisher

use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;
use tracing::debug;
use zenoh::Session;
use zenoh::pubsub::Publisher;

use crate::CommError;

use super::HosSession;

/// A typed Zenoh publisher for protobuf messages
pub struct ZenohPublisher<T: Message> {
    publisher: Publisher<'static>,
    topic: String,
    // Keep session alive
    _session: Arc<Session>,
    _marker: PhantomData<T>,
}

impl<T: Message> ZenohPublisher<T> {
    /// Create a new publisher for the given topic
    pub async fn new(session: &HosSession, topic: impl Into<String>) -> Result<Self, CommError> {
        let topic = topic.into();
        let publisher = session.inner().declare_publisher(topic.clone()).await?;
        Ok(Self {
            publisher,
            topic,
            _session: session.inner().clone(),
            _marker: PhantomData,
        })
    }

    /// Publish a message
    pub async fn put(&self, message: &T) -> Result<(), CommError> {
        let buf = message.encode_to_vec();
        debug!("Publishing {} bytes to {}", buf.len(), self.topic);
        self.publisher.put(buf).await?;
        Ok(())
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }
}
