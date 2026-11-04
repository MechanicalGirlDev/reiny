//! Explicit shared ownership for typed named-port publications and receives.

use std::sync::Arc;

use prost::Message;

use super::{Envelope, Publisher, Subscriber, decode, schema_matches};
use crate::engine::Payload;
use crate::{MessageType, Result};

impl<T: Message + MessageType + 'static> Publisher<T> {
    /// Publish an immutable shared value, preserving its allocation on Local.
    ///
    /// Unlike [`Self::send`], this does not take a serialized snapshot. The caller
    /// and all receivers must not mutate the protobuf value through interior
    /// aliases while any delivery or retained value can exist. Use `send` when
    /// that ownership contract cannot be met.
    ///
    /// Same-type Local [`Subscriber::recv_shared`] calls avoid protobuf encoding
    /// and decoding, including retained replay. Byte consumers, owned receives,
    /// other Rust types and byte-only engines use lazily cached protobuf bytes.
    /// Each publication is encoded at most once; byte copies can still occur.
    /// Like `send`, this future is lazy and retains the port's `QoS` and ordering.
    pub async fn send_shared(&self, message: Arc<T>) -> Result<()> {
        std::future::ready(self.core.send_payload(Payload::shared(message))).await
    }
}

impl<T: Message + Default + MessageType + 'static> Subscriber<T> {
    /// Receive shared ownership of the next value. Cancel-safe; `None` on shutdown.
    ///
    /// A same-type Local shared publication returns the publisher's allocation
    /// after checking its exact route and fingerprint. Otherwise protobuf bytes
    /// are decoded into a new `Arc`. Recipients must keep shared values immutable,
    /// including through interior aliases; see [`Publisher::send_shared`].
    pub async fn recv_shared(&mut self) -> Option<Arc<T>> {
        self.recv_shared_envelope().await.map(|e| e.value)
    }

    /// Shared receive with the same provenance and cancellation contract as
    /// [`Self::recv_envelope`]. Never mutates or clones the protobuf value.
    pub async fn recv_shared_envelope(&mut self) -> Option<Envelope<Arc<T>>> {
        loop {
            let sample = self.core.recv().await?;
            let source = sample.key.source.clone().unwrap_or_default();
            // Schema compatibility must be established before attempting downcast.
            if !schema_matches::<T>(&sample, &source, &mut self.warned) {
                continue;
            }
            let Some(value) = sample
                .payload
                .downcast::<T>()
                .or_else(|| decode::<T>(&sample).map(Arc::new))
            else {
                continue;
            };
            return Some(Envelope {
                value,
                source,
                source_port: sample.key.port()?.to_owned(),
                timestamp: sample.timestamp,
            });
        }
    }
}
