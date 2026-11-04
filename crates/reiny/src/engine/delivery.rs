//! Optional shared payloads on the existing ordered engine path.

use std::any::Any;
use std::fmt;
use std::sync::{Arc, OnceLock};

use prost::Message;

use super::{Key, Sample};

/// A wire payload or an explicitly immutable shared protobuf value.
///
/// Shared values keep their Rust allocation on native-capable engines. Bytes are
/// encoded at most once per publication, only when a consumer needs them.
#[derive(Clone, Debug)]
pub struct Payload(PayloadKind);

#[derive(Clone, Debug)]
enum PayloadKind {
    Bytes(Vec<u8>),
    Shared(Arc<Shared>),
}

struct Shared {
    value: Arc<dyn NativeMessage>,
    bytes: OnceLock<Vec<u8>>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("encoded", &self.bytes.get().is_some())
            .finish_non_exhaustive()
    }
}

// Type erasure belongs only at the engine boundary. Downcasting uses Any's exact
// Rust type identity, never a schema name or unchecked pointer conversion.
trait NativeMessage: Send + Sync {
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn encode(&self) -> Vec<u8>;
}

impl<T: Message + 'static> NativeMessage for T {
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn encode(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

impl Payload {
    /// Share a value without encoding it.
    ///
    /// The caller and every recipient must keep the protobuf value immutable,
    /// including through interior-mutable aliases, while any delivery or retained
    /// value can exist. `Arc` alone does not enforce that contract. Violating it
    /// can make native and encoded consumers observe different values.
    #[must_use]
    pub fn shared<T: Message + 'static>(value: Arc<T>) -> Self {
        Self(PayloadKind::Shared(Arc::new(Shared {
            value,
            bytes: OnceLock::new(),
        })))
    }

    /// Get wire bytes, encoding a shared value once on first access.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        match &self.0 {
            PayloadKind::Bytes(bytes) => bytes,
            PayloadKind::Shared(shared) => shared.bytes.get_or_init(|| shared.value.encode()),
        }
    }

    /// Take owned wire bytes. Shared payloads encode once and copy the cached bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        match self.0 {
            PayloadKind::Bytes(bytes) => bytes,
            PayloadKind::Shared(shared) => {
                shared.bytes.get_or_init(|| shared.value.encode()).clone()
            }
        }
    }

    /// Share the original allocation only for the exact Rust type.
    ///
    /// This does not validate a schema fingerprint or grant endpoint authority.
    /// Receivers must check both before using the value.
    #[must_use]
    pub fn downcast<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        match &self.0 {
            PayloadKind::Bytes(_) => None,
            PayloadKind::Shared(shared) => Arc::clone(&shared.value).into_any().downcast().ok(),
        }
    }
}

impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        Self(PayloadKind::Bytes(bytes))
    }
}

/// One ordered delivery, with optional native payload ownership.
///
/// Metadata and routing have exactly the same meaning as [`Sample`].
#[derive(Clone, Debug)]
pub struct Delivery {
    /// The publisher's or responder's concrete key.
    pub key: Key,
    /// The wire bytes or immutable shared protobuf value.
    pub payload: Payload,
    /// The schema fingerprint (8 bytes LE), or another attachment.
    pub attachment: Option<Vec<u8>>,
    /// The engine's send time, in unix nanoseconds.
    pub timestamp: Option<u64>,
}

impl From<Sample> for Delivery {
    fn from(sample: Sample) -> Self {
        Self {
            key: sample.key,
            payload: sample.payload.into(),
            attachment: sample.attachment,
            timestamp: sample.timestamp,
        }
    }
}

impl From<Delivery> for Sample {
    fn from(delivery: Delivery) -> Self {
        Self {
            key: delivery.key,
            payload: delivery.payload.into_bytes(),
            attachment: delivery.attachment,
            timestamp: delivery.timestamp,
        }
    }
}
