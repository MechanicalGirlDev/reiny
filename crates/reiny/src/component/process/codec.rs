use std::collections::BTreeSet;
use std::fmt;
use std::io::Write;

use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::{ProcessError, protocol};

// First walk preserves every object key, including nested binding maps. Only
// after this succeeds may derived serde DTOs collect maps or buffered enums.
struct Unique;

impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        decoder.deserialize_any(Self)
    }
}

impl<'de> Visitor<'de> for Unique {
    type Value = Self;

    fn expecting(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("JSON with unique object keys")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self, M::Error> {
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(serde::de::Error::custom("duplicate object key"));
            }
            map.next_value::<Self>()?;
        }
        Ok(Self)
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self, S::Error> {
        while seq.next_element::<Self>()?.is_some() {}
        Ok(Self)
    }

    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self, E> {
        Ok(Self)
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self, E> {
        Ok(Self)
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self, E> {
        Ok(Self)
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self, E> {
        Ok(Self)
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self, E> {
        Ok(Self)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self, E> {
        Ok(Self)
    }
}

/// Decode a bounded strict JSON payload, rejecting duplicate keys at every depth.
///
/// Successful decoding is not authentication, identity checking or admission.
pub fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, ProcessError> {
    protocol::frame_header(payload).map_err(ProcessError::frame)?;
    serde_json::from_slice::<Unique>(payload).map_err(ProcessError::json)?;
    serde_json::from_slice(payload).map_err(ProcessError::json)
}

struct Bounded(Vec<u8>);

impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let limit = usize::try_from(protocol::MAX_FRAME_BYTES).map_err(std::io::Error::other)?;
        let needed = self.0.len().saturating_add(bytes.len());
        if needed > limit {
            return Err(std::io::Error::other("host frame exceeds limit"));
        }
        if needed > self.0.capacity() {
            let capacity = self.0.capacity().saturating_mul(2).max(needed).min(limit);
            self.0
                .try_reserve_exact(capacity - self.0.len())
                .map_err(std::io::Error::other)?;
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serialize one JSON payload without allocating beyond the protocol frame bound.
///
/// The returned bytes do not include the four-byte framing header.
pub fn encode(value: &impl Serialize) -> Result<Vec<u8>, ProcessError> {
    let mut output = Bounded(Vec::new());
    serde_json::to_writer(&mut output, value).map_err(ProcessError::json)?;
    Ok(output.0)
}
