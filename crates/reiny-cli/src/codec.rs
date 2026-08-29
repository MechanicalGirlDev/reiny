//! Building payload ↔ JSON from the descriptors a running launch announces at `@schema`.
//!
//! The innards of `reiny topic echo` / `reiny service call`, and the only place `prost-reflect` is let
//! into the CLI — bag keeps it out on the grounds that `mcap cat --json` does the dynamic decoding of
//! a file, but **`mcap cat` cannot be pointed at a live bus**. reiny proper still never interprets a descriptor.

use anyhow::{Context, Result};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

/// The encoder / decoder for one message type.
pub(crate) struct Codec {
    message: MessageDescriptor,
    /// The descriptor-derived fingerprint (the same computation reiny-build uses). Matched against the attachment to warn.
    pub(crate) fingerprint: Option<u64>,
}

impl Codec {
    /// Build it from an `@schema` response (a `FileDescriptorSet` pruned by
    /// `reiny_build::descriptor_subset`) and a fully qualified message name.
    pub(crate) fn from_file_set(file_set: &[u8], fqn: &str) -> Result<Self> {
        let pool = DescriptorPool::decode(file_set)
            .with_context(|| format!("decoding descriptor set for {fqn}"))?;
        let message = pool
            .get_message_by_name(fqn)
            .with_context(|| format!("descriptor set lacks message {fqn}"))?;
        let fingerprint = reiny_build::message_fingerprint(file_set, fqn).unwrap_or(None);
        Ok(Self {
            message,
            fingerprint,
        })
    }

    /// The fully qualified message name (e.g. `hs.RobotState`).
    pub(crate) fn full_name(&self) -> &str {
        self.message.full_name()
    }

    /// proto bytes → JSON (the proto3 JSON mapping, where an int64 becomes a string).
    pub(crate) fn decode_json(&self, payload: &[u8]) -> Result<String> {
        let msg = DynamicMessage::decode(self.message.clone(), payload)
            .with_context(|| format!("decoding {} payload", self.full_name()))?;
        serde_json::to_string(&msg).context("serializing to JSON")
    }

    /// JSON → proto bytes.
    pub(crate) fn encode_json(&self, json: &str) -> Result<Vec<u8>> {
        let mut de = serde_json::Deserializer::from_str(json);
        let msg = DynamicMessage::deserialize(self.message.clone(), &mut de)
            .with_context(|| format!("parsing JSON as {}", self.full_name()))?;
        de.end().context("trailing characters after JSON")?;
        Ok(prost::Message::encode_to_vec(&msg))
    }
}

/// How the payload of a type with no descriptor is shown (hex).
pub(crate) fn hex(payload: &[u8]) -> String {
    use std::fmt::Write as _;
    payload.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use prost::Message;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto,
    };

    /// The descriptor set of `message Probe { uint32 seq = 1; string name = 2; }`, built by hand
    /// (so the test needs no protoc).
    fn probe_set() -> Vec<u8> {
        let field =
            |name: &str, number: i32, ty: field_descriptor_proto::Type| FieldDescriptorProto {
                name: Some(name.into()),
                number: Some(number),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(ty as i32),
                json_name: Some(name.into()),
                ..Default::default()
            };
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("probe.proto".into()),
                package: Some("e2e".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Probe".into()),
                    field: vec![
                        field("seq", 1, field_descriptor_proto::Type::Uint32),
                        field("name", 2, field_descriptor_proto::Type::String),
                    ],
                    ..Default::default()
                }],
                syntax: Some("proto3".into()),
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    #[test]
    fn json_round_trip() {
        let codec = Codec::from_file_set(&probe_set(), "e2e.Probe").unwrap();
        assert_eq!(codec.full_name(), "e2e.Probe");
        assert!(codec.fingerprint.is_some());
        let bytes = codec.encode_json(r#"{"seq": 7, "name": "x"}"#).unwrap();
        // field 1 varint 7, field 2 len-delimited "x"
        assert_eq!(bytes, [0x08, 0x07, 0x12, 0x01, b'x']);
        assert_eq!(
            codec.decode_json(&bytes).unwrap(),
            r#"{"seq":7,"name":"x"}"#
        );
        assert!(codec.encode_json(r#"{"nope": 1}"#).is_err());
        assert!(
            Codec::from_file_set(&probe_set(), "e2e.Missing").is_err(),
            "unknown message name must fail"
        );
    }

    #[test]
    fn hex_dump() {
        assert_eq!(hex(&[0x08, 0xff]), "08ff");
    }
}
