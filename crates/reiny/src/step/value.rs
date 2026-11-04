use std::{borrow::Cow, fmt};

use crate::{
    MessageType, SCHEMA_FINGERPRINT_VERSION,
    encoded::{EncodedType, SchemaError},
};

/// Exact protobuf schema identity, independent of routing authority.
///
/// Every identity uses the supported fingerprint version: encoded construction
/// validates it, and native construction reads the current compiled schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepType {
    type_name: Cow<'static, str>,
    fingerprint: u64,
}

impl StepType {
    /// Read a message's compiled schema identity.
    #[must_use]
    pub const fn of<T: MessageType>() -> Self {
        Self {
            type_name: Cow::Borrowed(T::TYPE_NAME),
            fingerprint: T::SCHEMA_FINGERPRINT,
        }
    }

    /// Import a validated owned schema identity without asserting payload validity.
    pub fn from_encoded_type(ty: EncodedType) -> Result<Self, SchemaError> {
        let fingerprint = ty.fingerprint();
        Ok(Self {
            type_name: Cow::Owned(ty.into_type_name()),
            fingerprint,
        })
    }

    /// Complete protobuf message name.
    #[must_use]
    pub const fn type_name(&self) -> &str {
        match &self.type_name {
            Cow::Borrowed(name) => name,
            Cow::Owned(name) => name.as_str(),
        }
    }

    /// Transitive schema fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Version of the fingerprint algorithm.
    #[must_use]
    pub const fn fingerprint_version(&self) -> u16 {
        SCHEMA_FINGERPRINT_VERSION
    }
}

/// An immutable, eagerly encoded protobuf snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepValue {
    step_type: StepType,
    bytes: Vec<u8>,
}

impl StepValue {
    /// Take ownership of an opaque, immutable encoded snapshot.
    ///
    /// The schema identity does not establish that the bytes decode or that
    /// domain values are valid. Consumers must validate at their decode boundary.
    #[must_use]
    pub fn from_encoded(step_type: StepType, bytes: Vec<u8>) -> Self {
        Self { step_type, bytes }
    }

    /// Borrow this snapshot's encoded bytes without allowing mutation.
    #[must_use]
    pub fn as_encoded(&self) -> &[u8] {
        &self.bytes
    }

    /// Snapshot the value now, without retaining any reference to it.
    #[must_use]
    pub fn encode<T: prost::Message + MessageType>(value: &T) -> Self {
        Self {
            step_type: StepType::of::<T>(),
            bytes: value.encode_to_vec(),
        }
    }

    /// Schema carried by this snapshot.
    #[must_use]
    pub const fn step_type(&self) -> &StepType {
        &self.step_type
    }

    /// Check all schema identity fields before decoding the protobuf snapshot.
    pub fn decode<T: prost::Message + Default + MessageType>(&self) -> Result<T, StepValueError> {
        let expected = StepType::of::<T>();
        if self.step_type != expected {
            return Err(StepValueError::TypeMismatch {
                expected,
                actual: self.step_type.clone(),
            });
        }
        T::decode(self.bytes.as_slice()).map_err(|error| StepValueError::Decode {
            type_name: self.step_type.type_name().to_owned(),
            message: error.to_string(),
        })
    }
}

/// A schema mismatch or invalid protobuf payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepValueError {
    /// The complete schema identities differ.
    TypeMismatch {
        /// Requested schema.
        expected: StepType,
        /// Stored schema.
        actual: StepType,
    },
    /// The schema matched, but decoding failed.
    Decode {
        /// Protobuf message name.
        type_name: String,
        /// Decoder diagnostic.
        message: String,
    },
}

impl fmt::Display for StepValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch { expected, actual } => {
                write!(
                    f,
                    "step value schema mismatch: expected {expected:?}, got {actual:?}"
                )
            }
            Self::Decode { type_name, message } => write!(f, "decode {type_name}: {message}"),
        }
    }
}

impl std::error::Error for StepValueError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::step::tests::Number;

    const NUMBER_TYPE: StepType = StepType::of::<Number>();
    static STATIC_NUMBER_TYPE: StepType = StepType::of::<Number>();
    const NUMBER_NAME: &str = STATIC_NUMBER_TYPE.type_name();

    #[test]
    fn encoded_roundtrip_matches_typed_identity() -> Result<(), Box<dyn std::error::Error>> {
        let original = Number { value: -123 };
        let snapshot = StepValue::encode(&original);
        let schema = EncodedType::new(
            String::from(NUMBER_NAME),
            SCHEMA_FINGERPRINT_VERSION,
            Number::SCHEMA_FINGERPRINT,
        )?;
        let imported = StepValue::from_encoded(
            StepType::from_encoded_type(schema)?,
            snapshot.as_encoded().into(),
        );

        assert_eq!(imported.step_type(), &NUMBER_TYPE);
        assert_eq!(imported, snapshot);
        assert_eq!(imported.decode::<Number>()?, original);
        Ok(())
    }

    #[test]
    fn imported_snapshot_owns_name_and_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let (snapshot, expected_bytes) = {
            let mut name = String::from(Number::TYPE_NAME);
            let mut bytes = prost::Message::encode_to_vec(&Number { value: 42 });
            let expected_bytes = bytes.clone();
            let schema = EncodedType::new(
                name.clone(),
                SCHEMA_FINGERPRINT_VERSION,
                Number::SCHEMA_FINGERPRINT,
            )?;
            let snapshot =
                StepValue::from_encoded(StepType::from_encoded_type(schema)?, bytes.clone());
            name.clear();
            bytes.fill(0x80);
            (snapshot, expected_bytes)
        };

        let cloned = snapshot.clone();
        drop(snapshot);
        assert_eq!(cloned.step_type().type_name(), Number::TYPE_NAME);
        assert_eq!(cloned.as_encoded(), expected_bytes);
        assert_eq!(cloned.decode::<Number>()?.value, 42);
        Ok(())
    }

    #[test]
    fn encoded_identity_rejects_invalid_names() {
        for name in [
            "",
            ".step.Number",
            "step.",
            "step..Number",
            "step.1Number",
            "step.N/umber",
        ] {
            assert!(EncodedType::new(name, SCHEMA_FINGERPRINT_VERSION, 1).is_err());
        }
    }

    #[test]
    fn imported_identity_mismatch_precedes_payload_decode() -> Result<(), Box<dyn std::error::Error>>
    {
        for (name, fingerprint) in [
            ("step.OtherNumber", Number::SCHEMA_FINGERPRINT),
            (Number::TYPE_NAME, Number::SCHEMA_FINGERPRINT + 1),
        ] {
            let schema = EncodedType::new(name, SCHEMA_FINGERPRINT_VERSION, fingerprint)?;
            let value = StepValue::from_encoded(StepType::from_encoded_type(schema)?, vec![0x80]);
            assert!(matches!(
                value.decode::<Number>(),
                Err(StepValueError::TypeMismatch { expected, actual })
                    if expected == NUMBER_TYPE && actual == *value.step_type()
            ));
        }
        Ok(())
    }

    #[test]
    fn zero_is_a_valid_fingerprint() -> Result<(), Box<dyn std::error::Error>> {
        let schema = EncodedType::new("Unpackaged", SCHEMA_FINGERPRINT_VERSION, 0)?;
        let ty = StepType::from_encoded_type(schema)?;
        assert_eq!(ty.type_name(), "Unpackaged");
        assert_eq!(ty.fingerprint(), 0);
        Ok(())
    }

    #[test]
    fn identity_checks_include_fingerprint_version() {
        let received = SCHEMA_FINGERPRINT_VERSION + 1;
        assert!(matches!(
            EncodedType::new(Number::TYPE_NAME, received, Number::SCHEMA_FINGERPRINT)
                .and_then(StepType::from_encoded_type),
            Err(SchemaError::UnsupportedFingerprintVersion { expected, received: actual })
                if expected == SCHEMA_FINGERPRINT_VERSION && actual == received
        ));
    }

    #[test]
    fn malformed_payload_returns_decode_error() {
        let value = StepValue::from_encoded(StepType::of::<Number>(), vec![0x80]);
        assert_eq!(value.as_encoded(), &[0x80]);
        assert!(matches!(
            value.decode::<Number>(),
            Err(StepValueError::Decode { .. })
        ));
    }
}
