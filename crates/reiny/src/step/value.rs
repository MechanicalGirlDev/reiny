use std::{collections::BTreeMap, fmt};

use crate::{
    MessageType, SCHEMA_FINGERPRINT_VERSION,
    encoded::{EncodedType, SchemaError},
};

/// Exact protobuf schema identity and application assertions, independent of routing authority.
///
/// Every identity uses the supported fingerprint version: encoded construction
/// validates it, and native construction reads the current compiled schema.
#[derive(Clone, Debug)]
pub struct StepType {
    data: TypeData,
}

// Keep compiled construction const and mismatch errors compact without changing
// their public fields. Imported identities and assertions own their allocation.
#[derive(Clone, Debug)]
enum TypeData {
    Compiled {
        type_name: &'static str,
        fingerprint: u64,
    },
    Owned(Box<OwnedType>),
}

#[derive(Clone, Debug)]
struct OwnedType {
    type_name: String,
    fingerprint: u64,
    semantics: BTreeMap<String, String>,
}

impl PartialEq for StepType {
    fn eq(&self, other: &Self) -> bool {
        self.type_name() == other.type_name()
            && self.fingerprint() == other.fingerprint()
            && self.semantics() == other.semantics()
    }
}

impl Eq for StepType {}

impl StepType {
    /// Read a message's compiled schema identity.
    #[must_use]
    pub const fn of<T: MessageType>() -> Self {
        Self {
            data: TypeData::Compiled {
                type_name: T::TYPE_NAME,
                fingerprint: T::SCHEMA_FINGERPRINT,
            },
        }
    }

    /// Import a validated owned schema identity without asserting payload validity.
    pub fn from_encoded_type(ty: EncodedType) -> Result<Self, SchemaError> {
        let fingerprint = ty.fingerprint();
        Ok(Self {
            data: TypeData::Owned(Box::new(OwnedType {
                type_name: ty.into_type_name(),
                fingerprint,
                semantics: BTreeMap::new(),
            })),
        })
    }

    /// Replace the application-owned assertions, compared exactly at admission.
    ///
    /// These opaque identifiers may describe units, identity, order, clocks or
    /// update rules. Reiny does not prove or implement them. Empty assertions
    /// match only empty assertions; this grants no routing authority.
    pub fn with_semantics(
        mut self,
        semantics: BTreeMap<String, String>,
    ) -> Result<Self, crate::bindings::BindingError> {
        crate::bindings::validate_semantics(&semantics)?;
        match &mut self.data {
            TypeData::Compiled {
                type_name,
                fingerprint,
            } => {
                self.data = TypeData::Owned(Box::new(OwnedType {
                    type_name: (*type_name).into(),
                    fingerprint: *fingerprint,
                    semantics,
                }));
            }
            TypeData::Owned(data) => data.semantics = semantics,
        }
        Ok(self)
    }

    /// Borrow the immutable application assertions.
    #[must_use]
    pub const fn semantics(&self) -> &BTreeMap<String, String> {
        static EMPTY: BTreeMap<String, String> = BTreeMap::new();
        match &self.data {
            TypeData::Compiled { .. } => &EMPTY,
            TypeData::Owned(data) => &data.semantics,
        }
    }

    /// Complete protobuf message name.
    #[must_use]
    pub const fn type_name(&self) -> &str {
        match &self.data {
            TypeData::Compiled { type_name, .. } => type_name,
            TypeData::Owned(data) => data.type_name.as_str(),
        }
    }

    /// Transitive schema fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> u64 {
        match &self.data {
            TypeData::Compiled { fingerprint, .. } => *fingerprint,
            TypeData::Owned(data) => data.fingerprint,
        }
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
    /// Explicitly assert application semantics for this owned snapshot.
    ///
    /// This does not convert or validate the payload.
    pub fn with_semantics(
        mut self,
        semantics: BTreeMap<String, String>,
    ) -> Result<Self, crate::bindings::BindingError> {
        self.step_type = self.step_type.with_semantics(semantics)?;
        Ok(self)
    }

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
    ///
    /// Application assertions are checked by graph and scheduler admission, not
    /// by protobuf decoding. Decoding neither proves nor changes those assertions.
    pub fn decode<T: prost::Message + Default + MessageType>(&self) -> Result<T, StepValueError> {
        let expected = StepType::of::<T>();
        if self.step_type.type_name() != expected.type_name()
            || self.step_type.fingerprint() != expected.fingerprint()
        {
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
