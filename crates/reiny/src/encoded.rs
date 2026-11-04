//! Owned schema identity for encoded protobuf values, independent of port authority.

use std::fmt;

use crate::MessageType;

/// A validated protobuf full name and exact compiled schema fingerprint.
///
/// Identity does not validate payload bytes or grant any communication permission.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodedType {
    type_name: String,
    fingerprint_version: u16,
    fingerprint: u64,
}

impl EncodedType {
    /// Validate an owned protobuf name and the supported fingerprint algorithm.
    /// Zero is a valid fingerprint.
    pub fn new(
        type_name: impl Into<String>,
        fingerprint_version: u16,
        fingerprint: u64,
    ) -> Result<Self, SchemaError> {
        let type_name = type_name.into();
        if !type_name.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        }) {
            return Err(SchemaError::InvalidTypeName);
        }
        if fingerprint_version != reiny_core::SCHEMA_FINGERPRINT_VERSION {
            return Err(SchemaError::UnsupportedFingerprintVersion {
                expected: reiny_core::SCHEMA_FINGERPRINT_VERSION,
                received: fingerprint_version,
            });
        }
        Ok(Self {
            type_name,
            fingerprint_version,
            fingerprint,
        })
    }

    /// Import a native type's compiled identity through the same validation.
    pub fn of<T: MessageType>() -> Result<Self, SchemaError> {
        Self::new(
            T::TYPE_NAME,
            reiny_core::SCHEMA_FINGERPRINT_VERSION,
            T::SCHEMA_FINGERPRINT,
        )
    }

    /// The full protobuf name, without a leading dot.
    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Transfer the owned protobuf name without allocating another copy.
    #[must_use]
    pub fn into_type_name(self) -> String {
        self.type_name
    }

    /// The fingerprint algorithm version.
    #[must_use]
    pub const fn fingerprint_version(&self) -> u16 {
        self.fingerprint_version
    }

    /// The exact compiled fingerprint; zero is not a sentinel.
    #[must_use]
    pub const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

/// Rejected encoded schema identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaError {
    /// A name is empty or contains something other than dot-separated protobuf identifiers.
    InvalidTypeName,
    /// The fingerprint algorithm is not supported by this runtime.
    UnsupportedFingerprintVersion {
        /// The runtime's supported algorithm.
        expected: u16,
        /// The supplied algorithm.
        received: u16,
    },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTypeName => formatter.write_str("invalid protobuf full name"),
            Self::UnsupportedFingerprintVersion { expected, received } => write!(
                formatter,
                "unsupported fingerprint version {received}; expected {expected}"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_identity_validates_complete_names_and_algorithm() -> Result<(), SchemaError> {
        let version = reiny_core::SCHEMA_FINGERPRINT_VERSION;
        for name in ["Value", "_Value", "package_1.Nested.Value2"] {
            let identity = EncodedType::new(name, version, 0)?;
            assert_eq!(identity.type_name(), name);
            assert_eq!(identity.fingerprint(), 0);
            assert_eq!(identity.fingerprint_version(), version);
            assert_eq!(identity.into_type_name(), name);
        }
        for name in [
            "",
            ".Value",
            "Value.",
            "a..Value",
            "1Value",
            "a/b",
            "a.*",
            "\u{e9}.Value",
            "a\0b",
        ] {
            assert_eq!(
                EncodedType::new(name, version, 0),
                Err(SchemaError::InvalidTypeName)
            );
        }
        assert_eq!(
            EncodedType::new("a.Value", version + 1, 0),
            Err(SchemaError::UnsupportedFingerprintVersion {
                expected: version,
                received: version + 1
            }),
        );
        Ok(())
    }
}
