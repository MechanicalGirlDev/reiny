use alloc::string::String;
use core::fmt;
use serde::{Deserialize, Serialize};

/// Host-control wire version. Deserialization rejects every value except one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub struct ProtocolVersion;

impl TryFrom<u16> for ProtocolVersion {
    type Error = EnvelopeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if value == 1 {
            Ok(Self)
        } else {
            Err(EnvelopeError::Version(value))
        }
    }
}

impl From<ProtocolVersion> for u16 {
    fn from(_: ProtocolVersion) -> Self {
        1
    }
}

/// One deployment owner's generation, assigned by the owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OwnerGeneration(pub u64);

/// One immutable graph admission attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GraphGeneration(pub u64);

/// One spawned host incarnation; a replacement gets a new generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostGeneration(pub u64);

/// Per-direction message ordering; never reused within a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sequence(pub u64);

/// Named host selected by the deployment owner, not a PID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostId(pub String);

/// Opaque digest of an owner's verified prepared artifact, not a signature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PreparedDigest(pub String);

/// Expected identities echoed in every message, including the handshake.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionIdentity {
    /// The deployment owner's current incarnation.
    pub owner_generation: OwnerGeneration,
    /// The complete prepared graph's admission attempt.
    pub graph_generation: GraphGeneration,
    /// The exact assigned host.
    pub host_id: HostId,
    /// The exact spawned incarnation.
    pub host_generation: HostGeneration,
    /// Digest of the owner's prepared graph.
    pub graph_digest: PreparedDigest,
    /// Digest of this host's verified executable bundle.
    pub bundle_digest: PreparedDigest,
}

/// A typed message with independently checked identity and ordering.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope<T> {
    /// Required wire version; unsupported values cannot deserialize.
    pub version: ProtocolVersion,
    /// Identity expected by the receiving process.
    pub session: SessionIdentity,
    /// Strictly increasing in this sender's direction.
    pub sequence: Sequence,
    /// Direction-specific command or event.
    pub body: T,
}

impl<T> Envelope<T> {
    /// Check identity and ordering, without granting authority or changing state.
    ///
    /// Pass `None` for the first message, which must have sequence zero.
    /// Retain the accepted sequence only after this check succeeds. Lifecycle,
    /// authentication, compatibility and graph admission belong to callers.
    ///
    /// # Errors
    /// Returns an identity or sequence mismatch. No peer-supplied values are
    /// included in the error, avoiding accidental authentication-data logging.
    pub fn check(
        &self,
        expected: &SessionIdentity,
        previous: Option<Sequence>,
    ) -> Result<(), EnvelopeError> {
        if self.session != *expected {
            return Err(EnvelopeError::Identity);
        }
        let ordered = match previous {
            None => self.sequence == Sequence(0),
            Some(previous) => self.sequence > previous,
        };
        if !ordered {
            return Err(EnvelopeError::Sequence);
        }
        Ok(())
    }
}

/// Fixed-size one-use secret supplied over the inherited bootstrap pipe.
///
/// The process owner supplies cryptographically random bytes and consumes them
/// once. This type neither generates entropy nor provides constant-time
/// comparison or memory zeroization. Serialization exposes the secret by design.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Nonce([u8; 32]);

impl Nonce {
    /// Wrap externally generated random bytes, without claiming entropy.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the bytes for the caller's authentication implementation.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Nonce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Nonce([REDACTED])")
    }
}

/// Envelope rejection independent of graph or lifecycle admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    /// Unsupported host-control protocol version.
    Version(u16),
    /// Host, generation, or digest differs from the prepared expectation.
    Identity,
    /// First sequence is nonzero, or a sequence was replayed or decreased.
    Sequence,
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Version(version) => write!(f, "unsupported host protocol version {version}"),
            Self::Identity => f.write_str("host protocol identity mismatch"),
            Self::Sequence => f.write_str("host protocol sequence mismatch"),
        }
    }
}

impl core::error::Error for EnvelopeError {}
