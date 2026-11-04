//! Neutral DTOs for a future owner/component-host control channel.
//!
//! This module does not launch processes, authenticate peers, validate graphs,
//! commit admission tickets, or implement a transport. A decoded message is not
//! approval. The owner must compare the handshake with its prepared deployment,
//! validate every compiled report against its own exact bindings, and validate
//! the complete graph before committing. All hosts must acknowledge commit
//! before the owner sends any run command.
//!
//! Wire framing is a four-byte big-endian payload length followed by one strict
//! JSON envelope, at most [`MAX_FRAME_BYTES`] payload bytes. This no-std crate
//! has serde but no JSON codec: [`frame_payload`] and [`frame_header`] enforce
//! framing, not JSON syntax. Callers must use a JSON deserializer that rejects
//! trailing data and duplicate object keys, including keys in binding/report
//! maps, and a bounded serializer. Do not deserialize through a generic JSON
//! value that discards duplicate keys. Check the header before allocating or
//! reading the body. Derived DTO deserializers reject unknown struct fields.
//!
//! Each direction has its own sequence, beginning at zero and strictly
//! increasing. A persistent connection is one session; reconnect/replay is not
//! supported. Call [`Envelope::check`] before handling each message, then enforce
//! the lifecycle transition in the caller. Repeated current-session stop
//! requests with fresh sequence numbers are harmless. Never persist or log
//! handshake bytes; nonce debug output alone is redacted, not arbitrary strings
//! or serialized messages. No signatures, cryptographic authentication, or
//! hostile same-user isolation are provided here.

mod assignment;
mod framing;
mod identity;
mod message;
mod status;

pub use assignment::{
    Compatibility, ComponentAssignment, ConfigOrigin, FactoryRegistration, Milliseconds, Prepare,
    TransportSettings,
};
pub use framing::{FrameError, MAX_FRAME_BYTES, frame_header, frame_payload, payload_length};
pub use identity::{
    Envelope, EnvelopeError, GraphGeneration, HostGeneration, HostId, Nonce, OwnerGeneration,
    PreparedDigest, ProtocolVersion, Sequence, SessionIdentity,
};
pub use message::{Authenticate, HostMessage, OwnerMessage, StopReason};
pub use status::{FailureCode, HostFailure, HostOutcome, HostPhase, HostStatus, InstanceStatus};

#[cfg(test)]
mod tests;
