//! Engine-neutral named-port contracts handed to a managed module at startup.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

/// Runtime contract and report version. Old binaries must not silently accept v2.
pub const CONTRACT_VERSION: u32 = 2;

/// The operation implemented by an endpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortKind {
    /// Typed publication or subscription.
    #[default]
    Stream,
    /// Typed request/response, addressed by its request type.
    Rpc,
}

/// Publication transport policy owned by the app.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QosProfile {
    /// Wait under congestion.
    #[default]
    Reliable,
    /// Low latency with samples discarded under congestion.
    Sensor,
}

/// Publisher-side retained history.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    /// Live samples only.
    #[default]
    Volatile,
    /// Keep one sample for late subscribers.
    Last,
}

/// Subscriber-side startup replay.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Replay {
    /// Receive live samples only.
    #[default]
    Live,
    /// Query retained samples on publisher arrival.
    Last,
}

/// Receive queue policy. `latest` depth must be positive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "BufferRepr", into = "BufferRepr")]
pub enum ReceiveBuffer {
    /// The bounded, blocking default queue.
    #[default]
    Fifo,
    /// Discard the oldest sample on overflow.
    Latest(usize),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum BufferRepr {
    Fifo(FifoBuffer),
    Latest { latest: usize },
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FifoBuffer {
    Fifo,
}

impl From<BufferRepr> for ReceiveBuffer {
    fn from(value: BufferRepr) -> Self {
        match value {
            BufferRepr::Fifo(FifoBuffer::Fifo) => Self::Fifo,
            BufferRepr::Latest { latest } => Self::Latest(latest),
        }
    }
}

impl From<ReceiveBuffer> for BufferRepr {
    fn from(value: ReceiveBuffer) -> Self {
        match value {
            ReceiveBuffer::Fifo => Self::Fifo(FifoBuffer::Fifo),
            ReceiveBuffer::Latest(latest) => Self::Latest { latest },
        }
    }
}

/// The complete app-owned endpoint contract.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointContract {
    /// Expected protobuf message name, or RPC request name.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Stream or RPC operation.
    #[serde(default)]
    pub kind: PortKind,
    /// RPC response type; absent for streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
    /// Publication profile.
    #[serde(default)]
    pub qos: QosProfile,
    /// Publisher retention.
    #[serde(default)]
    pub retention: Retention,
    /// Subscriber replay.
    #[serde(default)]
    pub replay: Replay,
    /// Subscriber receive queue.
    #[serde(default)]
    pub buffer: ReceiveBuffer,
}

/// A declared owned child's exclusive subset of its host's public endpoints.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortDelegation {
    /// Host input names owned by this child.
    #[serde(default)]
    pub inputs: Vec<String>,
    /// Host output names owned by this child.
    #[serde(default)]
    pub outputs: Vec<String>,
}

/// The resolved contract of one executable module.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleBindings {
    /// Runtime contract format version.
    pub version: u32,
    /// Canonical module instance path, including the deployment name.
    pub namespace: String,
    /// Publication/server source identity. Owned children retain their host's address.
    pub endpoint_namespace: String,
    /// Named subscriptions and their exact publisher namespaces.
    pub inputs: BTreeMap<String, InputBinding>,
    /// Named publications implemented by this module.
    pub outputs: BTreeMap<String, OutputBinding>,
    /// Declared owned children; endpoints cannot be delegated twice.
    #[serde(default)]
    pub children: BTreeMap<String, PortDelegation>,
    /// Declared executable names in the prepared bundle, without platform suffixes.
    #[serde(default)]
    pub executables: Vec<String>,
}

/// One resolved input port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBinding {
    /// Expected protobuf message name (qualified when necessary).
    pub contract: EndpointContract,
    /// Exact namespaces of the declared publishers. RPC permits exactly one.
    pub sources: Vec<String>,
}

/// One declared output port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputBinding {
    /// Expected protobuf message name (qualified when necessary).
    pub contract: EndpointContract,
}

/// The actual typed ports registered by a module before it announces readiness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleReport {
    /// Runtime report format version.
    pub version: u32,
    /// Canonical executable module namespace.
    pub namespace: String,
    /// Actual input contracts, indexed by declared port name.
    pub inputs: BTreeMap<String, PortReport>,
    /// Actual output contracts, indexed by declared port name.
    pub outputs: BTreeMap<String, PortReport>,
}

/// A verified type implemented by a named port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortReport {
    /// Descriptor message name, or the transport type when no descriptor exists.
    #[serde(rename = "type")]
    pub type_name: String,
    /// The actual compiled schema fingerprint. Managed ports cannot omit it.
    pub schema: u64,
    /// Effective policy and operation implemented by the handle.
    pub contract: EndpointContract,
    /// Compiled RPC response name and schema; absent for streams.
    pub response: Option<ResponseReport>,
}

/// The compiled response half of a managed RPC endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseReport {
    /// Descriptor response name.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Compiled response fingerprint.
    pub schema: u64,
}
