//! Engine-neutral named-port contracts handed to a managed module at startup.

use alloc::collections::BTreeMap;
use alloc::string::String;

use serde::{Deserialize, Serialize};

/// The resolved contract of one executable module.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleBindings {
    /// Runtime contract format version.
    pub version: u32,
    /// Canonical module instance path, including the deployment name.
    pub namespace: String,
    /// Named subscriptions and their exact publisher namespaces.
    pub inputs: BTreeMap<String, InputBinding>,
    /// Named publications implemented by this module.
    pub outputs: BTreeMap<String, OutputBinding>,
}

/// One resolved input port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBinding {
    /// Expected protobuf message name (qualified when necessary).
    #[serde(rename = "type")]
    pub type_name: String,
    /// Exact namespace of the executable module publishing this input.
    pub source: String,
}

/// One declared output port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputBinding {
    /// Expected protobuf message name (qualified when necessary).
    #[serde(rename = "type")]
    pub type_name: String,
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
}
