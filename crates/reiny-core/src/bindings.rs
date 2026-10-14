//! Engine-neutral named-port contracts handed to a managed module at startup.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

/// Version of reusable module definitions and invocation manifests.
pub const MODULE_MANIFEST_VERSION: u32 = 3;
/// Version of resolved runtime module bindings.
pub const MODULE_BINDINGS_VERSION: u32 = 3;
/// Version of compiled runtime module reports.
pub const MODULE_REPORT_VERSION: u32 = 3;

/// The operation implemented by an endpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortKind {
    /// Typed publication or subscription.
    #[default]
    Stream,
    /// Typed request/response, addressed by its declared server endpoint.
    Rpc,
}

/// Publication transport policy owned by the app.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QosProfile {
    /// Use the backend's default congestion policy.
    #[default]
    Default,
    /// Wait for congestion to clear, without promising delivery.
    Command,
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
    /// Named outputs implemented by this module.
    pub outputs: BTreeMap<String, OutputBinding>,
    /// Declared owned children; endpoints cannot be delegated twice.
    #[serde(default)]
    pub children: BTreeMap<String, PortDelegation>,
    /// Declared executable names in the prepared bundle, without platform suffixes.
    #[serde(default)]
    pub executables: Vec<String>,
}

/// The exact identity of a declared output endpoint.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointAddress {
    /// Canonical publication/server namespace.
    pub namespace: String,
    /// Declared output port name, independent of its message type.
    pub port: String,
}

impl EndpointAddress {
    /// Validate an exact namespace and ordinary port name.
    pub fn validate(&self) -> Result<(), BindingError> {
        validate_namespace(&self.namespace)?;
        validate_port_name(&self.port)
    }
}

/// One resolved input port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBinding {
    /// Expected protobuf message name (qualified when necessary).
    pub contract: EndpointContract,
    /// Exact declared output endpoints. RPC permits exactly one.
    pub sources: Vec<EndpointAddress>,
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
    /// Version of the compiled schema fingerprint algorithm.
    pub fingerprint_version: u16,
    /// Canonical executable module namespace.
    pub namespace: String,
    /// Host identity retained by delegated owned children.
    pub endpoint_namespace: String,
    /// Actual input contracts, indexed by declared port name.
    pub inputs: BTreeMap<String, PortReport>,
    /// Actual output contracts, indexed by declared port name.
    pub outputs: BTreeMap<String, PortReport>,
}

/// The exact endpoints implemented by a compiled port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "direction", rename_all = "snake_case", deny_unknown_fields)]
pub enum PortEndpoint {
    /// A receiver or RPC caller's declared output sources.
    Input {
        /// Exact source output endpoints.
        sources: Vec<EndpointAddress>,
    },
    /// A publisher or RPC server's concrete output identity.
    Output {
        /// Declared output endpoint.
        address: EndpointAddress,
    },
}

/// A verified type implemented by a named port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortReport {
    /// Exact address or sources, independent of the message type.
    pub endpoint: PortEndpoint,
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

/// A malformed resolved runtime contract, rejected before opening transport ports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingError(&'static str);

impl core::fmt::Display for BindingError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.0)
    }
}

impl core::error::Error for BindingError {}

fn require(condition: bool, message: &'static str) -> Result<(), BindingError> {
    if condition {
        Ok(())
    } else {
        Err(BindingError(message))
    }
}

fn segment(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && !value.contains(['/', '*', '?', '#', '$', '@'])
        && !value.contains(char::is_whitespace)
}

fn type_name(value: &str) -> bool {
    value.split('.').all(|part| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    })
}

/// Validate an exact hierarchical module identity or source.
pub fn validate_namespace(value: &str) -> Result<(), BindingError> {
    require(
        value
            .split('/')
            .all(|part| segment(part) && !matches!(part, "." | "..")),
        "invalid module namespace",
    )
}

/// Validate one exact declared endpoint name.
pub fn validate_port_name(value: &str) -> Result<(), BindingError> {
    require(segment(value), "invalid endpoint name")
}

impl EndpointContract {
    /// Validate a concrete endpoint's type, operation and direction-owned policies.
    ///
    /// Forwarded aliases and optional caller type assertions must be resolved before
    /// using this validator; an empty type is not a concrete endpoint.
    pub fn validate(&self, input: bool) -> Result<(), BindingError> {
        require(type_name(&self.type_name), "invalid endpoint type")?;
        require(
            self.buffer != ReceiveBuffer::Latest(0),
            "latest buffer depth must be positive",
        )?;
        require(
            !input || (self.qos == QosProfile::Default && self.retention == Retention::Volatile),
            "qos and retention belong to outputs",
        )?;
        require(
            input || (self.replay == Replay::Live && self.buffer == ReceiveBuffer::Fifo),
            "replay and buffer belong to inputs",
        )?;
        match self.kind {
            PortKind::Stream => {
                require(self.response.is_none(), "stream cannot declare a response")
            }
            PortKind::Rpc => {
                require(
                    self.response.as_deref().is_some_and(type_name),
                    "RPC requires a valid response type",
                )?;
                require(
                    self.qos == QosProfile::Default
                        && self.retention == Retention::Volatile
                        && self.replay == Replay::Live
                        && self.buffer == ReceiveBuffer::Fifo,
                    "stream policies do not apply to RPC",
                )
            }
        }
    }
}

impl ModuleBindings {
    /// Validate identity, endpoint policies, exact sources and exclusive delegation.
    ///
    /// Transport-specific domain and executable identity checks belong to callers.
    pub fn validate(&self) -> Result<(), BindingError> {
        require(
            self.version == MODULE_BINDINGS_VERSION,
            "unsupported module bindings version",
        )?;
        validate_namespace(&self.namespace)?;
        validate_namespace(&self.endpoint_namespace)?;
        require(
            self.namespace == self.endpoint_namespace
                || self
                    .namespace
                    .strip_prefix(&format!("{}/owned/", self.endpoint_namespace))
                    .is_some_and(|child| !child.is_empty() && !child.contains('/')),
            "endpoint namespace must be the module or its owning host",
        )?;
        require(
            self.namespace == self.endpoint_namespace || self.children.is_empty(),
            "owned children cannot delegate endpoints",
        )?;
        let mut executables = BTreeSet::new();
        for executable in &self.executables {
            require(
                segment(executable)
                    && !matches!(executable.as_str(), "." | "..")
                    && executables.insert(executable),
                "invalid or duplicate executable",
            )?;
        }
        let mut addresses = BTreeMap::new();
        for (name, port) in &self.inputs {
            validate_port_name(name)?;
            port.contract.validate(true)?;
            require(!port.sources.is_empty(), "input requires exact sources")?;
            require(
                port.contract.kind != PortKind::Rpc || port.sources.len() == 1,
                "RPC input requires exactly one source",
            )?;
            let mut unique = BTreeSet::new();
            for source in &port.sources {
                source.validate()?;
                require(unique.insert(source), "duplicate input source")?;
                let contract = (
                    &port.contract.type_name,
                    &port.contract.response,
                    port.contract.kind == PortKind::Rpc,
                );
                if let Some(previous) = addresses.insert(source, contract) {
                    require(
                        previous == contract,
                        "incompatible contracts for one source endpoint",
                    )?;
                }
            }
        }
        for (name, port) in &self.outputs {
            validate_port_name(name)?;
            port.contract.validate(false)?;
        }
        self.validate_delegations()
    }

    fn validate_delegations(&self) -> Result<(), BindingError> {
        let mut delegated = BTreeSet::new();
        for (name, child) in &self.children {
            require(
                segment(name) && !matches!(name.as_str(), "." | ".."),
                "invalid owned child name",
            )?;
            for (port, input) in child
                .inputs
                .iter()
                .map(|port| (port, true))
                .chain(child.outputs.iter().map(|port| (port, false)))
            {
                require(
                    if input {
                        self.inputs.contains_key(port)
                    } else {
                        self.outputs.contains_key(port)
                    },
                    "child delegates undefined endpoint",
                )?;
                require(
                    delegated.insert((port, input)),
                    "endpoint delegated more than once",
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;

    fn bindings() -> ModuleBindings {
        ModuleBindings {
            version: MODULE_BINDINGS_VERSION,
            namespace: "robot/control".into(),
            endpoint_namespace: "robot/control".into(),
            inputs: BTreeMap::from([(
                "state".into(),
                InputBinding {
                    contract: EndpointContract {
                        type_name: "robot.State".into(),
                        ..EndpointContract::default()
                    },
                    sources: alloc::vec![EndpointAddress {
                        namespace: "robot/sensor".into(),
                        port: "state".into(),
                    }],
                },
            )]),
            outputs: BTreeMap::from([(
                "command".into(),
                OutputBinding {
                    contract: EndpointContract {
                        type_name: "robot.Command".into(),
                        ..EndpointContract::default()
                    },
                },
            )]),
            children: BTreeMap::new(),
            executables: alloc::vec!["worker".into()],
        }
    }

    #[test]
    fn stream_policies_and_owned_delegations_are_validated() {
        let mut bindings = bindings();
        let input = &mut bindings.inputs.get_mut("state").unwrap().contract;
        input.replay = Replay::Last;
        input.buffer = ReceiveBuffer::Latest(2);
        let output = &mut bindings.outputs.get_mut("command").unwrap().contract;
        output.qos = QosProfile::Sensor;
        output.retention = Retention::Last;
        bindings.children.insert(
            "worker".into(),
            PortDelegation {
                inputs: alloc::vec!["state".into()],
                outputs: alloc::vec!["command".into()],
            },
        );
        bindings.validate().unwrap();
        bindings.children.clear();
        bindings.namespace = "robot/control/owned/worker".into();
        bindings.validate().unwrap();
    }

    #[test]
    fn malformed_bindings_are_rejected() {
        for mutation in 0..18 {
            let mut bindings = bindings();
            match mutation {
                0 => bindings.version = 1,
                1 => bindings.namespace = "robot/../control".into(),
                2 => bindings.endpoint_namespace = "other".into(),
                3 => bindings.inputs.get_mut("state").unwrap().sources.clear(),
                4 => bindings
                    .inputs
                    .get_mut("state")
                    .unwrap()
                    .sources
                    .push(EndpointAddress {
                        namespace: "robot/sensor".into(),
                        port: "state".into(),
                    }),
                5 => bindings.inputs.get_mut("state").unwrap().sources[0].namespace = "**".into(),
                6 => {
                    bindings.inputs.get_mut("state").unwrap().contract.buffer =
                        ReceiveBuffer::Latest(0);
                }
                7 => bindings.inputs.get_mut("state").unwrap().contract.retention = Retention::Last,
                8 => bindings.outputs.get_mut("command").unwrap().contract.replay = Replay::Last,
                9 => {
                    bindings.inputs.get_mut("state").unwrap().contract.response =
                        Some("robot.Result".into());
                }
                10 => bindings.inputs.get_mut("state").unwrap().contract.kind = PortKind::Rpc,
                11 => {
                    let duplicate = bindings.outputs["command"].clone();
                    bindings.outputs.insert("@duplicate".into(), duplicate);
                }
                12 => bindings.executables.push("worker".into()),
                13 => bindings.executables[0] = "..".into(),
                14 => {
                    bindings.children.insert(
                        "worker".into(),
                        PortDelegation {
                            inputs: alloc::vec!["missing".into()],
                            outputs: Vec::new(),
                        },
                    );
                }
                15 => {
                    let child = PortDelegation {
                        inputs: alloc::vec!["state".into()],
                        outputs: Vec::new(),
                    };
                    bindings.children.insert("first".into(), child.clone());
                    bindings.children.insert("second".into(), child);
                }
                16 => {
                    bindings.inputs.get_mut("state").unwrap().contract.type_name =
                        "robot..State".into();
                }
                _ => {
                    let input = &mut bindings.inputs.get_mut("state").unwrap().contract;
                    input.kind = PortKind::Rpc;
                    input.response = Some("robot..Result".into());
                }
            }
            assert!(bindings.validate().is_err(), "mutation {mutation}");
        }
    }

    #[test]
    fn distinct_protobuf_packages_do_not_collide_on_the_same_source() {
        // Given a source carrying same-named messages from different packages.
        let mut bindings = bindings();
        let mut other = bindings.inputs["state"].clone();
        other.contract.type_name = "other.State".into();
        other.sources[0].port = "other".into();
        bindings.inputs.insert("other".into(), other);
        // When the same namespace exposes distinct named output addresses.
        let result = bindings.validate();
        // Then both contracts are independently valid.
        result.unwrap();
    }

    #[test]
    fn legacy_binding_versions_are_rejected() {
        for version in [1, 2] {
            let mut bindings = bindings();
            bindings.version = version;
            assert!(bindings.validate().is_err());
        }
    }

    #[test]
    fn distinct_same_type_output_ports_and_fan_in_are_valid() {
        let mut bindings = bindings();
        bindings
            .outputs
            .insert("other".into(), bindings.outputs["command"].clone());
        let input = bindings.inputs.get_mut("state").unwrap();
        input.sources.push(EndpointAddress {
            namespace: "robot/sensor".into(),
            port: "other".into(),
        });
        bindings.validate().unwrap();
    }

    #[test]
    fn source_port_and_source_contract_conflicts_are_rejected() {
        let mut bindings = bindings();
        let mut other = bindings.inputs["state"].clone();
        other.contract.type_name = "other.State".into();
        bindings.inputs.insert("other".into(), other);
        assert!(bindings.validate().is_err());
        bindings.inputs.remove("other");
        bindings.inputs.get_mut("state").unwrap().sources[0].port = "../state".into();
        assert!(bindings.validate().is_err());
    }

    #[test]
    fn rpc_requires_one_source_and_default_stream_policies() {
        let mut bindings = bindings();
        let input = bindings.inputs.get_mut("state").unwrap();
        input.contract.kind = PortKind::Rpc;
        input.contract.response = Some("robot.Result".into());
        bindings.validate().unwrap();
        bindings
            .inputs
            .get_mut("state")
            .unwrap()
            .sources
            .push(EndpointAddress {
                namespace: "robot/other".into(),
                port: "state".into(),
            });
        assert!(bindings.validate().is_err());
        bindings.inputs.get_mut("state").unwrap().sources.pop();
        bindings.inputs.get_mut("state").unwrap().contract.qos = QosProfile::Sensor;
        assert!(bindings.validate().is_err());
    }
}
