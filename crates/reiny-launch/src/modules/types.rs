use super::{
    BTreeMap, BTreeSet, Deserialize, EndpointContract, GitSource, PathBuf, PortKind, QosProfile,
    ReceiveBuffer, Replay, Retention, Serialize, invalid, validate_connection, validate_contract,
    validate_name,
};

/// Failure to load or connect a module declaration.
#[derive(Debug, thiserror::Error)]
pub enum ModuleError {
    /// A module file could not be read.
    #[error("reading {path}: {source}")]
    Io {
        /// The affected path.
        path: PathBuf,
        /// The underlying filesystem error.
        source: std::io::Error,
    },
    /// YAML did not match the module language.
    #[error("parsing {path}: {source}")]
    Yaml {
        /// The affected path.
        path: PathBuf,
        /// The YAML parsing error.
        source: serde_yaml::Error,
    },
    /// A declaration violates a module invariant.
    #[error("{module}: {message}")]
    Invalid {
        /// The module instance.
        module: String,
        /// The violated invariant.
        message: String,
    },
    /// A port reference does not name a visible endpoint.
    #[error("{module}: invalid reference '{reference}': {message}")]
    Reference {
        /// The module containing the reference.
        module: String,
        /// The supplied reference.
        reference: String,
        /// The missing or inaccessible endpoint.
        message: String,
    },
    /// Two connected ports declare different types.
    #[error("{port}: expected {expected}, found {actual}")]
    TypeMismatch {
        /// The destination port.
        port: String,
        /// Its declared type.
        expected: String,
        /// The source type.
        actual: String,
    },
    /// Module sources or endpoint aliases recurse without an executable source.
    #[error("module or endpoint alias cycle: {0}")]
    Cycle(String),
    /// Git module acquisition failed.
    #[error(transparent)]
    Acquisition(#[from] anyhow::Error),
}

/// One declared input or output.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortSpec {
    /// Application-owned semantic assertions; invocations cannot replace these.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub semantics: BTreeMap<String, String>,
    /// The expected message type.
    #[serde(default, rename = "type", skip_serializing_if = "String::is_empty")]
    pub type_name: String,
    /// A sibling output or parent-supplied input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<PortSources>,
    /// Endpoint operation, owned by the app definition.
    #[serde(default)]
    pub kind: PortKind,
    /// RPC response message type.
    #[serde(default)]
    pub response: Option<String>,
    /// Publication transport profile.
    #[serde(default)]
    pub qos: QosProfile,
    /// Publisher retained history.
    #[serde(default)]
    pub retention: Retention,
    /// Subscriber startup replay.
    #[serde(default)]
    pub replay: Replay,
    /// Subscriber queue policy.
    #[serde(default)]
    pub buffer: ReceiveBuffer,
}

/// One exact endpoint reference, or an explicit fan-in set.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum PortSources {
    /// One sibling output or parent input.
    One(String),
    /// Explicit stream fan-in.
    Many(Vec<String>),
}

impl PortSources {
    pub(super) fn references(&self) -> &[String] {
        match self {
            Self::One(source) => std::slice::from_ref(source),
            Self::Many(sources) => sources,
        }
    }
}

impl PortSpec {
    pub(super) fn contract(&self) -> EndpointContract {
        EndpointContract {
            semantics: self.semantics.clone(),
            type_name: self.type_name.clone(),
            kind: self.kind,
            response: self.response.clone(),
            qos: self.qos,
            retention: self.retention,
            replay: self.replay,
            buffer: self.buffer,
        }
    }
}

/// A caller's connection or optional type assertion, without app-owned policies.
///
/// Existing explicit default policy fields are accepted when reading YAML, but
/// nondefault policies are rejected rather than exposed on an invocation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "PortSpec")]
pub struct PortConnection {
    /// Optional assertion against the app's declared type.
    #[serde(rename = "type", skip_serializing_if = "String::is_empty")]
    pub type_name: String,
    /// Exact sibling outputs or a parent input; absent for output assertions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<PortSources>,
}

impl TryFrom<PortSpec> for PortConnection {
    type Error = ModuleError;

    fn try_from(port: PortSpec) -> Result<Self, Self::Error> {
        validate_connection("connection", &port)?;
        if !port.type_name.is_empty() {
            validate_contract("connection", &port.contract(), true)?;
        }
        Ok(Self {
            type_name: port.type_name,
            from: port.from,
        })
    }
}

/// A local module directory or a revision-pinned Git source.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ModuleSource {
    /// A directory relative to the declaring manifest.
    Local(PathBuf),
    /// A Git repository, reference and optional subdirectory.
    Git(GitSource),
}

/// One named child module invocation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleCall {
    /// The module definition to instantiate.
    pub source: ModuleSource,
    /// Provider slots remapped to configurations visible in the parent.
    #[serde(default)]
    pub providers: BTreeMap<String, String>,
    /// Input connections checked against the child's public contract.
    #[serde(default, rename = "in")]
    pub inputs: BTreeMap<String, PortConnection>,
    /// Public outputs visible to the caller.
    #[serde(default, rename = "out")]
    pub outputs: BTreeMap<String, PortConnection>,
    /// Instance configuration, relative to this caller, replacing the app default.
    #[serde(default)]
    pub config: Option<PathBuf>,
    /// Instance assets replacing app defaults, relative to the selected config's parent.
    #[serde(default)]
    pub config_assets: Option<Vec<PathBuf>>,
    /// Instance arguments replacing, not appending to, the app defaults.
    #[serde(default)]
    pub args: Option<Vec<String>>,
}

/// The supported provider implementations.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Spawn and supervise native executable modules.
    Process,
    /// Materialize immutable local file artifacts.
    Artifact,
}

/// A root-level provider configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSpec {
    /// The implementation used by this configuration.
    #[serde(rename = "type")]
    pub kind: ProviderKind,
    /// Prebuilt executable search directory, relative to the declaring root.
    #[serde(default = "current_dir")]
    pub bin_dir: PathBuf,
    /// Optional Zenoh configuration passed through without wrapping its schema.
    #[serde(default)]
    pub zenoh_config: Option<PathBuf>,
    /// Explicit transport endpoints passed to the executable modules.
    #[serde(default)]
    pub connect: Vec<String>,
}

fn current_dir() -> PathBuf {
    PathBuf::from(".")
}

/// Restart policy of a long-running executable module.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    /// Keep a failed module stopped until an explicit apply.
    #[default]
    Manual,
    /// Restart a failed task or an unexpectedly terminated service.
    OnFailure,
    /// Restart after service termination or task failure, but not task completion.
    Always,
}

/// Deployment-level behavior when a module cannot recover.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailurePolicy {
    /// Report the failed module while observing its siblings.
    #[default]
    Report,
    /// Cooperatively stop the whole deployment and require explicit resumption.
    SuspendDeployment,
}

/// The meaning of successful, unrequested process termination.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// A long-running process whose natural exit is a failure.
    #[default]
    Service,
    /// A finite process whose successful exit completes its work.
    Task,
}

/// A module's native executable declaration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunSpec {
    /// Long-running service or finite task.
    #[serde(default)]
    pub kind: RunKind,
    /// The provider slot used to execute this module.
    pub provider: String,
    /// The binary built or found in the provider's directory.
    pub bin: String,
    /// Additional executable names staged in the same immutable runtime bundle.
    #[serde(default)]
    pub companions: Vec<String>,
    /// Optional application configuration, resolved against this module.
    #[serde(default)]
    pub config: Option<PathBuf>,
    /// Declared assets, relative to the selected configuration's parent.
    #[serde(default)]
    pub config_assets: Vec<PathBuf>,
    /// Additional application arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Process restart policy.
    #[serde(default)]
    pub restart: RestartPolicy,
    /// Unrecoverable failure behavior.
    #[serde(default)]
    pub on_failure: FailurePolicy,
}

impl RunSpec {
    pub(crate) fn validate(&self, namespace: &str) -> Result<(), ModuleError> {
        self.validate_names(namespace)?;
        if self.config.is_none() && !self.config_assets.is_empty() {
            return Err(invalid(
                namespace,
                "config_assets requires the selected instance config",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_names(&self, namespace: &str) -> Result<(), ModuleError> {
        validate_name(&self.bin, "binary")?;
        let mut binaries = BTreeSet::from([self.bin.as_str()]);
        for companion in &self.companions {
            validate_name(companion, "companion binary")?;
            if !binaries.insert(companion.as_str()) {
                return Err(invalid(namespace, "duplicate executable in runtime bundle"));
            }
        }
        Ok(())
    }
}

/// A file managed by an artifact provider.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    /// Currently `artifact.file`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The artifact provider slot.
    pub provider: String,
    /// The local source file.
    pub source: PathBuf,
    /// Optional expected SHA-256 digest.
    #[serde(default)]
    pub sha256: Option<String>,
}
