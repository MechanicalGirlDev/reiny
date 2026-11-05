use super::{
    BTreeMap, BuildSpec, ComponentHost, ComponentHostSpec, ComponentModule, ComponentSpec,
    Deserialize, EndpointContract, ModuleBindings, ModuleCall, PathBuf, PortDelegation, PortSpec,
    ProviderSpec, ResourceSpec, RunSpec, Serialize, hosted,
};

/// The contents of one `main.yaml`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleManifest {
    /// Module language version: three for legacy modules, four for hosted leaves.
    pub version: u32,
    /// Root deployment identity.
    #[serde(default)]
    pub deployment: Option<String>,
    /// Root communication domain (defaults to the deployment name).
    #[serde(default)]
    pub domain: Option<String>,
    /// Root provider configurations.
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderSpec>,
    /// Child module invocations.
    #[serde(default)]
    pub modules: BTreeMap<String, ModuleCall>,
    /// Native executable declaration (mutually exclusive with child modules).
    #[serde(default)]
    pub run: Option<RunSpec>,
    /// Registered component factory, mutually exclusive with run and modules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<ComponentSpec>,
    /// Root-only process hosts for explicitly placed component instances.
    #[serde(
        default,
        deserialize_with = "hosted::unique_map",
        skip_serializing_if = "Option::is_none"
    )]
    pub component_hosts: Option<BTreeMap<String, ComponentHostSpec>>,
    /// Root-only full canonical instance namespace to host name assignments.
    #[serde(
        default,
        deserialize_with = "hosted::unique_map",
        skip_serializing_if = "Option::is_none"
    )]
    pub placement: Option<BTreeMap<String, String>>,
    /// Optional explicit build for the native executable.
    #[serde(default)]
    pub build: Option<BuildSpec>,
    /// Public input contract.
    #[serde(default, rename = "in")]
    pub inputs: BTreeMap<String, PortSpec>,
    /// Public output contract.
    #[serde(default, rename = "out")]
    pub outputs: BTreeMap<String, PortSpec>,
    /// Named file artifacts.
    #[serde(default)]
    pub resources: BTreeMap<String, ResourceSpec>,
    /// Exclusive endpoint subsets implemented by owned child processes.
    #[serde(default)]
    pub owned_children: BTreeMap<String, PortDelegation>,
}

/// A fully resolved executable module.
#[derive(Clone, Debug, Serialize)]
pub struct ExecutableModule {
    /// Canonical namespace, including the deployment.
    pub namespace: String,
    /// Directory of this instance's source manifest.
    pub module_dir: PathBuf,
    /// Executable settings.
    pub run: RunSpec,
    /// Optional build settings.
    pub build: Option<BuildSpec>,
    /// The selected provider configuration.
    pub provider: ProviderSpec,
    /// Exact runtime port bindings, without forwarding publishers.
    pub bindings: ModuleBindings,
}

/// One file artifact to materialize.
#[derive(Clone, Debug, Serialize)]
pub struct ArtifactResource {
    /// Stable deployment-relative resource address.
    pub address: String,
    /// Absolute source path.
    pub source: PathBuf,
    /// Optional expected content digest.
    pub sha256: Option<String>,
}

/// A validated deployment, before any build or process start.
#[derive(Clone, Debug, Serialize)]
pub struct DeploymentPlan {
    /// Directory of the root `main.yaml`.
    pub root: PathBuf,
    /// Stable owner/state identity.
    pub deployment: String,
    /// Shared communication domain.
    pub domain: String,
    /// Definition directories of every instance, including composite modules.
    pub module_sources: BTreeMap<String, PathBuf>,
    /// Executable modules sorted by namespace.
    pub nodes: Vec<ExecutableModule>,
    /// Hosted component instances sorted by canonical namespace.
    pub components: Vec<ComponentModule>,
    /// Process hosts keyed by the root's explicit host names.
    pub component_hosts: BTreeMap<String, ComponentHost>,
    /// Concrete connections sorted by destination namespace and input name,
    /// preserving declared fan-in order within each input.
    pub connections: Vec<ResolvedConnection>,
    /// Managed immutable files.
    pub resources: Vec<ArtifactResource>,
}

/// One named input or output of a module instance.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct PortAddress {
    /// Deployment-relative module namespace.
    pub namespace: String,
    /// Whether this address names an input rather than an output.
    pub input: bool,
    /// Declared port name.
    pub name: String,
}

/// One resolved route between executable ports, without intermediate forwarding.
#[derive(Clone, Debug, Serialize)]
pub struct ResolvedConnection {
    /// Concrete executable output publishing this route.
    pub source: PortAddress,
    /// Concrete executable input receiving this route.
    pub destination: PortAddress,
    /// Intermediate alias addresses, ordered from destination toward source,
    /// excluding both concrete endpoints.
    pub via: Vec<PortAddress>,
    /// Contract of the concrete executable output.
    pub contract: EndpointContract,
}
