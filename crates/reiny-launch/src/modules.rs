//! Strict module manifests and engine-neutral endpoint resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use reiny_core::bindings::{
    CONTRACT_VERSION, EndpointContract, InputBinding, ModuleBindings, OutputBinding,
    PortDelegation, PortKind, QosProfile, ReceiveBuffer, Replay, Retention,
};
use serde::{Deserialize, Serialize};

use crate::acquire::{GitSource, SourceResolver};
use crate::artifacts::BuildSpec;

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
    fn references(&self) -> &[String] {
        match self {
            Self::One(source) => std::slice::from_ref(source),
            Self::Many(sources) => sources,
        }
    }
}

impl PortSpec {
    fn contract(&self) -> EndpointContract {
        EndpointContract {
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
    pub inputs: BTreeMap<String, PortSpec>,
    /// Public outputs visible to the caller.
    #[serde(default, rename = "out")]
    pub outputs: BTreeMap<String, PortSpec>,
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
        validate_name(&self.bin, "binary")?;
        let mut binaries = BTreeSet::from([self.bin.as_str()]);
        for companion in &self.companions {
            validate_name(companion, "companion binary")?;
            if !binaries.insert(companion.as_str()) {
                return Err(invalid(namespace, "duplicate executable in runtime bundle"));
            }
        }
        if self.config.is_none() && !self.config_assets.is_empty() {
            return Err(invalid(
                namespace,
                "config_assets requires the selected instance config",
            ));
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

/// The contents of one `main.yaml`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleManifest {
    /// Module language version; currently two.
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
    /// Build-time type definitions; never interpreted as runtime actions.
    #[serde(default)]
    pub schema: Option<serde_yaml::Value>,
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
    /// Managed immutable files.
    pub resources: Vec<ArtifactResource>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PortAddress {
    namespace: String,
    input: bool,
    name: String,
}

impl std::fmt::Display for PortAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.namespace,
            if self.input { "in" } else { "out" },
            self.name
        )
    }
}

struct Instance {
    dir: PathBuf,
    manifest: ModuleManifest,
    providers: BTreeMap<String, ProviderSpec>,
}

struct Loader {
    resolver: SourceResolver,
    instances: BTreeMap<String, Instance>,
    source_stack: Vec<PathBuf>,
}

impl DeploymentPlan {
    /// Load a root directory or its `main.yaml`, resolve sources and check all ports.
    ///
    /// No build or executable module is run. `update` explicitly refreshes Git refs.
    pub fn load(path: &Path, update: bool) -> Result<Self, ModuleError> {
        let root = manifest_dir(path)?;
        let manifest = read_manifest(&root)?;
        let deployment = manifest.deployment.clone().ok_or_else(|| {
            invalid(
                root.display().to_string(),
                "root main.yaml requires deployment",
            )
        })?;
        validate_name(&deployment, "deployment")?;
        let domain = manifest
            .domain
            .clone()
            .unwrap_or_else(|| deployment.clone());
        validate_name(&domain, "domain")?;
        if !manifest.inputs.is_empty() {
            return Err(invalid(&deployment, "root inputs have no supplying parent"));
        }
        let mut providers = manifest.providers.clone();
        for (name, provider) in &mut providers {
            validate_name(name, "provider")?;
            provider.bin_dir = absolute(&root, &provider.bin_dir);
            if let Some(config) = &mut provider.zenoh_config {
                *config = absolute(&root, config);
            }
        }
        let mut loader = Loader {
            resolver: SourceResolver::open(&root, update)?,
            instances: BTreeMap::new(),
            source_stack: Vec::new(),
        };
        loader.visit(&deployment, &root, manifest, providers, true)?;
        let (nodes, resources) = loader.connect()?;
        let module_sources = loader
            .instances
            .iter()
            .map(|(name, instance)| (name.clone(), instance.dir.clone()))
            .collect();
        loader.resolver.finish()?;
        Ok(Self {
            root,
            deployment,
            domain,
            module_sources,
            nodes,
            resources,
        })
    }
}

fn manifest_dir(path: &Path) -> Result<PathBuf, ModuleError> {
    let path = if path.is_file() {
        if path.file_name().is_none_or(|name| name != "main.yaml") {
            return Err(invalid(
                path.display().to_string(),
                "expected main.yaml or its directory",
            ));
        }
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    } else {
        path
    };
    std::fs::canonicalize(path).map_err(|source| ModuleError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_manifest(dir: &Path) -> Result<ModuleManifest, ModuleError> {
    let path = dir.join("main.yaml");
    let text = std::fs::read_to_string(&path).map_err(|source| ModuleError::Io {
        path: path.clone(),
        source,
    })?;
    serde_yaml::from_str(&text).map_err(|source| ModuleError::Yaml { path, source })
}

fn invalid(module: impl Into<String>, message: impl Into<String>) -> ModuleError {
    ModuleError::Invalid {
        module: module.into(),
        message: message.into(),
    }
}

fn validate_name(name: &str, kind: &str) -> Result<(), ModuleError> {
    if name.is_empty()
        || name == "in"
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(invalid(
            name,
            format!("invalid {kind} name; use letters, digits, '_' or '-'"),
        ));
    }
    Ok(())
}

fn validate_ports(namespace: &str, ports: &BTreeMap<String, PortSpec>) -> Result<(), ModuleError> {
    for (name, port) in ports {
        validate_name(name, "port")?;
        if !port.type_name.is_empty()
            && !port.type_name.split('.').all(|part| {
                !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            })
        {
            return Err(invalid(
                namespace,
                format!("invalid message type '{}'", port.type_name),
            ));
        }
    }
    Ok(())
}

fn absolute(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

impl Loader {
    fn visit(
        &mut self,
        namespace: &str,
        dir: &Path,
        manifest: ModuleManifest,
        providers: BTreeMap<String, ProviderSpec>,
        root: bool,
    ) -> Result<(), ModuleError> {
        let dir = std::fs::canonicalize(dir).map_err(|source| ModuleError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        if self.source_stack.contains(&dir) {
            return Err(ModuleError::Cycle(format!("source {}", dir.display())));
        }
        validate_manifest(namespace, &manifest, &providers, root)?;
        self.source_stack.push(dir.clone());
        for (name, call) in &manifest.modules {
            validate_name(name, "module")?;
            validate_ports(namespace, &call.inputs)?;
            validate_ports(namespace, &call.outputs)?;
            let child_namespace = format!("{namespace}/{name}");
            let child_dir = match &call.source {
                ModuleSource::Local(path) => absolute(&dir, path),
                ModuleSource::Git(source) => {
                    self.resolver.resolve(&child_namespace, &dir, source)?
                }
            };
            let mut child = read_manifest(&child_dir)?;
            if call.inputs.keys().ne(child.inputs.keys()) {
                return Err(invalid(
                    &child_namespace,
                    "caller must connect exactly the child's declared inputs",
                ));
            }
            for (name, port) in &call.inputs {
                check_type(
                    &format!("{child_namespace}.in.{name}"),
                    &child.inputs[name],
                    port,
                )?;
                if port.from.is_none() {
                    return Err(invalid(&child_namespace, "caller inputs require from"));
                }
                validate_connection(&child_namespace, port)?;
            }
            for (name, port) in &call.outputs {
                let declared = child.outputs.get(name).ok_or_else(|| {
                    invalid(&child_namespace, format!("child has no output '{name}'"))
                })?;
                check_type(&format!("{child_namespace}.out.{name}"), declared, port)?;
                if port.from.is_some() {
                    return Err(invalid(
                        &child_namespace,
                        "caller output contracts cannot contain from",
                    ));
                }
                validate_connection(&child_namespace, port)?;
            }
            if call.config.is_some() || call.config_assets.is_some() || call.args.is_some() {
                let run = child.run.as_mut().ok_or_else(|| {
                    invalid(
                        &child_namespace,
                        "config, config_assets and args overrides require an executable app",
                    )
                })?;
                if let Some(config) = &call.config {
                    run.config = Some(absolute(&dir, config));
                }
                if let Some(config_assets) = &call.config_assets {
                    run.config_assets.clone_from(config_assets);
                }
                if let Some(args) = &call.args {
                    run.args.clone_from(args);
                }
            }
            let mut child_providers = providers.clone();
            for (slot, name) in &call.providers {
                validate_name(slot, "provider slot")?;
                let provider = providers.get(name).ok_or_else(|| {
                    invalid(namespace, format!("unknown provider mapping '{name}'"))
                })?;
                child_providers.insert(slot.clone(), provider.clone());
            }
            self.visit(&child_namespace, &child_dir, child, child_providers, false)?;
        }
        self.source_stack.pop();
        self.instances.insert(
            namespace.to_string(),
            Instance {
                dir,
                manifest,
                providers,
            },
        );
        Ok(())
    }

    fn reference(&self, namespace: &str, text: &str) -> Result<PortAddress, ModuleError> {
        let error = |message: &str| ModuleError::Reference {
            module: namespace.to_string(),
            reference: text.to_string(),
            message: message.to_string(),
        };
        let (owner, name) = text
            .split_once('.')
            .ok_or_else(|| error("use sibling.port or in.port"))?;
        if name.contains('.') {
            return Err(error(
                "module internals and absolute paths are not public references",
            ));
        }
        let instance = self
            .instances
            .get(namespace)
            .ok_or_else(|| error("unknown module"))?;
        if owner == "in" {
            if !instance.manifest.inputs.contains_key(name) {
                return Err(error("input is not declared"));
            }
            Ok(PortAddress {
                namespace: namespace.to_string(),
                input: true,
                name: name.to_string(),
            })
        } else {
            instance
                .manifest
                .modules
                .get(owner)
                .ok_or_else(|| error("sibling module is not declared"))?;
            let child = self
                .instances
                .get(&format!("{namespace}/{owner}"))
                .ok_or_else(|| error("sibling module is not resolved"))?;
            if !child.manifest.outputs.contains_key(name) {
                return Err(error("sibling output is not declared by its app"));
            }
            Ok(PortAddress {
                namespace: format!("{namespace}/{owner}"),
                input: false,
                name: name.to_string(),
            })
        }
    }

    fn connect(&self) -> Result<(Vec<ExecutableModule>, Vec<ArtifactResource>), ModuleError> {
        let mut edges = BTreeMap::new();
        let mut port_types = BTreeMap::new();
        let mut terminals = BTreeMap::new();
        for (namespace, instance) in &self.instances {
            for (name, port) in &instance.manifest.inputs {
                port_types.insert(
                    PortAddress {
                        namespace: namespace.clone(),
                        input: true,
                        name: name.clone(),
                    },
                    port.contract(),
                );
            }
            for (name, port) in &instance.manifest.outputs {
                let address = PortAddress {
                    namespace: namespace.clone(),
                    input: false,
                    name: name.clone(),
                };
                port_types.insert(address.clone(), port.contract());
                if instance.manifest.run.is_some() {
                    terminals.insert(address, (namespace.clone(), port.contract()));
                } else if let Some(from) = &port.from {
                    edges.insert(address, self.references(namespace, from)?);
                }
            }
            for (child, call) in &instance.manifest.modules {
                for (name, port) in &call.inputs {
                    let from = port
                        .from
                        .as_ref()
                        .ok_or_else(|| invalid(namespace, "missing input reference"))?;
                    edges.insert(
                        PortAddress {
                            namespace: format!("{namespace}/{child}"),
                            input: true,
                            name: name.clone(),
                        },
                        self.references(namespace, from)?,
                    );
                }
            }
        }
        for (destination, sources) in &edges {
            let expected = port_types
                .get(destination)
                .ok_or_else(|| invalid(destination.to_string(), "unknown destination"))?;
            for source in sources {
                let endpoints = resolve_endpoint(source, &edges, &terminals)?;
                for (_, actual) in endpoints {
                    check_contract(&destination.to_string(), expected, &actual)?;
                }
            }
        }
        for address in edges.keys() {
            resolve_endpoint(address, &edges, &terminals)?;
        }
        let mut nodes = Vec::new();
        let mut resources = Vec::new();
        for (namespace, instance) in &self.instances {
            resources.extend(instance.resources(namespace)?);
            if let Some(node) = instance.executable(namespace, &edges, &terminals)? {
                nodes.push(node);
            }
        }
        if nodes.is_empty() {
            return Err(invalid("deployment", "no executable modules are declared"));
        }
        Ok((nodes, resources))
    }

    fn references(
        &self,
        namespace: &str,
        sources: &PortSources,
    ) -> Result<Vec<PortAddress>, ModuleError> {
        if sources.references().is_empty() {
            return Err(invalid(namespace, "fan-in requires at least one source"));
        }
        let mut unique = BTreeSet::new();
        sources
            .references()
            .iter()
            .map(|text| {
                let address = self.reference(namespace, text)?;
                if !unique.insert(address.clone()) {
                    return Err(invalid(namespace, "duplicate endpoint in fan-in"));
                }
                Ok(address)
            })
            .collect()
    }
}

fn validate_manifest(
    namespace: &str,
    manifest: &ModuleManifest,
    providers: &BTreeMap<String, ProviderSpec>,
    root: bool,
) -> Result<(), ModuleError> {
    if manifest.version != CONTRACT_VERSION {
        return Err(invalid(
            namespace,
            format!("unsupported module version {}", manifest.version),
        ));
    }
    if !root
        && (manifest.deployment.is_some()
            || manifest.domain.is_some()
            || !manifest.providers.is_empty())
    {
        return Err(invalid(
            namespace,
            "deployment, domain and provider configurations belong to the root",
        ));
    }
    if manifest.run.is_some() && !manifest.modules.is_empty() {
        return Err(invalid(namespace, "run and modules are mutually exclusive"));
    }
    if manifest.build.is_some() && manifest.run.is_none() {
        return Err(invalid(namespace, "build requires run"));
    }
    validate_ports(namespace, &manifest.inputs)?;
    validate_ports(namespace, &manifest.outputs)?;
    for port in manifest.inputs.values() {
        validate_contract(namespace, &port.contract(), true)?;
        if port.from.is_some() {
            return Err(invalid(
                namespace,
                "module inputs are supplied by their caller, not by from",
            ));
        }
    }
    if let Some(run) = &manifest.run {
        run.validate(namespace)?;
        providers
            .get(&run.provider)
            .filter(|provider| provider.kind == ProviderKind::Process)
            .ok_or_else(|| {
                invalid(
                    namespace,
                    format!("unknown process provider '{}'", run.provider),
                )
            })?;
        let mut types = BTreeSet::new();
        for port in manifest.outputs.values() {
            validate_contract(namespace, &port.contract(), false)?;
            if port.from.is_some() {
                return Err(invalid(
                    namespace,
                    "executable outputs cannot forward another module",
                ));
            }
            let segment = port.type_name.rsplit('.').next().unwrap_or(&port.type_name);
            if !types.insert(segment) {
                return Err(invalid(
                    namespace,
                    format!("multiple independent outputs use transport type '{segment}'"),
                ));
            }
        }
    } else {
        for port in manifest.inputs.values() {
            if port.replay != Replay::Live || port.buffer != ReceiveBuffer::Fifo {
                return Err(invalid(
                    namespace,
                    "composite inputs forward contracts; receive policy belongs to the executable app",
                ));
            }
        }
        for port in manifest.outputs.values() {
            if port.from.is_none() {
                return Err(invalid(namespace, "composite outputs require from"));
            }
            validate_connection(namespace, port)?;
        }
    }
    let (mut delegated_inputs, mut delegated_outputs) = (BTreeSet::new(), BTreeSet::new());
    if !manifest.owned_children.is_empty() && manifest.run.is_none() {
        return Err(invalid(namespace, "owned_children requires run"));
    }
    for (name, child) in &manifest.owned_children {
        validate_name(name, "owned child")?;
        for (names, ports, seen) in [
            (&child.inputs, &manifest.inputs, &mut delegated_inputs),
            (&child.outputs, &manifest.outputs, &mut delegated_outputs),
        ] {
            for port in names {
                if !ports.contains_key(port) || !seen.insert(port) {
                    return Err(invalid(
                        namespace,
                        format!(
                            "owned child '{name}' has undefined or multiply delegated port '{port}'"
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

impl Instance {
    fn executable(
        &self,
        namespace: &str,
        edges: &BTreeMap<PortAddress, Vec<PortAddress>>,
        terminals: &BTreeMap<PortAddress, (String, EndpointContract)>,
    ) -> Result<Option<ExecutableModule>, ModuleError> {
        let Some(run) = &self.manifest.run else {
            return Ok(None);
        };
        let mut inputs = BTreeMap::new();
        for (name, port) in &self.manifest.inputs {
            let address = PortAddress {
                namespace: namespace.to_string(),
                input: true,
                name: name.clone(),
            };
            let endpoints = resolve_endpoint(&address, edges, terminals)?;
            let contract = port.contract();
            if contract.kind == PortKind::Rpc && endpoints.len() != 1 {
                return Err(invalid(
                    namespace,
                    "RPC inputs require exactly one endpoint",
                ));
            }
            let mut sources = Vec::new();
            for (source, output) in endpoints {
                check_contract(&address.to_string(), &contract, &output)?;
                if contract.replay == Replay::Last && output.retention != Retention::Last {
                    return Err(invalid(
                        address.to_string(),
                        "replay last requires retained output",
                    ));
                }
                if sources.contains(&source) {
                    return Err(invalid(
                        address.to_string(),
                        "multiple aliases resolve to the same publisher",
                    ));
                }
                sources.push(source);
            }
            inputs.insert(name.clone(), InputBinding { sources, contract });
        }
        let outputs = self
            .manifest
            .outputs
            .iter()
            .map(|(name, port)| {
                (
                    name.clone(),
                    OutputBinding {
                        contract: port.contract(),
                    },
                )
            })
            .collect();
        let mut run = run.clone();
        if let Some(config) = &mut run.config {
            *config = absolute(&self.dir, config);
            if !config.is_file() {
                return Err(invalid(
                    namespace,
                    format!("configuration file not found: {}", config.display()),
                ));
            }
        }
        let provider = self
            .providers
            .get(&run.provider)
            .ok_or_else(|| invalid(namespace, "unknown executable provider"))?
            .clone();
        let executables = std::iter::once(run.bin.clone())
            .chain(run.companions.iter().cloned())
            .collect();
        Ok(Some(ExecutableModule {
            namespace: namespace.to_string(),
            module_dir: self.dir.clone(),
            run,
            build: self.manifest.build.clone(),
            provider,
            bindings: ModuleBindings {
                version: CONTRACT_VERSION,
                namespace: namespace.to_string(),
                endpoint_namespace: namespace.to_string(),
                inputs,
                outputs,
                children: self.manifest.owned_children.clone(),
                executables,
            },
        }))
    }

    fn resources(&self, namespace: &str) -> Result<Vec<ArtifactResource>, ModuleError> {
        let mut resources = Vec::new();
        for (name, resource) in &self.manifest.resources {
            validate_name(name, "resource")?;
            let provider = self.providers.get(&resource.provider).ok_or_else(|| {
                invalid(
                    namespace,
                    format!("unknown artifact provider '{}'", resource.provider),
                )
            })?;
            if resource.kind != "artifact.file" || provider.kind != ProviderKind::Artifact {
                return Err(invalid(
                    namespace,
                    "file resources require type artifact.file and an artifact provider",
                ));
            }
            resources.push(ArtifactResource {
                address: format!("{namespace}/{name}"),
                source: absolute(&self.dir, &resource.source),
                sha256: resource.sha256.clone(),
            });
        }
        Ok(resources)
    }
}

fn check_type(port: &str, expected: &PortSpec, actual: &PortSpec) -> Result<(), ModuleError> {
    if actual.type_name.is_empty() || expected.type_name == actual.type_name {
        Ok(())
    } else {
        Err(ModuleError::TypeMismatch {
            port: port.to_string(),
            expected: expected.type_name.clone(),
            actual: actual.type_name.clone(),
        })
    }
}

fn resolve_endpoint(
    address: &PortAddress,
    edges: &BTreeMap<PortAddress, Vec<PortAddress>>,
    terminals: &BTreeMap<PortAddress, (String, EndpointContract)>,
) -> Result<Vec<(String, EndpointContract)>, ModuleError> {
    fn walk(
        address: &PortAddress,
        edges: &BTreeMap<PortAddress, Vec<PortAddress>>,
        terminals: &BTreeMap<PortAddress, (String, EndpointContract)>,
        visited: &mut BTreeSet<PortAddress>,
        found: &mut Vec<(String, EndpointContract)>,
    ) -> Result<(), ModuleError> {
        if let Some(endpoint) = terminals.get(address) {
            found.push(endpoint.clone());
            return Ok(());
        }
        if !visited.insert(address.clone()) {
            return Err(ModuleError::Cycle(address.to_string()));
        }
        let sources = edges
            .get(address)
            .ok_or_else(|| invalid(address.to_string(), "port has no executable publisher"))?;
        for source in sources {
            walk(source, edges, terminals, visited, found)?;
        }
        visited.remove(address);
        Ok(())
    }
    let mut found = Vec::new();
    walk(address, edges, terminals, &mut BTreeSet::new(), &mut found)?;
    Ok(found)
}

fn validate_connection(namespace: &str, port: &PortSpec) -> Result<(), ModuleError> {
    if port.kind != PortKind::Stream
        || port.response.is_some()
        || port.qos != QosProfile::Reliable
        || port.retention != Retention::Volatile
        || port.replay != Replay::Live
        || port.buffer != ReceiveBuffer::Fifo
    {
        return Err(invalid(
            namespace,
            "callers connect app-owned endpoints; they cannot redefine endpoint policy",
        ));
    }
    Ok(())
}

pub(crate) fn validate_contract(
    namespace: &str,
    contract: &EndpointContract,
    input: bool,
) -> Result<(), ModuleError> {
    if contract.type_name.is_empty() {
        return Err(invalid(
            namespace,
            "executable endpoints and public inputs require type",
        ));
    }
    match contract.kind {
        PortKind::Stream => {
            if contract.response.is_some() {
                return Err(invalid(
                    namespace,
                    "stream endpoints cannot declare response",
                ));
            }
        }
        PortKind::Rpc => {
            let response = contract
                .response
                .as_deref()
                .ok_or_else(|| invalid(namespace, "RPC endpoints require response"))?;
            if response.is_empty()
                || !response.split('.').all(|part| {
                    !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
            {
                return Err(invalid(namespace, "invalid RPC response type"));
            }
            if contract.retention != Retention::Volatile
                || contract.replay != Replay::Live
                || contract.buffer != ReceiveBuffer::Fifo
                || contract.qos != QosProfile::Reliable
            {
                return Err(invalid(
                    namespace,
                    "stream policy does not apply to RPC endpoints",
                ));
            }
        }
    }
    if matches!(contract.buffer, ReceiveBuffer::Latest(0)) {
        return Err(invalid(namespace, "latest receive depth must be positive"));
    }
    if input && (contract.retention != Retention::Volatile || contract.qos != QosProfile::Reliable)
    {
        return Err(invalid(namespace, "qos and retention belong to outputs"));
    }
    if !input && (contract.replay != Replay::Live || contract.buffer != ReceiveBuffer::Fifo) {
        return Err(invalid(namespace, "replay and buffer belong to inputs"));
    }
    Ok(())
}

fn check_contract(
    port: &str,
    expected: &EndpointContract,
    actual: &EndpointContract,
) -> Result<(), ModuleError> {
    if !expected.type_name.is_empty()
        && (expected.type_name != actual.type_name
            || expected.kind != actual.kind
            || expected.response != actual.response)
    {
        return Err(ModuleError::TypeMismatch {
            port: port.to_string(),
            expected: format!(
                "{:?} {} {:?}",
                expected.kind, expected.type_name, expected.response
            ),
            actual: format!(
                "{:?} {} {:?}",
                actual.kind, actual.type_name, actual.response
            ),
        });
    }
    Ok(())
}
