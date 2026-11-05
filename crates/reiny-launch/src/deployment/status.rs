use super::{
    BTreeMap, ComponentHostStatus, ComponentStatus, Deserialize, ModuleReport, PathBuf, Serialize,
};

/// Overall observed deployment state.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentPhase {
    /// Processes have been started but their contracts are not ready.
    Starting,
    /// Every desired process and connected schema is ready.
    Ready,
    /// Stop was requested; owned process trees have not yet been reaped.
    Stopping,
    /// At least one desired process is unavailable or restarting.
    Degraded,
    /// Failure policy stopped the deployment until explicit apply.
    Suspended,
    /// All owned processes have been stopped and reaped.
    Stopped,
    /// Every desired task finished successfully and its process tree was reaped.
    Completed,
    /// A contract or owner operation failed.
    Failed,
}

/// One desired module and its actual owned process.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ModuleStatus {
    /// Canonical deployment-qualified namespace.
    pub namespace: String,
    /// Desired executable/configuration fingerprint.
    pub desired_fingerprint: String,
    /// Fingerprint of the actual process, absent when stopped.
    pub actual_fingerprint: Option<String>,
    /// Informational PID, never authority to stop a process.
    pub pid: Option<u32>,
    /// Monotonic spawn generation within this owner.
    pub generation: u64,
    /// Observed module lifecycle state.
    pub phase: DeploymentPhase,
    /// Last observed error or termination.
    pub error: Option<String>,
    /// Last actual typed-port report from this generation.
    pub report: Option<ModuleReport>,
}

/// Persisted, credential-free desired/actual deployment status.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DeploymentStatus {
    /// Status format version.
    pub version: u32,
    /// Canonical deployment root.
    pub root: PathBuf,
    /// Deployment identity.
    pub deployment: String,
    /// Communication domain.
    pub domain: String,
    /// Unique owner incarnation; not an authentication credential.
    pub owner_generation: String,
    /// Informational owner PID.
    pub owner_pid: u32,
    /// Whether this status was verified against the active owner.
    pub owner_alive: bool,
    /// Monotonic state revision for bounded event waits.
    pub revision: u64,
    /// Overall lifecycle state.
    pub phase: DeploymentPhase,
    /// Desired modules and their actual owned processes.
    pub modules: BTreeMap<String, ModuleStatus>,
    /// Hosted process observations, separate from legacy executable modules.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub component_hosts: BTreeMap<String, ComponentHostStatus>,
    /// Hosted instance observations; shared hosts own their process lifecycle.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub components: BTreeMap<String, ComponentStatus>,
    /// Managed resource addresses and paths.
    pub resources: BTreeMap<String, PathBuf>,
    /// Last deployment-level failure.
    pub error: Option<String>,
}
