//! Declarative modules, revision-pinned sources, immutable builds and owned deployments.
//!
//! Endpoint aliases resolve to their executable publisher, without forwarding data.
//! Communication observation is supplied by the CLI; this crate remains engine-neutral.

mod acquire;
mod apply_progress;
mod artifacts;
mod deployment;
mod file_lock;
mod materialize;
mod modules;
mod prepared;
pub mod progress;
mod runner;
#[cfg(windows)]
mod windows_job;

#[cfg(windows)]
pub use windows_job::WindowsJobCompletion;

pub use acquire::GitSource;
pub use apply_progress::{ApplyEvent, ApplyStatus};
pub use artifacts::{BuildKind, BuildSpec, CargoBuild, CommandBuild};
pub use deployment::{
    DeploymentClient, DeploymentPhase, DeploymentStatus, ModuleObserver, ModuleStatus,
    StopSubscription, TerminalSubscription, last_status, serve,
};
pub use modules::{
    ArtifactResource, DeploymentPlan, ExecutableModule, FailurePolicy, ModuleCall, ModuleError,
    ModuleManifest, ModuleSource, PortAddress, PortConnection, PortSources, PortSpec, ProviderKind,
    ProviderSpec, ResolvedConnection, ResourceSpec, RestartPolicy, RunKind, RunSpec,
    check_manifest,
};
pub use prepared::{PreparedDeployment, PreparedModule};
pub use progress::{
    CancelToken, GroupKind, GroupState, PrepareError, PrepareEvent, PrepareObserver,
    PrepareOptions, PreparePhase, PrepareStatus,
};

/// Release version of the linked launch implementation.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Preparation event/control protocol version.
pub const PREPARE_PROTOCOL_VERSION: u32 = 1;
/// Public prepared deployment envelope version.
pub const PREPARED_FILE_VERSION: u32 = 1;
/// Controlled apply event/control protocol version.
pub const APPLY_PROTOCOL_VERSION: u32 = 1;
