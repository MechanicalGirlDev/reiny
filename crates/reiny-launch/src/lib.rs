//! Declarative modules, revision-pinned sources, immutable builds and owned deployments.
//!
//! Endpoint aliases resolve to their executable publisher, without forwarding data.
//! Communication observation is supplied by the CLI; this crate remains engine-neutral.

mod acquire;
mod artifacts;
mod deployment;
mod file_lock;
mod modules;
mod prepared;

pub use acquire::GitSource;
pub use artifacts::{BuildKind, BuildSpec};
pub use deployment::{
    DeploymentClient, DeploymentPhase, DeploymentStatus, ModuleObserver, ModuleStatus,
    StopSubscription, last_status, serve,
};
pub use modules::{
    ArtifactResource, DeploymentPlan, ExecutableModule, FailurePolicy, ModuleCall, ModuleError,
    ModuleManifest, ModuleSource, PortSpec, ProviderKind, ProviderSpec, ResourceSpec,
    RestartPolicy, RunSpec,
};
pub use prepared::{PreparedDeployment, PreparedModule};
