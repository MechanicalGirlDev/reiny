//! Shared payloads for the controlled-apply JSONL protocol.

use serde::{Deserialize, Serialize};

use crate::DeploymentStatus;

/// Terminal outcome of a controlled apply, after its owned processes are reaped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyStatus {
    /// The deployment stopped normally.
    Stopped,
    /// Startup, execution or cleanup failed.
    Failed,
    /// The caller cancelled the deployment.
    Cancelled,
}

/// Payload of the versioned controlled-apply JSONL protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplyEvent {
    /// First event, before owner acquisition.
    ApplyStarted {
        /// Requested deployment identity.
        deployment: String,
    },
    /// An authenticated owner has published its control endpoint.
    OwnerListening {
        /// Owner process identity.
        pid: u32,
    },
    /// All managed modules have acknowledged readiness.
    Ready {
        /// Authenticated ready status.
        status: DeploymentStatus,
    },
    /// A subsequent authenticated deployment revision.
    Status {
        /// Current status.
        status: DeploymentStatus,
    },
    /// Exactly one terminal event after cleanup.
    Finished {
        /// Final outcome.
        status: ApplyStatus,
        /// Failure or cancellation details.
        error: Option<String>,
    },
}
