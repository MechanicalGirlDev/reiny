use alloc::{collections::BTreeMap, string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use crate::bindings::ModuleReport;

/// Observable host/instance stage; a stage alone never grants authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostPhase {
    /// Bootstrap authentication has not completed.
    Authenticating,
    /// Opening contexts, constructing components or preparing hooks.
    Preparing,
    /// Local tickets retained; complete graph validation belongs to the owner.
    Prepared,
    /// Local admission committed; workers have not yet been released.
    Committed,
    /// Run hooks may execute.
    Running,
    /// Readiness withdrawn; hooks cancelling or cleaning up.
    Stopping,
    /// Every local task joined successfully.
    Stopped,
    /// Every local task joined, with a failure.
    Failed,
    /// Task completion could not be confirmed by the cleanup deadline.
    Unresponsive,
}

/// Stable protocol failure categories, not SDK error serialization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    /// Invalid frame, JSON, field set or protocol version.
    MalformedMessage,
    /// Nonce was rejected; diagnostics must not contain it.
    Authentication,
    /// A linked release or wire contract differs.
    Compatibility,
    /// Identity, generation, or prepared digest differs.
    Identity,
    /// Duplicate or out-of-order sequence.
    Sequence,
    /// A command arrived at an illegal stage.
    Transition,
    /// Required factory/revision is absent.
    Factory,
    /// Assignments or resolved endpoint bindings are invalid.
    Assignment,
    /// Opening a context failed.
    ContextOpen,
    /// Preparation failed.
    Prepare,
    /// Retaining admission tickets failed.
    Admission,
    /// Compiled report or whole-graph admission failed.
    Report,
    /// Committing tickets failed.
    Commit,
    /// Run hook failed.
    Run,
    /// Service returned before a stop request.
    UnexpectedExit,
    /// Component code unwound.
    Panic,
    /// Prepared or committed readiness was lost.
    ReadinessLost,
    /// Cleanup hook failed.
    StopHook,
    /// A lifecycle deadline expired.
    Timeout,
    /// The operation was cancelled.
    Cancelled,
    /// Control transport disconnected.
    Disconnected,
}

/// Failure context. Diagnostics are untrusted and must never contain secrets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostFailure {
    /// Machine-readable reason.
    pub code: FailureCode,
    /// Phase in which the failure was observed.
    pub phase: HostPhase,
    /// Canonical instance namespace, or none for host-level failures.
    pub instance: Option<String>,
    /// Human explanation; not parsed for control flow.
    pub message: String,
}

/// Retained observation of one exact instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceStatus {
    /// Last observed local stage.
    pub phase: HostPhase,
    /// Actual compiled report, when preparation reached that point.
    pub report: Option<ModuleReport>,
    /// First observed failure, never replaced by cleanup diagnostics.
    pub first_failure: Option<HostFailure>,
}

/// Coalescible snapshot indexed by canonical assigned namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostStatus {
    /// Last observed group stage.
    pub phase: HostPhase,
    /// Exact assigned instance set, in deterministic namespace order.
    pub instances: BTreeMap<String, InstanceStatus>,
}

/// Terminal local-task outcome; the owner still waits for the actual process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum HostOutcome {
    /// Every task joined after the requested stop, without failure.
    Stopped {},
    /// Every task joined but admission, runtime or cleanup failed.
    Failed {
        /// First failure retained independently of cleanup failures.
        primary: HostFailure,
        /// Additional cleanup failures, never silently discarded.
        cleanup: Vec<HostFailure>,
    },
    /// Some tasks did not join before the shared cleanup deadline.
    Unresponsive {
        /// First admission/runtime failure, if there was one.
        primary: Option<HostFailure>,
        /// Failures from cleanup that did finish.
        cleanup: Vec<HostFailure>,
        /// Sorted namespaces whose completion remains unconfirmed.
        instances: Vec<String>,
    },
}
