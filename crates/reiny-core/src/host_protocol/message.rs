use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

use super::{
    Compatibility, FactoryRegistration, HostFailure, HostOutcome, HostStatus, Milliseconds, Nonce,
    Prepare,
};
use crate::bindings::ModuleReport;

/// Initial host proof of bootstrap-secret possession and linked contracts.
///
/// The owner verifies identity/digests, exact compatibility, one-use nonce and
/// every required factory/revision. Extra registrations grant no assignments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authenticate {
    /// Secret inherited through stdin, never argv or persisted status.
    pub nonce: Nonce,
    /// Actual linked release and wire versions.
    pub compatibility: Compatibility,
    /// Available allowlisted implementations, sorted and unique.
    pub registrations: Vec<FactoryRegistration>,
}

/// Owner-to-host commands; the receiver enforces legal lifecycle transitions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum OwnerMessage {
    /// Explicitly negotiated synchronous-step extension.
    Steps(alloc::boxed::Box<super::step::StepOwnerMessage>),
    /// Authentication succeeded, not graph admission or permission to run.
    Authenticated {
        /// Owner's exact linked contracts, for the host to check.
        compatibility: Compatibility,
        /// Exact factory/revisions required by the prepared assignments.
        required_factories: Vec<FactoryRegistration>,
    },
    /// Prepare only; hold tickets and emit compiled reports without running.
    Prepare(Prepare),
    /// Commit retained tickets after owner-wide graph validation; no run hooks.
    Commit {},
    /// Release workers only after every host acknowledged commit.
    ///
    /// Each host must recheck local readiness loss and cancellation. Delivery
    /// races are possible: this barrier is not a distributed transaction.
    Run {},
    /// Withdraw readiness, cancel hooks and clean up. Repetition is harmless.
    Stop {
        /// Why this admission attempt or running group is being stopped.
        reason: StopReason,
        /// Remaining shared cleanup budget.
        remaining_budget: Milliseconds,
    },
}

/// Host-to-owner events; failures and terminal outcomes must not be coalesced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum HostMessage {
    /// Explicit step capability negotiation and correlated execution events.
    Steps(alloc::boxed::Box<super::step::StepHostMessage>),
    /// First host message, before constructing any application component.
    Authenticate(Authenticate),
    /// One actual compiled report, not an admission ticket or approved binding.
    Report(ModuleReport),
    /// Every assigned report was emitted and prepared tickets remain retained.
    PreparedComplete {},
    /// Every local ticket committed and local readiness-loss checks passed.
    Committed {},
    /// Workers were released after local loss/stop checks.
    Running {},
    /// Coalescible observation; never substitutes for phase acknowledgements.
    Status(HostStatus),
    /// Structured failure event; retain the first failure through cleanup.
    Failure(HostFailure),
    /// Terminal local-task result, not proof of process exit or reaping.
    Outcome(HostOutcome),
}

/// Cooperative shutdown cause, separate from a failure diagnostic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// Explicit stop requested by the owner.
    Requested,
    /// An admission attempt failed or was cancelled before activation.
    AdmissionAborted,
    /// The deployment's failure policy stops this host.
    DeploymentFailure,
    /// Owner shutdown, lease loss, or control-channel EOF.
    OwnerLost,
}
