//! Deterministic, local logical-time execution over explicitly wired protobuf values.
//!
//! Step routes are separate from ordinary stream/RPC bindings. Successful advancement
//! commits scheduler bookkeeping only: component state and external effects cannot be
//! rolled back. A failure after dispatch permanently faults this scheduler's epoch.

mod execution;
mod graph;
mod plan;
mod request;
mod scheduler;
mod status;
mod value;

pub use graph::{StepGraph, StepInput, StepOutput, StepPlanError, StepSchema, StepSource};
pub use request::{StepRequest, StepRequestError};
pub use scheduler::StepScheduler;
pub use status::{
    ClockPhase, ClockSnapshot, EpochId, NodeError, SchedulerError, StepExecutor, StepFailure,
    StepFailureKind, StepFuture, StepId, StepInputs, StepOutputs, StepReceipt, StepTick,
};
pub use value::{StepType, StepValue, StepValueError};

#[cfg(test)]
mod tests;
