//! In-process application jobs, separate from launch preparation and readiness.
//!
//! An explicit [`JobOwner`] owns a bounded set of workers. Handles only observe
//! and request cooperative cancellation; dropping a handle never stops work.
//! Progress is latest-value, not an event log. Only the latest progress and one
//! terminal result are retained, until their owner/handles/snapshots are dropped.
//! Payload sizes and snapshots retained by callers remain the caller's budget.
//!
//! Workers must yield, observe cancellation, and join any work they spawn before
//! returning. Neither cancellation nor a shutdown deadline kills native work.
//! Components should retain an owner and await [`JobOwner::shutdown`] in their
//! stop hook. Dropping the owner requests cancellation but cannot join; keep the
//! Tokio runtime alive through explicit shutdown when completion matters.

mod handle;
mod owner;
mod state;
mod types;

pub use handle::{JobContext, JobHandle};
pub use owner::JobOwner;
pub use types::{
    CancelRequest, JobId, JobOutcome, JobRevision, JobSnapshot, JobStatus, ShutdownError,
    ShutdownReport, StartError,
};

#[cfg(test)]
mod tests;
