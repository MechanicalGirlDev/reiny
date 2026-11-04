use std::fmt;
use std::sync::Arc;

/// A process-local operation identity, never reused within this loaded library.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobId(pub(super) u64);

impl JobId {
    /// Numeric identity; not persistent or globally unique across processes.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Strictly increasing state revision within a job.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct JobRevision(pub(super) u64);

impl JobRevision {
    /// Numeric revision; a new job begins at zero.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Exactly one retained terminal result, shared without cloning user payloads.
#[derive(Debug, Eq, PartialEq)]
pub enum JobOutcome<T, E> {
    /// Work returned successfully before cancellation was committed.
    Succeeded(T),
    /// Work returned an application error (also preserved after cancellation).
    Failed(E),
    /// Cancellation preceded successful worker exit; cleanup has returned.
    Cancelled,
    /// The worker unwound with a panic. Requires a panic-unwinding build.
    Panicked,
    /// The runtime dropped the worker future before it returned.
    ///
    /// This does not certify that native work spawned by the worker has stopped.
    Interrupted,
}

/// Observable execution state. Cancellation is not a terminal result.
#[derive(Debug)]
pub enum JobStatus<T, E> {
    /// Worker has been scheduled and may report progress.
    Running,
    /// Cancellation requested; worker has not yet exited.
    Cancelling,
    /// Immutable retained result.
    Complete(Arc<JobOutcome<T, E>>),
}

impl<T, E> Clone for JobStatus<T, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Running => Self::Running,
            Self::Cancelling => Self::Cancelling,
            Self::Complete(outcome) => Self::Complete(Arc::clone(outcome)),
        }
    }
}

/// A consistent, owned view of one operation; no channel lock escapes the API.
#[derive(Debug)]
pub struct JobSnapshot<P, T, E> {
    /// The operation observed.
    pub id: JobId,
    /// Advances on accepted progress, first cancellation, and terminal commit.
    pub revision: JobRevision,
    /// Latest worker-reported progress, retained after completion.
    pub progress: Option<Arc<P>>,
    /// Whether cancellation won before completion, retained even after an error.
    pub cancellation_requested: bool,
    /// Current execution state.
    pub status: JobStatus<T, E>,
}

impl<P, T, E> Clone for JobSnapshot<P, T, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            revision: self.revision,
            progress: self.progress.clone(),
            cancellation_requested: self.cancellation_requested,
            status: self.status.clone(),
        }
    }
}

/// Result of an idempotent cancellation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelRequest {
    /// This request won the race against terminal completion.
    Requested,
    /// An earlier request already won; no revision was added.
    AlreadyRequested,
    /// The terminal result was already committed and cannot change.
    Finished,
}

/// Admission failure; the worker closure was not invoked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartError {
    /// Owner shutdown has begun.
    Closed,
    /// The owner's explicit concurrent job capacity is exhausted.
    AtCapacity,
    /// All process-local operation identities have been consumed.
    IdExhausted,
    /// Start was called outside an active Tokio runtime.
    RuntimeUnavailable,
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Closed => "job owner is closed",
            Self::AtCapacity => "job owner is at capacity",
            Self::IdExhausted => "job operation identities exhausted",
            Self::RuntimeUnavailable => "job start requires a Tokio runtime",
        })
    }
}

impl std::error::Error for StartError {}

/// Shutdown could not establish a bounded join deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownError {
    /// The grace duration exceeds the platform's representable instant range.
    ///
    /// Admission is closed and cancellation requested; all workers remain owned
    /// for a retry with a representable duration.
    InvalidDeadline,
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidDeadline => "job shutdown deadline is not representable",
        })
    }
}

impl std::error::Error for ShutdownError {}

/// Shutdown receipt. Empty `unresponsive` means every owned worker was joined.
#[derive(Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Workers still owned after the deadline, in start order.
    ///
    /// They remain cancellable/observable and can be joined by another shutdown.
    /// No terminal result is fabricated for them.
    pub unresponsive: Vec<JobId>,
    /// Joined workers that panicked or were interrupted, in start order.
    pub join_failures: Vec<JobId>,
}
