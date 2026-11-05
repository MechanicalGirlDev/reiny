use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, time::Duration};

use super::{StepOutput, StepRequestError, StepValue};

/// Process-local, monotonically allocated scheduler identity.
///
/// Not globally unique across process restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EpochId(pub(super) u64);

impl EpochId {
    pub(crate) const fn from_owner(value: u64) -> Self {
        Self(value)
    }
    /// Numeric process-local identity.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One logical tick in an epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StepId {
    /// Owning scheduler epoch.
    pub epoch: EpochId,
    /// Zero-based logical tick number.
    pub index: u64,
}

/// A positive logical interval, unrelated to wall-clock deadlines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepTick {
    /// Tick identity.
    pub id: StepId,
    /// Last committed logical time.
    pub start: Duration,
    /// Requested positive logical duration.
    pub dt: Duration,
    /// Checked sum of start and duration.
    pub end: Duration,
}

/// Named values delivered to a single node.
pub type StepInputs = BTreeMap<String, StepValue>;
/// Named values produced by a single node.
pub type StepOutputs = BTreeMap<String, StepValue>;

/// Evidence that every node succeeded and scheduler bookkeeping committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepReceipt {
    /// Committed logical interval.
    pub tick: StepTick,
    /// All validated outputs, addressed by exact instance and port.
    pub outputs: BTreeMap<StepOutput, StepValue>,
}

/// Machine-readable reason an epoch can no longer advance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepFailureKind {
    /// A node returned an application error.
    NodeError,
    /// A node unwound while being invoked, polled, or disposed.
    NodePanicked,
    /// Output names or schemas violated the graph.
    InvalidOutputs,
    /// A host cancelled after dispatch.
    Cancelled,
    /// A host deadline expired after dispatch.
    TimedOut,
    /// The owning host stopped after dispatch.
    HostStopped,
    /// A dispatched worker disappeared.
    WorkerLost,
    /// The advancement future was dropped after dispatch.
    AdvanceDropped,
}

/// Known progress at failure; it does not imply rollback of any component effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepFailure {
    /// Tick that failed to commit.
    pub tick: StepId,
    /// Machine-readable cause.
    pub kind: StepFailureKind,
    /// Node active when the failure occurred, if known.
    pub instance: Option<String>,
    /// Invalid output name, if applicable.
    pub port: Option<String>,
    /// Nodes handed to the executor, in dispatch order.
    pub attempted: Vec<String>,
    /// Nodes whose outputs passed validation, in execution order.
    pub completed: Vec<String>,
    /// Human diagnostic, not a machine-readable contract.
    pub detail: String,
}

impl fmt::Display for StepFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "step {:?} failed ({:?}): {}",
            self.tick, self.kind, self.detail
        )
    }
}

impl std::error::Error for StepFailure {}

/// Rejected request, newly faulted epoch, or an already faulted epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchedulerError {
    /// Rejected without dispatch; the epoch is still usable.
    Request(StepRequestError),
    /// This advancement faulted the epoch.
    Failed(Box<StepFailure>),
    /// A preceding advancement already faulted the epoch.
    Faulted(Box<StepFailure>),
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => error.fmt(f),
            Self::Failed(failure) => failure.fmt(f),
            Self::Faulted(failure) => write!(f, "epoch already faulted: {failure}"),
        }
    }
}

impl std::error::Error for SchedulerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Request(error) => Some(error),
            Self::Failed(failure) | Self::Faulted(failure) => Some(failure.as_ref()),
        }
    }
}

/// Application error from a step callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeError {
    /// Human diagnostic from the component.
    pub message: String,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for NodeError {}

/// One borrowed, sequential step invocation.
pub type StepFuture<'a> = Pin<Box<dyn Future<Output = Result<StepOutputs, NodeError>> + Send + 'a>>;

/// Dispatches explicitly addressed local nodes, one at a time.
pub trait StepExecutor: Send {
    /// Execute one logical interval with exactly the declared input values.
    fn invoke<'a>(
        &'a mut self,
        instance: &'a str,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a>;
}

/// Observable scheduler or host clock phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClockPhase {
    /// Host adapter has not completed startup.
    Starting,
    /// Ready for a request.
    Ready,
    /// A host-owned tick is in flight.
    Running(StepId),
    /// Further advancement is forbidden.
    Faulted(StepFailure),
    /// Host adapter has stopped.
    Stopped,
}

/// Last committed logical clock plus the current phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClockSnapshot {
    /// Process-local run identity.
    pub epoch: EpochId,
    /// Index assigned to the next request.
    pub next_index: u64,
    /// Last successfully committed logical time.
    pub time: Duration,
    /// Current lifecycle phase.
    pub phase: ClockPhase,
}
