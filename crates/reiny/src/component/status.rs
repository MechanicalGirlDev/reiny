use std::collections::BTreeMap;

/// Machine-readable host failure categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HostErrorCode {
    /// A factory identifier is already registered.
    DuplicateFactory,
    /// No instances were supplied.
    EmptyGraph,
    /// Instance namespaces are not unique.
    DuplicateInstance,
    /// An instance selects an unregistered factory.
    UnknownFactory,
    /// Bindings are invalid or unsupported by this slice.
    InvalidBindings,
    /// Domain or deadlines are invalid.
    InvalidOptions,
    /// Launch was called outside a Tokio runtime.
    NoRuntime,
    /// The hosted context could not be opened.
    ContextOpen,
    /// Factory construction failed.
    Factory,
    /// A preparation hook failed.
    Prepare,
    /// A prepared admission ticket could not be obtained.
    Admission,
    /// Compiled reports do not form the declared closed graph.
    Graph,
    /// A prepared ticket could not be committed.
    Activate,
    /// A service run hook failed.
    Run,
    /// A service returned successfully before shutdown.
    UnexpectedExit,
    /// Component code unwound.
    Panic,
    /// An admitted or prepared endpoint was lost.
    ReadinessLost,
    /// A cleanup hook failed.
    StopHook,
    /// A lifecycle deadline expired.
    Timeout,
    /// Startup was cancelled before running.
    Cancelled,
}

/// Contextual failure with an optional structured graph validation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostError {
    /// Stable category.
    pub code: HostErrorCode,
    /// Instance namespace, when the error belongs to one instance.
    pub instance: Option<String>,
    /// Human-readable diagnostic, not a machine-consumed contract.
    pub message: String,
    /// The common validator's error for graph failures.
    pub report: Option<Box<crate::bindings::ReportError>>,
}

impl HostError {
    pub(super) fn new(
        code: HostErrorCode,
        instance: Option<&str>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            instance: instance.map(str::to_owned),
            message: message.into(),
            report: None,
        }
    }
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.instance {
            Some(instance) => write!(f, "{:?} ({instance}): {}", self.code, self.message),
            None => write!(f, "{:?}: {}", self.code, self.message),
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.report
            .as_deref()
            .map(|report| report as &dyn std::error::Error)
    }
}

/// Group lifecycle snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostPhase {
    /// Contexts and component state are being prepared.
    Starting,
    /// Every compiled report passed closed-graph validation.
    Prepared,
    /// Every admission committed; run hooks may execute.
    Running,
    /// Readiness is withdrawn and tasks are being joined.
    Stopping,
    /// Every task joined successfully after a requested stop.
    Stopped,
    /// Every task joined, with a primary or cleanup failure.
    Failed,
    /// The cleanup deadline expired with tasks still unjoined.
    Unresponsive,
}

/// Per-instance lifecycle snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentPhase {
    /// Opening its context or constructing its component.
    Starting,
    /// Running the preparation hook.
    Preparing,
    /// Holding a prepared admission ticket.
    Prepared,
    /// Its readiness has committed.
    Ready,
    /// Its run hook is active.
    Running,
    /// Cancelling its active hook or running cleanup.
    Stopping,
    /// Joined without an instance failure.
    Stopped,
    /// Joined with an instance failure.
    Failed,
    /// Not joined when the group cleanup deadline expired.
    Unresponsive,
}

/// An instance's current phase and first observed failure.
#[derive(Clone, Debug)]
pub struct ComponentStatus {
    /// Current lifecycle phase.
    pub phase: ComponentPhase,
    /// The first failure observed for this instance.
    pub error: Option<HostError>,
}

/// A consistent, retained group snapshot.
#[derive(Clone, Debug)]
pub struct HostStatus {
    /// Group phase.
    pub phase: HostPhase,
    /// Per-instance state indexed by namespace.
    pub components: BTreeMap<String, ComponentStatus>,
}

pub(super) fn phase(
    status: &tokio::sync::watch::Sender<HostStatus>,
    id: &str,
    phase: ComponentPhase,
) {
    status.send_modify(|status| {
        match status.phase {
            HostPhase::Stopped | HostPhase::Failed | HostPhase::Unresponsive => return,
            HostPhase::Starting
            | HostPhase::Prepared
            | HostPhase::Running
            | HostPhase::Stopping => {}
        }
        if let Some(component) = status.components.get_mut(id)
            && component.phase != ComponentPhase::Unresponsive
        {
            component.phase = phase;
        }
    });
}

/// The same terminal result is returned to every waiter.
#[derive(Clone, Debug)]
pub enum HostOutcome {
    /// Every task joined after a requested stop.
    Stopped,
    /// All tasks joined, but startup, service, or cleanup failed.
    Failed {
        /// First failure, retained rather than replaced by cleanup errors.
        primary: HostError,
        /// Additional failures observed during shutdown.
        cleanup: Vec<HostError>,
    },
    /// Some tasks could not be joined by the cleanup deadline.
    Unresponsive {
        /// First startup or service failure, if any.
        primary: Option<HostError>,
        /// Failures from completed cleanup hooks or joins.
        cleanup: Vec<HostError>,
        /// Sorted namespaces whose task completion is unconfirmed.
        instances: Vec<String>,
    },
}
