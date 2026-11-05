//! Local managed step components and cancellation-safe run observation.
//!
//! Step routes grant only step-value access, independently of stream/RPC bindings.
//! A receipt commits scheduler bookkeeping, never arbitrary component effects.
//! Unwind panics discard the component without calling its stop hook; aborting
//! panics still terminate the process.

use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};

use super::{ComponentContext, ComponentFuture, HostError, StopReason};
use crate::step::{
    ClockSnapshot, EpochId, StepFailure, StepFuture, StepId, StepInputs, StepPlanError,
    StepReceipt, StepRequest, StepRequestError, StepTick,
};

pub(crate) mod control;
pub(super) mod driver;
mod launch;
mod session;
pub(in crate::component) mod worker;

pub use session::{
    DispatchFailure, ManagedStepExecutor, ManagedStepFuture, ManagedStepSession,
    StepDispatchContext,
};

#[cfg(test)]
mod tests;

/// A component invoked sequentially at explicit logical intervals.
pub trait StepComponent: Send + 'static {
    /// Open and retain all ordinary declared ports before managed admission.
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a>;
    /// Snapshot the actual prepared object, without advancing its state.
    fn prepared_outputs(&self) -> Result<crate::step::StepOutputs, crate::step::NodeError> {
        Ok(std::collections::BTreeMap::new())
    }
    /// Application-owned configuration-dependent admission evidence.
    fn prepared_contract(&self) -> Result<Vec<u8>, crate::step::NodeError> {
        Ok(Vec::new())
    }
    /// Compute exactly the outputs declared by the immutable step schema.
    fn step<'a>(
        &'a mut self,
        ctx: &'a ComponentContext,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a>;
    /// Clean up after cancellation, including partially prepared state.
    fn stop<'a>(&'a mut self, ctx: &'a ComponentContext, reason: StopReason)
    -> ComponentFuture<'a>;
}

/// Constructs independent step components without communication or detached work.
pub trait StepComponentFactory: Send + Sync + 'static {
    /// Construct one instance from its explicit settings.
    fn create(&self, config: Option<&toml::Table>) -> crate::Result<Box<dyn StepComponent>>;
}

/// An accepted attempt; retries before dispatch retain a tick but get a new sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepRunId {
    /// Scheduler identity.
    pub epoch: EpochId,
    /// Monotonically increasing attempt number.
    pub sequence: u64,
}

/// Synchronous launch rejection, before any component construction.
#[derive(Debug)]
pub enum StepLaunchError {
    /// Existing host validation failed.
    Host(HostError),
    /// Step graph compilation failed.
    Plan(StepPlanError),
    /// Only an actual in-process Local engine is supported.
    UnsupportedEngine,
    /// Graph nodes and stepped instances differ.
    NodeSetMismatch {
        /// Offending namespace.
        instance: String,
    },
    /// A service instance was selected as a step node.
    WrongFactoryKind {
        /// Offending namespace.
        instance: String,
    },
    /// The graph does not match the registered immutable schema.
    SchemaMismatch {
        /// Offending namespace.
        instance: String,
    },
}

/// A request rejected synchronously without admission to the one-in-flight slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepStartError {
    /// Host preparation has not completed.
    NotRunning,
    /// Another attempt owns the slot; requests are never queued behind it.
    Busy {
        /// Current attempt.
        in_flight: StepRunId,
    },
    /// Input names, schemas, duration or logical arithmetic are invalid.
    InvalidRequest(StepRequestError),
    /// Wall-clock budget is zero, unrepresentable, or absent on a session handle.
    InvalidTimeout,
    /// Per-epoch attempt numbers are exhausted.
    RunIdExhausted,
    /// A preceding dispatch permanently faulted the epoch.
    Faulted(Box<StepFailure>),
    /// Host shutdown has started.
    Stopped,
    /// Supervisor disappeared; effects are unknown.
    SupervisorGone,
}

/// A retained terminal attempt result, independent of host cleanup completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepRunError {
    /// No node was dispatched; the epoch remains usable.
    CancelledBeforeDispatch {
        /// Logical tick.
        tick: StepId,
    },
    /// No node was dispatched before the wall-clock deadline.
    TimedOutBeforeDispatch {
        /// Logical tick.
        tick: StepId,
    },
    /// The epoch faulted; effects are not rolled back.
    Failed(Box<StepFailure>),
    /// Supervisor disappeared; do not infer whether any node ran.
    SupervisorGone {
        /// Logical tick with unknown effects.
        tick: StepId,
    },
}

/// Cancellation acknowledgement, not completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelStatus {
    /// Cancellation won the gate before commit.
    Requested,
    /// This attempt already has a cancellation request.
    AlreadyRequested,
    /// A terminal result already won the gate.
    Finished,
    /// The supervisor disappeared; effects are unknown.
    SupervisorGone,
}

macro_rules! error_display {
    ($($name:ty),+ $(,)?) => {$(
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{self:?}")
            }
        }
        impl std::error::Error for $name {}
    )+};
}
error_display!(StepLaunchError, StepStartError, StepRunError);

/// Cloneable admission capability for one host-owned scheduler.
#[derive(Clone)]
pub struct StepHandle {
    gate: Arc<control::Gate>,
    requests: mpsc::UnboundedSender<driver::Request>,
    plan: Arc<crate::step::Plan>,
    local: bool,
}

impl StepHandle {
    /// Read the canonical clock, including a stop not yet delivered to the driver.
    #[must_use]
    pub fn clock(&self) -> ClockSnapshot {
        self.gate.clock()
    }

    /// Subscribe to retained clock changes; intermediate phases may coalesce.
    #[must_use]
    pub fn subscribe_clock(&self) -> watch::Receiver<ClockSnapshot> {
        self.gate.subscribe()
    }

    /// Validate and synchronously reserve the global slot, without waiting.
    pub fn try_step(
        &self,
        request: StepRequest,
        timeout: Duration,
    ) -> Result<StepRun, StepStartError> {
        self.try_step_with_timeout(request, Some(timeout))
    }

    /// Reserve a local attempt with an optional wall-clock budget.
    ///
    /// `None` installs no deadline at admission, dispatch or commit. Cancellation
    /// and host stop still use the same gate. Handles from `ManagedStepSession`
    /// require a finite budget and reject `None` with `InvalidTimeout`.
    pub fn try_step_with_timeout(
        &self,
        request: StepRequest,
        timeout: Option<Duration>,
    ) -> Result<StepRun, StepStartError> {
        if timeout.is_none() && !self.local {
            return Err(StepStartError::InvalidTimeout);
        }
        let run = self.gate.start(&request, timeout, &self.plan)?;
        if self
            .requests
            .send(driver::Request {
                run: run.clone(),
                request,
            })
            .is_err()
        {
            self.gate.disappear();
            return Err(StepStartError::SupervisorGone);
        }
        Ok(StepRun {
            gate: self.gate.clone(),
            run,
        })
    }
}

/// Repeatable observation and explicit cancellation of an accepted attempt.
///
/// Dropping this object or a `wait` future does not cancel execution.
pub struct StepRun {
    gate: Arc<control::Gate>,
    run: Arc<control::Run>,
}

impl StepRun {
    pub(in crate::component) fn retained_result(
        &self,
    ) -> Option<Result<Arc<StepReceipt>, StepRunError>> {
        self.run.result.get().cloned().or_else(|| {
            self.gate
                .gone()
                .then_some(Err(StepRunError::SupervisorGone {
                    tick: self.run.tick,
                }))
        })
    }
    /// Distinct acceptance identity, even for a pre-dispatch retry.
    #[must_use]
    pub fn id(&self) -> StepRunId {
        self.run.id
    }

    /// Logical tick requested by this attempt.
    #[must_use]
    pub fn tick(&self) -> StepId {
        self.run.tick
    }

    /// Serialize a cancellation request against bookkeeping commit.
    #[must_use]
    pub fn cancel(&self) -> CancelStatus {
        self.gate.cancel(&self.run)
    }

    /// Await the retained result. This does not wait for host cleanup.
    pub async fn wait(&self) -> Result<Arc<StepReceipt>, StepRunError> {
        loop {
            let notified = self.gate.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.run.result.get() {
                return result.clone();
            }
            if self.gate.gone() {
                return Err(StepRunError::SupervisorGone {
                    tick: self.run.tick,
                });
            }
            notified.await;
        }
    }
}
