use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::sync::{mpsc, watch};

use super::{
    StepHandle, StepRunId,
    control::Gate,
    driver::{Driver, DriverExecutor},
};
use crate::step::{
    NodeError, StepExecutor, StepFailureKind, StepFuture, StepGraph, StepInputs, StepOutputs,
    StepPlanError, StepScheduler, StepTick,
};

/// Owner-selected correlation and remaining wall-clock budget.
#[derive(Clone, Copy, Debug)]
pub struct StepDispatchContext {
    /// Accepted attempt, distinct from a logical tick.
    pub run: StepRunId,
    /// Canonical scheduler interval.
    pub tick: StepTick,
    /// Zero-based position in the authoritative scheduler order.
    pub ordinal: u32,
    /// One owner deadline shared by every node and commit.
    pub deadline: tokio::time::Instant,
}

/// Structured adapter failure; unknown effects must use `WorkerLost`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchFailure {
    /// Stable scheduler cause, preserved through the driver.
    pub kind: StepFailureKind,
    /// Diagnostic from the failing boundary.
    pub message: String,
}

/// One sequential transport invocation.
pub type ManagedStepFuture<'a> =
    Pin<Box<dyn Future<Output = Result<StepOutputs, DispatchFailure>> + Send + 'a>>;

/// Trusted owner adapter. This interface grants no ordinary application ports.
pub trait ManagedStepExecutor: Send + 'static {
    /// Revoke/close transport ownership after driver termination.
    /// This is separate from the retained tick result.
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
    /// Dispatch exactly one assigned node; never retry after entering this call.
    fn invoke<'a>(
        &'a mut self,
        context: StepDispatchContext,
        instance: &'a str,
        inputs: StepInputs,
    ) -> ManagedStepFuture<'a>;
}

struct Adapter {
    executor: Box<dyn ManagedStepExecutor>,
    run: Option<(StepRunId, tokio::time::Instant)>,
    ordinal: u32,
    failure: Option<StepFailureKind>,
}

impl DriverExecutor for Adapter {
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.executor.shutdown()
    }
    fn begin(&mut self, run: &super::control::Run) {
        self.run = Some((run.id, run.deadline));
        self.ordinal = 0;
        self.failure = None;
    }

    fn failure(&self) -> Option<StepFailureKind> {
        self.failure
    }
}

impl StepExecutor for Adapter {
    fn invoke<'a>(
        &'a mut self,
        instance: &'a str,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let Some((run, deadline)) = self.run else {
                self.failure = Some(StepFailureKind::WorkerLost);
                return Err(NodeError {
                    message: "missing owner attempt".into(),
                });
            };
            let ordinal = self.ordinal;
            let Some(next) = ordinal.checked_add(1) else {
                self.failure = Some(StepFailureKind::WorkerLost);
                return Err(NodeError {
                    message: "node ordinal exhausted".into(),
                });
            };
            self.ordinal = next;
            self.executor
                .invoke(
                    StepDispatchContext {
                        run,
                        tick,
                        ordinal,
                        deadline,
                    },
                    instance,
                    inputs,
                )
                .await
                .map_err(|error| {
                    self.failure = Some(error.kind);
                    NodeError {
                        message: error.message,
                    }
                })
        })
    }
}

/// Owns the existing admission/commit driver independently of observation handles.
///
/// The caller activates only after all remote hosts acknowledge Running.
/// Dropping this owner revokes admission; transport/process joining remains the
/// caller's responsibility, never implied by a terminal step result.
pub struct ManagedStepSession {
    gate: Arc<Gate>,
    terminal: watch::Receiver<bool>,
    driver: Option<tokio::task::JoinHandle<()>>,
}

impl ManagedStepSession {
    /// Compile once and start a dormant driver on the current Tokio runtime.
    pub fn prepare(
        graph: StepGraph,
        initial_time: Duration,
        executor: Box<dyn ManagedStepExecutor>,
    ) -> Result<(Self, StepHandle), StepPlanError> {
        let scheduler = StepScheduler::new(graph, initial_time)?;
        let gate = Gate::new(scheduler.clock());
        let (requests, received) = mpsc::unbounded_channel();
        let handle = StepHandle {
            gate: gate.clone(),
            requests,
            plan: scheduler.plan.clone(),
        };
        let driver = Driver {
            scheduler,
            executor: Box::new(Adapter {
                executor,
                run: None,
                ordinal: 0,
                failure: None,
            }),
            requests: received,
            gate: gate.clone(),
        };
        let (stop, terminal) = watch::channel(false);
        let (control, mut errors) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            driver.run(stop.clone(), control).await;
            // The driver is the only producer; retain no unconsumed abort.
            errors.close();
            stop.send_replace(true);
        });
        Ok((
            Self {
                gate,
                terminal,
                driver: Some(task),
            },
            handle,
        ))
    }

    /// Open admission after the owner's complete staged barrier.
    pub fn activate(&self) {
        self.gate.activate();
    }

    /// Revoke admission and interrupt an accepted attempt at the commit gate.
    pub fn request_stop(&self) {
        self.gate.stop();
    }

    /// Retained driver termination, not callback or process containment.
    #[must_use]
    pub fn subscribe_terminal(&self) -> watch::Receiver<bool> {
        self.terminal.clone()
    }

    /// Stop and join the coordinator driver; does not certify remote cleanup.
    pub async fn stop(mut self) -> Result<(), tokio::task::JoinError> {
        self.request_stop();
        if let Some(driver) = self.driver.take() {
            driver.await?;
        }
        Ok(())
    }
}

impl Drop for ManagedStepSession {
    fn drop(&mut self) {
        self.gate.stop();
    }
}
