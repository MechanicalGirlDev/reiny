use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use super::{
    StepRunError,
    control::{ControlledCommit, Gate, Run},
    worker::Executor,
};
use crate::{
    component::{HostError, HostErrorCode, staged},
    step::{ClockPhase, SchedulerError, StepFailure, StepFailureKind, StepRequest, StepScheduler},
};

pub(super) struct Request {
    pub run: Arc<Run>,
    pub request: StepRequest,
}

pub(in crate::component) struct Driver {
    pub(super) scheduler: StepScheduler,
    pub(super) executor: Executor,
    pub(super) requests: mpsc::UnboundedReceiver<Request>,
    pub gate: Arc<Gate>,
}

struct Lifetime {
    gate: Arc<Gate>,
    stop: watch::Sender<bool>,
    completed: bool,
}

impl Drop for Lifetime {
    fn drop(&mut self) {
        if !self.completed {
            self.gate.disappear();
            self.gate.publish();
            self.stop.send_replace(true);
        }
    }
}

impl Driver {
    pub(in crate::component) fn run(
        self,
        stop: watch::Sender<bool>,
        control: mpsc::Sender<staged::Request>,
    ) -> impl std::future::Future<Output = ()> + Send {
        // Construct the guard before creating the future: aborting an unpolled
        // task must also publish unknown effects and request group shutdown.
        let mut lifetime = Lifetime {
            gate: self.gate.clone(),
            stop,
            completed: false,
        };
        async move {
            self.serve(control).await;
            lifetime.completed = !lifetime.gate.gone();
            drop(lifetime);
        }
    }

    async fn serve(mut self, control: mpsc::Sender<staged::Request>) {
        loop {
            let request = tokio::select! {
                biased;
                () = self.gate.stopped() => {
                    self.gate.stop();
                    self.requests.close();
                    // Accepted but not yet dispatched requests still receive a
                    // retained terminal result, rather than disappearing.
                    match self.requests.try_recv() {
                        Ok(request) => request,
                        Err(_) => return,
                    }
                }
                request = self.requests.recv() => if let Some(request) = request {
                    request
                } else {
                        // Dropping all StepHandles is observation abandonment,
                        // not a host stop request.
                        self.gate.stopped().await;
                        return;
                }
            };
            if let Some(failure) = self.advance(request).await {
                self.gate.stop();
                if failure.kind != StepFailureKind::HostStopped {
                    let error = HostError::new(
                        HostErrorCode::Step,
                        failure.instance.as_deref(),
                        failure.to_string(),
                    );
                    let _ = control.send(staged::Request::Abort(error)).await;
                }
                return;
            }
        }
    }

    async fn advance(&mut self, request: Request) -> Option<StepFailure> {
        let Request { run, request } = request;
        self.executor.failure = None;
        let control = ControlledCommit {
            gate: &self.gate,
            run: &run,
        };
        let result = {
            let advancement =
                self.scheduler
                    .advance_controlled(&mut self.executor, request, &control);
            tokio::pin!(advancement);
            tokio::select! {
                biased;
                kind = self.gate.interrupted(&run) => Err(kind),
                result = &mut advancement => Ok(result),
            }
        };
        let failure = match result {
            Ok(Ok(_)) => return None, // Gate already retained the success.
            Ok(Err(SchedulerError::Failed(failure) | SchedulerError::Faulted(failure))) => {
                let kind = self.executor.failure.unwrap_or(failure.kind);
                self.scheduler.interrupted(kind).unwrap_or(*failure)
            }
            Ok(Err(SchedulerError::Request(_))) => {
                // Synchronous validation used the same immutable plan and clock.
                self.gate.disappear();
                return Some(unknown(&run));
            }
            Err(kind) => {
                if let Some(failure) = self.scheduler.interrupted(kind) {
                    failure
                } else {
                    let result = match kind {
                        StepFailureKind::Cancelled => {
                            StepRunError::CancelledBeforeDispatch { tick: run.tick }
                        }
                        StepFailureKind::TimedOut => {
                            StepRunError::TimedOutBeforeDispatch { tick: run.tick }
                        }
                        _ => StepRunError::Failed(Box::new(StepFailure {
                            kind,
                            ..unknown(&run)
                        })),
                    };
                    self.gate.finish(&run, Err(result), self.scheduler.clock());
                    return None;
                }
            }
        };
        let mut clock = self.scheduler.clock();
        clock.phase = ClockPhase::Faulted(failure.clone());
        self.gate.finish(
            &run,
            Err(StepRunError::Failed(Box::new(failure.clone()))),
            clock,
        );
        Some(failure)
    }
}

fn unknown(run: &Run) -> StepFailure {
    StepFailure {
        tick: run.tick,
        kind: StepFailureKind::WorkerLost,
        instance: None,
        port: None,
        attempted: Vec::new(),
        completed: Vec::new(),
        detail: "step supervisor unavailable; effects unknown".into(),
    }
}
