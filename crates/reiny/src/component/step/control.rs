use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{Notify, watch};

use super::{CancelStatus, StepRunError, StepRunId};
use crate::component::deadline::Deadline;
use crate::step::{ClockPhase, ClockSnapshot, Commit, StepFailureKind, StepId, StepReceipt};

mod admission;

#[cfg(test)]
use super::StepStartError;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
#[path = "tests/gate.rs"]
mod tests;

pub(super) type RunResult = Result<Arc<StepReceipt>, StepRunError>;

pub(super) struct Run {
    pub id: StepRunId,
    pub tick: StepId,
    pub deadline: Deadline,
    cancelled: AtomicBool,
    pub result: OnceLock<RunResult>,
}

struct State {
    ready: bool,
    stopped: bool,
    sequence: u64,
    clock: Arc<ClockSnapshot>,
    active: Option<Arc<Run>>,
}

/// One ordering point for host stop, cancellation, admission and scheduler commit.
///
/// No notifications, user code, allocation or disposal occur while `state` is held.
/// Watch publication samples the canonical state in its own update closure, then
/// releases both locks before waking observers.
pub(crate) struct Gate {
    state: Mutex<State>,
    clock_tx: watch::Sender<ClockSnapshot>,
    lost: AtomicBool,
    pub(super) changed: Notify,
}

impl Gate {
    pub(super) fn new(mut clock: ClockSnapshot) -> Arc<Self> {
        clock.phase = ClockPhase::Starting;
        let (clock_tx, _) = watch::channel(clock.clone());
        Arc::new(Self {
            state: Mutex::new(State {
                ready: false,
                stopped: false,
                sequence: 0,
                clock: Arc::new(clock),
                active: None,
            }),
            clock_tx,
            lost: AtomicBool::new(false),
            changed: Notify::new(),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, StepFailureKind> {
        match self.state.lock() {
            Ok(state) if !self.gone() => Ok(state),
            Ok(state) => {
                drop(state);
                Err(StepFailureKind::WorkerLost)
            }
            Err(error) => {
                // Never clear poison or resume a partially applied transaction.
                drop(error.into_inner());
                self.disappear();
                Err(StepFailureKind::WorkerLost)
            }
        }
    }

    pub(super) fn gone(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    pub(super) fn disappear(&self) {
        self.lost.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub(crate) fn stop(&self) {
        self.mark_stop();
        self.changed.notify_waiters();
        self.publish();
    }

    /// Event sources call this before publishing their retained stop/loss signal.
    /// Their ordinary worker observer owns wakeups and group cleanup.
    pub(crate) fn mark_stop(&self) {
        if let Ok(mut state) = self.lock() {
            state.stopped = true;
        }
    }

    pub(crate) fn activate(&self) {
        if let Ok(mut state) = self.lock()
            && !state.stopped
        {
            state.ready = true;
        }
        self.publish();
    }

    pub(super) fn clock(&self) -> ClockSnapshot {
        let snapshot = self.snapshot();
        if self.gone() {
            self.changed.notify_waiters();
        }
        snapshot
    }

    fn snapshot(&self) -> ClockSnapshot {
        let (clock, ready, stopped) = match self.state.lock() {
            Ok(state) => (state.clock.clone(), state.ready, state.stopped),
            Err(error) => {
                let state = error.into_inner();
                let clock = state.clock.clone();
                drop(state);
                self.lost.store(true, Ordering::Release);
                (clock, false, true)
            }
        };
        let mut clock = (*clock).clone();
        if !matches!(clock.phase, ClockPhase::Faulted(_)) {
            if stopped || self.gone() {
                clock.phase = ClockPhase::Stopped;
            } else if ready && clock.phase == ClockPhase::Starting {
                clock.phase = ClockPhase::Ready;
            }
        }
        clock
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<ClockSnapshot> {
        self.publish();
        self.clock_tx.subscribe()
    }

    pub(super) fn publish(&self) {
        // Serializing the sample with watch replacement prevents an older
        // publisher from overwriting a newer phase. No adapter lock survives
        // into watch's notification, including when a waker reenters this gate.
        self.clock_tx.send_if_modified(|snapshot| {
            let latest = self.snapshot();
            if *snapshot == latest {
                false
            } else {
                *snapshot = latest;
                true
            }
        });
        if self.gone() {
            self.changed.notify_waiters();
        }
    }

    pub(super) fn cancel(&self, run: &Run) -> CancelStatus {
        let state = match self.lock() {
            Ok(state) => state,
            Err(_) if run.result.get().is_some() => return CancelStatus::Finished,
            Err(_) => return CancelStatus::SupervisorGone,
        };
        let status = if run.result.get().is_some() {
            CancelStatus::Finished
        } else if run.cancelled.swap(true, Ordering::Relaxed) {
            CancelStatus::AlreadyRequested
        } else {
            CancelStatus::Requested
        };
        drop(state);
        self.changed.notify_waiters();
        status
    }

    fn reason(state: &State, run: &Run) -> Option<StepFailureKind> {
        if state.stopped {
            Some(StepFailureKind::HostStopped)
        } else if run.cancelled.load(Ordering::Relaxed) {
            Some(StepFailureKind::Cancelled)
        } else if run.deadline.expired() {
            Some(StepFailureKind::TimedOut)
        } else {
            None
        }
    }

    pub(super) fn interruption(&self, run: &Run) -> Option<StepFailureKind> {
        match self.lock() {
            Ok(state) => Self::reason(&state, run),
            Err(kind) => Some(kind),
        }
    }

    pub(super) async fn interrupted(&self, run: &Run) -> StepFailureKind {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(kind) = self.interruption(run) {
                return kind;
            }
            tokio::select! {
                () = notified => {}
                () = run.deadline.elapsed() => {}
            }
        }
    }

    pub(super) async fn stopped(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.lock().map_or(true, |state| state.stopped) {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn finish(&self, run: &Run, result: RunResult, clock: ClockSnapshot) {
        let clock = Arc::new(clock);
        let Ok(mut state) = self.lock() else {
            return;
        };
        let retired_clock = std::mem::replace(&mut state.clock, clock);
        let retired_run = state.active.take();
        let duplicate = run.result.set(result);
        drop(state);
        drop((retired_clock, retired_run, duplicate));
        self.changed.notify_waiters();
        self.publish();
    }
}

pub(super) struct ControlledCommit<'a> {
    pub gate: &'a Gate,
    pub run: &'a Run,
}

impl Commit for ControlledCommit<'_> {
    fn commit<T>(
        &self,
        receipt: &Arc<StepReceipt>,
        clock: &Arc<ClockSnapshot>,
        apply: impl FnOnce() -> T,
    ) -> Result<T, StepFailureKind> {
        let success = Ok(receipt.clone());
        let ready = clock.clone();
        let mut state = self.gate.lock()?;
        // The monotonic deadline is checked at the actual linearization point.
        if let Some(kind) = Gate::reason(&state, self.run) {
            return Err(kind);
        }
        let retired = apply();
        let retired_clock = std::mem::replace(&mut state.clock, ready);
        let retired_run = state.active.take();
        let duplicate = self.run.result.set(success);
        drop(state);
        drop((retired_clock, retired_run, duplicate));
        self.gate.changed.notify_waiters();
        self.gate.publish();
        Ok(retired)
    }
}
