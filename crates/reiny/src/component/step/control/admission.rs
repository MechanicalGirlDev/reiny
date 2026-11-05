use std::time::Duration;

use super::{
    Arc, AtomicBool, ClockPhase, ClockSnapshot, Deadline, Gate, OnceLock, Run, StepId, StepRunId,
};
use crate::component::step::StepStartError;
use crate::step::{Plan, StepRequest};

impl Gate {
    pub(in crate::component::step) fn start(
        &self,
        request: &StepRequest,
        timeout: impl Into<Option<Duration>>,
        plan: &Plan,
    ) -> Result<Arc<Run>, StepStartError> {
        let timeout = timeout.into();
        let deadline = Deadline::checked_after(timeout).ok_or(StepStartError::InvalidTimeout)?;
        loop {
            let (clock, ready, stopped, active, sequence) = {
                let state = self.lock().map_err(|_| StepStartError::SupervisorGone)?;
                (
                    state.clock.clone(),
                    state.ready,
                    state.stopped,
                    state.active.as_ref().map(|run| run.id),
                    state.sequence,
                )
            };
            if let ClockPhase::Faulted(failure) = &clock.phase {
                return Err(StepStartError::Faulted(Box::new(failure.clone())));
            }
            if stopped {
                return Err(StepStartError::Stopped);
            }
            if !ready {
                return Err(StepStartError::NotRunning);
            }
            if let Some(in_flight) = active {
                return Err(StepStartError::Busy { in_flight });
            }
            request
                .clone()
                .validate(plan, &clock)
                .map_err(StepStartError::InvalidRequest)?;
            let next_sequence = sequence
                .checked_add(1)
                .ok_or(StepStartError::RunIdExhausted)?;
            let tick = StepId {
                epoch: clock.epoch,
                index: clock.next_index,
            };
            let mut run = Arc::new(Run {
                id: StepRunId {
                    epoch: tick.epoch,
                    sequence,
                },
                tick,
                deadline,
                cancelled: AtomicBool::new(false),
                result: OnceLock::new(),
            });
            let running = Arc::new(ClockSnapshot {
                phase: ClockPhase::Running(tick),
                ..(*clock).clone()
            });
            let mut state = self.lock().map_err(|_| StepStartError::SupervisorGone)?;
            if state.stopped {
                return Err(StepStartError::Stopped);
            }
            if let Some(active) = &state.active {
                return Err(StepStartError::Busy {
                    in_flight: active.id,
                });
            }
            if state.sequence != sequence || !Arc::ptr_eq(&state.clock, &clock) {
                // A complete intervening tick requires revalidating arithmetic,
                // not waiting for an occupied slot.
                drop(state);
                continue;
            }
            // The Arc is still private. Only its primitive deadline changes at
            // acceptance; allocation and logical validation happened outside.
            Arc::get_mut(&mut run)
                .ok_or(StepStartError::SupervisorGone)?
                .deadline =
                Deadline::checked_after(timeout).ok_or(StepStartError::InvalidTimeout)?;
            let retired = std::mem::replace(&mut state.clock, running);
            state.sequence = next_sequence;
            state.active = Some(run.clone());
            drop(state);
            drop(retired);
            self.publish();
            return Ok(run);
        }
    }
}
