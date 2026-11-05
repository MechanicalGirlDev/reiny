use super::{codec, protocol::step::MAX_RETAINED_BYTES, step_wire::export_values};
use crate::{
    component::step::{StepHandle, StepRun, StepStartError},
    step::{ClockSnapshot, StepRequest},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Owner admission or request-correlation rejection, before new dispatch.
#[derive(Debug)]
pub enum ProcessStepStartError {
    /// Existing scheduler/gate validation.
    Admission(StepStartError),
    /// Request bytes exceed the process protocol limits.
    Bounds,
    /// Sixty-four accepted records remain unreleased.
    RetentionCapacity,
    /// This ID was previously consumed and its record is no longer retained.
    Released,
    /// A retained ID was reused with different request data or budget.
    RequestConflict,
    /// Sequence identifiers are exhausted.
    SequenceExhausted,
    /// Owner state is unavailable; outcome must not be inferred.
    OwnerUnknown,
}
impl std::fmt::Display for ProcessStepStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ProcessStepStartError {}

struct Record {
    request: StepRequest,
    timeout: Duration,
    run: Arc<StepRun>,
}
struct State {
    high_water: Option<u64>,
    records: BTreeMap<u64, Record>,
    active: Option<Arc<StepRun>>,
}

pub(super) struct Retention {
    pub bootstrap: usize,
    pub previous: std::sync::atomic::AtomicUsize,
    pub available: std::sync::atomic::AtomicUsize,
}

/// Cloneable in-owner client capability with retained request correlation.
///
/// This explicit Rust owner API does not provide a cross-process client socket.
/// Clones share one admission identity and monotonic request sequence.
#[derive(Clone)]
pub struct ProcessStepClient {
    steps: StepHandle,
    state: Arc<Mutex<State>>,
    retention: Arc<Retention>,
}

impl ProcessStepClient {
    pub(super) fn new(steps: StepHandle, retention: Arc<Retention>) -> Self {
        Self {
            steps,
            state: Arc::new(Mutex::new(State {
                high_water: None,
                records: BTreeMap::new(),
                active: None,
            })),
            retention,
        }
    }
    /// Canonical owner clock, never a separate client counter.
    #[must_use]
    pub fn clock(&self) -> ClockSnapshot {
        self.steps.clock()
    }

    /// Allocate and admit one new request. Dropping its returned waiter is inert.
    pub fn try_step(
        &self,
        request: StepRequest,
        timeout: Duration,
    ) -> Result<Arc<StepRun>, ProcessStepStartError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessStepStartError::OwnerUnknown)?;
        let id = match state.high_water {
            Some(id) => id
                .checked_add(1)
                .ok_or(ProcessStepStartError::SequenceExhausted)?,
            None => 0,
        };
        self.admit(&mut state, id, request, timeout)
    }

    /// Admit an explicit session-local request ID, or return its original run.
    /// Exact duplicates never invoke a callback twice.
    pub fn start(
        &self,
        id: u64,
        request: StepRequest,
        timeout: Duration,
    ) -> Result<Arc<StepRun>, ProcessStepStartError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessStepStartError::OwnerUnknown)?;
        self.admit(&mut state, id, request, timeout)
    }

    fn admit(
        &self,
        state: &mut State,
        id: u64,
        request: StepRequest,
        timeout: Duration,
    ) -> Result<Arc<StepRun>, ProcessStepStartError> {
        if let Some(record) = state.records.get(&id) {
            return if record.request == request && record.timeout == timeout {
                Ok(record.run.clone())
            } else {
                Err(ProcessStepStartError::RequestConflict)
            };
        }
        if state.high_water.is_some_and(|high| id <= high) {
            return Err(ProcessStepStartError::Released);
        }
        state.high_water = Some(id);
        if state.records.len() >= 64 {
            return Err(ProcessStepStartError::RetentionCapacity);
        }
        if let Some(active) = &state.active
            && active.retained_result().is_none()
        {
            return Err(ProcessStepStartError::Admission(StepStartError::Busy {
                in_flight: active.id(),
            }));
        }
        state.active = None;
        let mut inputs: BTreeMap<String, crate::step::StepInputs> = BTreeMap::new();
        let mut bytes = 0usize;
        for (input, value) in &request.inputs {
            bytes = bytes
                .checked_add(value.as_encoded().len())
                .ok_or(ProcessStepStartError::Bounds)?;
            if bytes > MAX_RETAINED_BYTES {
                return Err(ProcessStepStartError::Bounds);
            }
            inputs
                .entry(input.instance.clone())
                .or_default()
                .insert(input.port.clone(), value.clone());
        }
        for inputs in inputs.into_values() {
            let wire = export_values(inputs).map_err(|_| ProcessStepStartError::Bounds)?;
            // Reserve a bounded envelope/correlation allowance; no partial send.
            let payload = codec::encode(&wire).map_err(|_| ProcessStepStartError::Bounds)?;
            if payload.len() > super::protocol::MAX_FRAME_BYTES as usize - 65536 {
                return Err(ProcessStepStartError::Bounds);
            }
        }
        let mut retained = self
            .retention
            .bootstrap
            .checked_add(
                self.retention
                    .previous
                    .load(std::sync::atomic::Ordering::Acquire),
            )
            .and_then(|total| {
                bytes
                    .checked_mul(2)
                    .and_then(|bytes| total.checked_add(bytes))
            })
            .ok_or(ProcessStepStartError::Bounds)?;
        for record in state.records.values() {
            for value in record.request.inputs.values() {
                retained = retained
                    .checked_add(value.as_encoded().len())
                    .ok_or(ProcessStepStartError::Bounds)?;
            }
            if let Some(Ok(receipt)) = record.run.retained_result() {
                for value in receipt.outputs.values() {
                    retained = retained
                        .checked_add(value.as_encoded().len())
                        .ok_or(ProcessStepStartError::Bounds)?;
                }
            }
        }
        let available = MAX_RETAINED_BYTES
            .checked_sub(retained)
            .filter(|available| *available > 0)
            .ok_or(ProcessStepStartError::RetentionCapacity)?;
        self.retention
            .available
            .store(available, std::sync::atomic::Ordering::Release);
        let run = Arc::new(
            self.steps
                .try_step(request.clone(), timeout)
                .map_err(ProcessStepStartError::Admission)?,
        );
        state.active = Some(run.clone());
        state.records.insert(
            id,
            Record {
                request,
                timeout,
                run: run.clone(),
            },
        );
        Ok(run)
    }

    /// Observe an accepted request; released/consumed IDs cannot be executed again.
    pub fn observe(&self, id: u64) -> Result<Option<Arc<StepRun>>, ProcessStepStartError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ProcessStepStartError::OwnerUnknown)?;
        if let Some(record) = state.records.get(&id) {
            return Ok(Some(record.run.clone()));
        }
        if state.high_water.is_some_and(|high| id <= high) {
            return Err(ProcessStepStartError::Released);
        }
        Ok(None)
    }

    /// Release one retained owner record. This never requests cancellation.
    pub fn release(&self, id: u64) -> Result<(), ProcessStepStartError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessStepStartError::OwnerUnknown)?;
        if state.records.remove(&id).is_none() {
            return Err(ProcessStepStartError::Released);
        }
        Ok(())
    }
}
