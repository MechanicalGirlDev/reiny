use super::{
    Arc, Bytes, Duration, Error, INVALID, Object, Reservation, STATE, Tick, Time, boundary, export,
    failure, insert, object, output, variant, wait, wrong,
};
use reiny::component::{CancelStatus, StepRunError, StepStartError};
use reiny::step::{ClockPhase, StepFailure, StepFailureKind, StepOutput, StepReceipt};
use serde_json::json;

#[repr(C)]
pub(crate) struct Clock {
    epoch: u64,
    next_index: u64,
    time: Time,
    phase: u32,
}

type Outcome = std::result::Result<Arc<StepReceipt>, StepRunError>;

fn kind(result: &Outcome) -> u32 {
    match result {
        Ok(_) => 0,
        Err(StepRunError::CancelledBeforeDispatch { .. }) => 1,
        Err(StepRunError::TimedOutBeforeDispatch { .. }) => 2,
        Err(StepRunError::Failed(_)) => 3,
        Err(StepRunError::SupervisorGone { .. }) => 4,
    }
}

fn failure_info(failure: &StepFailure) -> serde_json::Value {
    let kind = match failure.kind {
        StepFailureKind::NodeError => 1,
        StepFailureKind::NodePanicked => 2,
        StepFailureKind::InvalidOutputs => 3,
        StepFailureKind::Cancelled => 4,
        StepFailureKind::TimedOut => 5,
        StepFailureKind::HostStopped => 6,
        StepFailureKind::WorkerLost => 7,
        StepFailureKind::AdvanceDropped => 8,
    };
    json!({"kind":kind, "instance":failure.instance, "port":failure.port,
        "attempted":failure.attempted, "completed":failure.completed, "detail":failure.detail})
}

export!(reiny_step_clock_read(handle: u64, out: *mut Clock) {
    output(out)?;
    let object = object(handle)?;
    let clock = variant!(object, StepHandle).clock();
    let phase = match clock.phase {
        ClockPhase::Starting => 1, ClockPhase::Ready => 2, ClockPhase::Running(_) => 3,
        ClockPhase::Faulted(_) => 4, ClockPhase::Stopped => 5,
    };
    out.write(Clock { epoch: clock.epoch.get(), next_index: clock.next_index, time: clock.time.into(), phase });
    Ok(())
});

export!(reiny_step_try(handle: u64, request: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    let handle = variant!(object, StepHandle);
    let request = super::object(request)?;
    let request = variant!(request, StepRequest).clone();
    let slot = Reservation::new()?;
    let run = handle.try_step(request, Duration::from_nanos(timeout_ns)).map_err(|error| {
        let code = match &error {
            StepStartError::NotRunning => -101, StepStartError::Busy { .. } => -102,
            StepStartError::InvalidRequest(_) => -103, StepStartError::InvalidTimeout => -104,
            StepStartError::RunIdExhausted => -105, StepStartError::Faulted(_) => -106,
            StepStartError::Stopped => -107, StepStartError::SupervisorGone => -108,
        };
        Error::new(code, error.to_string())
    })?;
    out.write(slot.publish(Object::StepRun(run))?);
    Ok(())
});

export!(reiny_step_run_id(handle: u64, epoch: *mut u64, sequence: *mut u64, index: *mut u64) {
    output(epoch)?;
    output(sequence)?;
    output(index)?;
    let object = object(handle)?;
    let run = variant!(object, StepRun);
    epoch.write(run.id().epoch.get());
    sequence.write(run.id().sequence);
    index.write(run.tick().index);
    Ok(())
});

export!(reiny_step_cancel(handle: u64, out: *mut u32) {
    output(out)?;
    let object = object(handle)?;
    out.write(match variant!(object, StepRun).cancel() {
        CancelStatus::Requested => 1, CancelStatus::AlreadyRequested => 2,
        CancelStatus::Finished => 3, CancelStatus::SupervisorGone => 4,
    });
    Ok(())
});

export!(reiny_step_wait(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    let run = variant!(object, StepRun);
    let slot = Reservation::new()?;
    let result = wait(timeout_ns, async { Ok(run.wait().await) })?;
    out.write(slot.publish(Object::StepResult(result))?);
    Ok(())
});

export!(reiny_step_result_kind(handle: u64, out: *mut u32) {
    output(out)?;
    let object = object(handle)?;
    out.write(kind(variant!(object, StepResult)));
    Ok(())
});

export!(reiny_step_result_tick(handle: u64, out: *mut Tick) {
    output(out)?;
    let object = object(handle)?;
    let receipt = variant!(object, StepResult).as_ref().map_err(|_| Error::new(STATE, "attempt did not commit"))?;
    out.write(receipt.tick.into());
    Ok(())
});

export!(reiny_step_result_output(handle: u64, instance: Bytes, port: Bytes, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    let receipt = variant!(object, StepResult).as_ref().map_err(|_| Error::new(STATE, "attempt did not commit"))?;
    let key = StepOutput { instance: instance.text()?, port: port.text()? };
    let value = receipt.outputs.get(&key).ok_or_else(|| Error::new(INVALID, "unknown step output"))?;
    out.write(insert(Object::StepValue(value.clone()), false)?);
    Ok(())
});

export!(reiny_step_result_info(handle: u64, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    let result = variant!(object, StepResult);
    let (tick, diagnostic) = match result {
        Ok(receipt) => (receipt.tick.id, serde_json::Value::Null),
        Err(StepRunError::CancelledBeforeDispatch { tick }
            | StepRunError::TimedOutBeforeDispatch { tick }
            | StepRunError::SupervisorGone { tick }) => (*tick, serde_json::Value::Null),
        Err(StepRunError::Failed(failure)) => (failure.tick, failure_info(failure)),
    };
    let json = json!({"version":1, "kind":kind(result), "epoch":tick.epoch.get(), "index":tick.index, "failure":diagnostic});
    out.write(insert(Object::Buffer(serde_json::to_vec(&json).map_err(failure)?), false)?);
    Ok(())
});
