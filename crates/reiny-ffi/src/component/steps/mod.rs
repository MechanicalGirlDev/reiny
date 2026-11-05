//! Additive immutable encoded values and managed Local step capabilities.
use super::{
    Arc, Bytes, Duration, Error, Factory, FactoryV1, Host, INVALID, Lanes, Object, Reservation,
    Result, SCHEMA, STATE, UNSUPPORTED, abi, boundary, budget, callbacks, export, failure, insert,
    lock, object, ordinary, runtime, variant, wait, wrong,
};
use abi::output;
use reiny::step::{StepOutputs, StepSchema, StepTick};
use serde::Deserialize;

mod import;
mod launch;
mod results;

pub(super) struct Sink {
    pub(super) owner: std::thread::ThreadId,
    pub(super) outputs: StepOutputs,
}

pub(super) struct Registration {
    pub(super) callback: StepCallback,
    pub(super) schema: StepSchema,
}

type StepCallback =
    unsafe extern "C" fn(*mut std::ffi::c_void, u64, *const Tick, u64, u64, u64) -> i32;

#[repr(C)]
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Time {
    secs: u64,
    nanos: u32,
}

impl Time {
    fn duration(self) -> Result<Duration> {
        if self.nanos >= 1_000_000_000 {
            return Err(Error::new(INVALID, "nanoseconds must be normalized"));
        }
        Ok(Duration::new(self.secs, self.nanos))
    }
}

impl From<Duration> for Time {
    fn from(time: Duration) -> Self {
        Self {
            secs: time.as_secs(),
            nanos: time.subsec_nanos(),
        }
    }
}

#[repr(C)]
pub(crate) struct Tick {
    epoch: u64,
    index: u64,
    start: Time,
    dt: Time,
    end: Time,
}

impl From<StepTick> for Tick {
    fn from(tick: StepTick) -> Self {
        Self {
            epoch: tick.id.epoch.get(),
            index: tick.id.index,
            start: tick.start.into(),
            dt: tick.dt.into(),
            end: tick.end.into(),
        }
    }
}

export!(reiny_step_value_new(schema: u64, bytes: Bytes, out: *mut u64) {
    output(out)?;
    let ty = object(schema)?;
    let ty = reiny::step::StepType::from_encoded_type(variant!(ty, Schema).clone()).map_err(failure)?;
    out.write(insert(Object::StepValue(reiny::step::StepValue::from_encoded(ty, bytes.bytes()?)), false)?);
    Ok(())
});

export!(reiny_step_value_copy(value: u64, schema: u64, dest: *mut u8, capacity: usize, required: *mut usize) {
    let value = object(value)?;
    let value = variant!(value, StepValue);
    let schema = object(schema)?;
    let ty = reiny::step::StepType::from_encoded_type(variant!(schema, Schema).clone()).map_err(failure)?;
    if value.step_type().type_name() != ty.type_name() || value.step_type().fingerprint() != ty.fingerprint() {
        return Err(Error::new(SCHEMA, "step value schema mismatch"));
    }
    abi::copy_out(value.as_encoded(), dest, capacity, required)
});

export!(reiny_step_value_new_with_semantics(schema: u64, semantics: Bytes, bytes: Bytes, out: *mut u64) {
    output(out)?;
    let semantics = reiny::component::process::decode_payload(&semantics.bytes()?)
        .map_err(|error| Error::new(INVALID, error.to_string()))?;
    let schema = object(schema)?;
    let ty = reiny::step::StepType::from_encoded_type(variant!(schema, Schema).clone())
        .map_err(failure)?.with_semantics(semantics).map_err(failure)?;
    out.write(insert(Object::StepValue(reiny::step::StepValue::from_encoded(ty, bytes.bytes()?)), false)?);
    Ok(())
});

export!(reiny_step_value_semantics(value: u64, out: *mut u64) {
    output(out)?;
    let value = object(value)?;
    let semantics = variant!(value, StepValue).step_type().semantics();
    out.write(insert(Object::Buffer(serde_json::to_vec(semantics).map_err(failure)?), false)?);
    Ok(())
});

export!(reiny_step_input_get(inputs: u64, port: Bytes, out: *mut u64) {
    output(out)?;
    let inputs = object(inputs)?;
    let value = variant!(inputs, StepInputs).get(&port.text()?)
        .ok_or_else(|| Error::new(INVALID, "unknown step input"))?;
    out.write(insert(Object::StepValue(value.clone()), false)?);
    Ok(())
});

export!(reiny_step_output_set(sink: u64, port: Bytes, value: u64) {
    let port = port.text()?;
    let value = object(value)?;
    let value = variant!(value, StepValue).clone();
    let sink = object(sink)?;
    let mut sink = lock(variant!(sink, StepSink))?;
    if sink.owner != std::thread::current().id() {
        return Err(Error::new(STATE, "step outputs require the callback lane"));
    }
    if sink.outputs.contains_key(&port) {
        return Err(Error::new(STATE, "duplicate step output"));
    }
    sink.outputs.insert(port, value);
    Ok(())
});
