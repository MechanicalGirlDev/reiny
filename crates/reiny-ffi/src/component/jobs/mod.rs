//! Explicit standalone job ownership, delegated to the core `JobOwner`.
use super::{
    Arc, Bytes, CALLBACK, CLOSED, Duration, Error, INVALID, Mutex, Object, Reservation, Result,
    STATE, abi, boundary, export, failure, insert, lock, object, ordinary, runtime, variant, wait,
    wrong,
};
use abi::output;

mod callback;
mod observation;
mod owner;
#[cfg(test)]
mod tests;
mod work;

pub(super) use work::Work;
pub(super) type Context = reiny::job::JobContext<Vec<u8>, Vec<u8>, Error>;
pub(super) type Handle = reiny::job::JobHandle<Vec<u8>, Vec<u8>, Error>;
pub(super) type Snapshot = reiny::job::JobSnapshot<Vec<u8>, Vec<u8>, Error>;
pub(super) type Outcome = reiny::job::JobOutcome<Vec<u8>, Error>;

pub(super) struct Job {
    core: Handle,
    joined: tokio::sync::watch::Receiver<bool>,
}

pub(super) struct Sink {
    owner: std::thread::ThreadId,
    value: Option<Vec<u8>>,
}

export!(reiny_job_report(context: u64, progress: Bytes, accepted: *mut u32) {
    output(accepted)?;
    let context = object(context)?;
    accepted.write(u32::from(variant!(context, JobContext).report(progress.bytes()?)));
    Ok(())
});

export!(reiny_job_is_cancelled(context: u64, out: *mut u32) {
    output(out)?;
    let context = object(context)?;
    out.write(u32::from(variant!(context, JobContext).is_cancelled()));
    Ok(())
});

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_job_wait_cancelled(context: u64, timeout_ns: u64) -> i32 {
    boundary(|| {
        let context = object(context)?;
        wait(timeout_ns, async {
            variant!(context, JobContext).cancelled().await;
            Ok(())
        })
    })
}

export!(reiny_job_succeed(sink: u64, bytes: Bytes) {
    let value = bytes.bytes()?;
    let sink = object(sink)?;
    let mut sink = lock(variant!(sink, JobSink))?;
    if sink.owner != std::thread::current().id() || sink.value.is_some() {
        return Err(Error::new(STATE, "success sink requires its lane and exactly one value"));
    }
    sink.value = Some(value);
    Ok(())
});
