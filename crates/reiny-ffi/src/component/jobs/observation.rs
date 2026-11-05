use super::{
    Arc, CLOSED, Error, Object, Outcome, Reservation, STATE, boundary, export, insert, object,
    output, variant, wait, wrong,
};
use reiny::job::{CancelRequest, JobStatus};

#[repr(C)]
pub(crate) struct SnapshotInfo {
    id: u64,
    revision: u64,
    phase: u32,
    cancellation_requested: u32,
    has_progress: u32,
}

export!(reiny_job_cancel(job: u64, out: *mut u32) {
    output(out)?;
    let job = object(job)?;
    out.write(match variant!(job, Job).core.cancel() {
        CancelRequest::Requested => 1, CancelRequest::AlreadyRequested => 2,
        CancelRequest::Finished => 3,
    });
    Ok(())
});

export!(reiny_job_wait(job: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let job = object(job)?;
    let job = variant!(job, Job);
    let reservation = Reservation::new()?;
    let result = wait(timeout_ns, async { Ok(job.core.wait().await) })?;
    out.write(reservation.publish(Object::JobResult(result))?);
    Ok(())
});

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_job_wait_joined(job: u64, timeout_ns: u64) -> i32 {
    boundary(|| {
        let job = object(job)?;
        let mut joined = variant!(job, Job).joined.clone();
        wait(timeout_ns, async {
            loop {
                if *joined.borrow_and_update() {
                    return Ok(());
                }
                joined
                    .changed()
                    .await
                    .map_err(|_| Error::new(CLOSED, "native join was not certified"))?;
            }
        })
    })
}

export!(reiny_job_snapshot(job: u64, out: *mut u64) {
    output(out)?;
    let job = object(job)?;
    out.write(insert(Object::JobSnapshot(variant!(job, Job).core.snapshot()), false)?);
    Ok(())
});

export!(reiny_job_subscribe(job: u64, out: *mut u64) {
    output(out)?;
    let job = object(job)?;
    out.write(insert(Object::JobObserver(tokio::sync::Mutex::new(variant!(job, Job).core.subscribe())), false)?);
    Ok(())
});

export!(reiny_job_changed(observer: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let observer = object(observer)?;
    let observer = variant!(observer, JobObserver);
    let reservation = Reservation::new()?;
    let snapshot = wait(timeout_ns, async { Ok(observer.lock().await.changed().await) })?;
    out.write(reservation.publish(Object::JobSnapshot(snapshot))?);
    Ok(())
});

export!(reiny_job_snapshot_read(snapshot: u64, out: *mut SnapshotInfo) {
    output(out)?;
    let snapshot = object(snapshot)?;
    let snapshot = variant!(snapshot, JobSnapshot);
    out.write(SnapshotInfo {
        id: snapshot.id.get(), revision: snapshot.revision.get(),
        phase: match snapshot.status { JobStatus::Running => 1, JobStatus::Cancelling => 2, JobStatus::Complete(_) => 3 },
        cancellation_requested: u32::from(snapshot.cancellation_requested),
        has_progress: u32::from(snapshot.progress.is_some()),
    });
    Ok(())
});

export!(reiny_job_snapshot_progress(snapshot: u64, out: *mut u64) {
    output(out)?;
    let snapshot = object(snapshot)?;
    let progress = variant!(snapshot, JobSnapshot).progress.as_ref()
        .ok_or_else(|| Error::new(STATE, "job has not reported progress"))?;
    out.write(insert(Object::Buffer(progress.as_ref().clone()), false)?);
    Ok(())
});

export!(reiny_job_snapshot_result(snapshot: u64, out: *mut u64) {
    output(out)?;
    let snapshot = object(snapshot)?;
    let result = match &variant!(snapshot, JobSnapshot).status {
        JobStatus::Complete(result) => Arc::clone(result),
        JobStatus::Running | JobStatus::Cancelling => return Err(Error::new(STATE, "job is not complete")),
    };
    out.write(insert(Object::JobResult(result), false)?);
    Ok(())
});

export!(reiny_job_result_kind(result: u64, out: *mut u32) {
    output(out)?;
    let result = object(result)?;
    out.write(match variant!(result, JobResult).as_ref() {
        Outcome::Succeeded(_) => 1, Outcome::Failed(_) => 2, Outcome::Cancelled => 3,
        Outcome::Panicked => 4, Outcome::Interrupted => 5,
    });
    Ok(())
});

export!(reiny_job_result_success(result: u64, out: *mut u64) {
    output(out)?;
    let result = object(result)?;
    let bytes = match variant!(result, JobResult).as_ref() {
        Outcome::Succeeded(bytes) => bytes,
        Outcome::Failed(_) | Outcome::Cancelled | Outcome::Panicked | Outcome::Interrupted =>
            return Err(Error::new(STATE, "job did not succeed")),
    };
    out.write(insert(Object::Buffer(bytes.clone()), false)?);
    Ok(())
});

export!(reiny_job_result_error(result: u64, code: *mut i32, out: *mut u64) {
    output(code)?;
    output(out)?;
    let result = object(result)?;
    let error = match variant!(result, JobResult).as_ref() {
        Outcome::Failed(error) => error,
        Outcome::Succeeded(_) | Outcome::Cancelled | Outcome::Panicked | Outcome::Interrupted =>
            return Err(Error::new(STATE, "job has no application error")),
    };
    let message = insert(Object::Buffer(error.message.as_bytes().to_vec()), false)?;
    code.write(error.code);
    out.write(message);
    Ok(())
});
