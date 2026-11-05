use super::{
    Arc, Duration, Error, INVALID, Job, Object, Reservation, STATE, boundary, callback, export,
    failure, lock, object, ordinary, output, runtime, variant, wrong,
};
use reiny::job::{JobOwner, StartError};
use std::num::NonZeroUsize;

export!(reiny_job_owner_new(capacity: usize, out: *mut u64) {
    output(out)?;
    let capacity = NonZeroUsize::new(capacity).ok_or_else(|| Error::new(INVALID, "job capacity must be positive"))?;
    out.write(super::insert(Object::JobOwner(tokio::sync::Mutex::new(JobOwner::new(capacity))), false)?);
    Ok(())
});

export!(reiny_job_start(owner: u64, work: u64, out: *mut u64) {
    output(out)?;
    ordinary()?;
    let owner = object(owner)?;
    let owner = variant!(owner, JobOwner);
    let work_object = object(work)?;
    let work = variant!(work_object, JobWork);
    let reservation = Reservation::new()?;
    let (joined, receiver) = tokio::sync::watch::channel(false);
    let job = runtime()?.block_on(async {
        let mut owner = owner.lock().await;
        let mut admitted = lock(&work.admitted)?;
        if *admitted { return Err(Error::new(STATE, "work was already admitted")); }
        let work = Arc::clone(work);
        let job = owner.start(move |context| async move {
            // The core worker stays alive until the ordinary native thread has
            // actually joined. Cancellation never drops this wait.
            let native = std::thread::Builder::new().name("reiny-job".into())
                .spawn(move || callback::run(work, context));
            match native {
                Ok(native) => {
                    let result = tokio::task::spawn_blocking(move || native.join()).await.map_err(failure)?;
                    joined.send_replace(true);
                    match result {
                        Ok(result) => result,
                        Err(panic) => std::panic::resume_unwind(panic),
                    }
                }
                Err(error) => {
                    joined.send_replace(true);
                    Err(failure(error))
                }
            }
        }).map_err(|error| {
            let code = match error {
                StartError::Closed => -201, StartError::AtCapacity => -202,
                StartError::IdExhausted => -203, StartError::RuntimeUnavailable => -204,
            };
            Error::new(code, error.to_string())
        })?;
        *admitted = true;
        Ok(Job { core: job, joined: receiver })
    })?;
    out.write(reservation.publish(Object::Job(job))?);
    Ok(())
});

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_job_owner_close(owner: u64) -> i32 {
    boundary(|| {
        ordinary()?;
        let owner = object(owner)?;
        runtime()?.block_on(async {
            variant!(owner, JobOwner).lock().await.close();
            Ok(())
        })
    })
}

export!(reiny_job_owner_shutdown(owner: u64, grace_ns: u64, out: *mut u64) {
    output(out)?;
    ordinary()?;
    let owner = object(owner)?;
    let owner = variant!(owner, JobOwner);
    let reservation = Reservation::new()?;
    let report = runtime()?.block_on(async {
        owner.lock().await.shutdown(Duration::from_nanos(grace_ns)).await.map_err(failure)
    })?;
    let report = serde_json::json!({
        "version":1,
        "unresponsive":report.unresponsive.iter().map(|id| id.get()).collect::<Vec<_>>(),
        "join_failures":report.join_failures.iter().map(|id| id.get()).collect::<Vec<_>>(),
    });
    out.write(reservation.publish(Object::Buffer(serde_json::to_vec(&report).map_err(failure)?))?);
    Ok(())
});
