use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use super::state::{Cancel, Completion, State};
use super::{
    JobContext, JobHandle, JobId, JobOutcome, JobRevision, JobSnapshot, JobStatus, ShutdownError,
    ShutdownReport, StartError,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct Worker {
    id: JobId,
    control: Arc<dyn Cancel>,
    join: JoinHandle<()>,
}

/// An explicit lifetime and capacity boundary for heterogeneous typed jobs.
///
/// Keep this in the parent component. `shutdown` closes admission permanently,
/// requests cancellation, then joins workers. It can be retried after timeout.
/// Drop requests cancellation but cannot await cleanup, and does not abort tasks.
/// Finished entries are reaped on start; no historical result registry grows here.
pub struct JobOwner {
    capacity: NonZeroUsize,
    closed: bool,
    workers: Vec<Worker>,
}

impl JobOwner {
    /// Create an owner with an explicit maximum number of concurrent workers.
    #[must_use]
    pub const fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            closed: false,
            workers: Vec::new(),
        }
    }

    /// Schedule actual application work on the current Tokio runtime.
    ///
    /// The observation channel exists before the closure can run, so even an
    /// immediate completion is retained. The closure itself runs inside the task,
    /// making both factory and future panics observable.
    ///
    /// # Errors
    ///
    /// Returns [`StartError`] if closed, at capacity, outside Tokio, or out of IDs.
    pub fn start<P, T, E, F, Fut>(&mut self, work: F) -> Result<JobHandle<P, T, E>, StartError>
    where
        P: Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
        F: FnOnce(JobContext<P, T, E>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        if self.closed {
            return Err(StartError::Closed);
        }
        self.workers.retain(|worker| !worker.join.is_finished());
        if self.workers.len() >= self.capacity.get() {
            return Err(StartError::AtCapacity);
        }
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| StartError::RuntimeUnavailable)?;
        let id = JobId(
            NEXT_ID
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .map_err(|_| StartError::IdExhausted)?,
        );
        let (updates, receiver) = watch::channel(JobSnapshot {
            id,
            revision: JobRevision(0),
            progress: None,
            cancellation_requested: false,
            status: JobStatus::Running,
        });
        let state = Arc::new(State { updates });
        let completion = Completion(Arc::clone(&state));
        let context = JobContext {
            state: Arc::clone(&state),
        };
        let join = runtime.spawn(async move {
            let completion = completion;
            let outcome = match work(context).await {
                Ok(value) => JobOutcome::Succeeded(value),
                Err(error) => JobOutcome::Failed(error),
            };
            completion.0.complete(outcome);
        });
        self.workers.push(Worker {
            id,
            control: state.clone(),
            join,
        });
        Ok(JobHandle {
            state,
            updates: receiver,
        })
    }

    /// Close admission and request cancellation of all owned jobs, idempotently.
    pub fn close(&mut self) {
        self.closed = true;
        for worker in &self.workers {
            worker.control.cancel();
        }
    }

    /// Cancel and join until one shared deadline, retaining unfinished workers.
    ///
    /// Cancel-safe: abandoning this future keeps workers owned and admission
    /// closed. Repeating shutdown resumes joining. Timeout does not abort workers
    /// or turn their snapshots terminal. A non-yielding task can also prevent a
    /// single-thread Tokio runtime from polling this deadline.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::InvalidDeadline`] if `grace` cannot be added to
    /// the current instant. Admission is still closed and cancellation requested,
    /// but no workers are joined or removed. Retry with a representable grace.
    pub async fn shutdown(&mut self, grace: Duration) -> Result<ShutdownReport, ShutdownError> {
        self.close();
        let deadline = Instant::now()
            .checked_add(grace)
            .ok_or(ShutdownError::InvalidDeadline)?;
        let mut report = ShutdownReport {
            unresponsive: Vec::new(),
            join_failures: Vec::new(),
        };
        let mut index = 0;
        while index < self.workers.len() {
            let worker = &mut self.workers[index];
            if let Ok(joined) = timeout_at(deadline, &mut worker.join).await {
                if joined.is_err() {
                    report.join_failures.push(worker.id);
                }
                self.workers.remove(index);
            } else {
                report.unresponsive.push(worker.id);
                index += 1;
            }
        }
        Ok(report)
    }
}

impl Drop for JobOwner {
    fn drop(&mut self) {
        self.close();
    }
}
