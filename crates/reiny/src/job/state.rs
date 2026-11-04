use std::sync::Arc;

use tokio::sync::watch;

use super::{CancelRequest, JobOutcome, JobRevision, JobSnapshot, JobStatus};

pub(super) trait Cancel: Send + Sync {
    fn cancel(&self) -> CancelRequest;
}

pub(super) struct State<P, T, E> {
    pub(super) updates: watch::Sender<JobSnapshot<P, T, E>>,
}

impl<P, T, E> State<P, T, E> {
    pub(super) fn request_cancel(&self) -> CancelRequest {
        let mut request = CancelRequest::Finished;
        self.updates
            .send_if_modified(|snapshot| match snapshot.status {
                JobStatus::Running => {
                    snapshot.status = JobStatus::Cancelling;
                    snapshot.cancellation_requested = true;
                    snapshot.revision.0 = snapshot.revision.0.saturating_add(1);
                    request = CancelRequest::Requested;
                    true
                }
                JobStatus::Cancelling => {
                    request = CancelRequest::AlreadyRequested;
                    false
                }
                JobStatus::Complete(_) => false,
            });
        request
    }

    pub(super) fn progress(&self, progress: P) -> bool {
        let mut incoming = Some(Arc::new(progress));
        // Move the old payload outside the lock before running its destructor.
        let mut previous = None;
        self.updates.send_if_modified(|snapshot| {
            match snapshot.status {
                JobStatus::Running => {}
                JobStatus::Cancelling | JobStatus::Complete(_) => return false,
            }
            // Reserve revisions for cancellation and completion; never wrap.
            let Some(next) = snapshot
                .revision
                .0
                .checked_add(1)
                .filter(|n| *n <= u64::MAX - 2)
            else {
                return false;
            };
            previous = std::mem::replace(&mut snapshot.progress, incoming.take());
            snapshot.revision = JobRevision(next);
            true
        })
    }

    pub(super) fn complete(&self, result: JobOutcome<T, E>) {
        let mut outcome = Some(Arc::new(result));
        let mut discarded = None;
        self.updates.send_if_modified(|snapshot| {
            let cancelled = match snapshot.status {
                JobStatus::Running => false,
                JobStatus::Cancelling => true,
                JobStatus::Complete(_) => return false,
            };
            let Some(terminal) = outcome.take() else {
                return false;
            };
            if cancelled {
                match terminal.as_ref() {
                    JobOutcome::Succeeded(_) => {
                        discarded = Some(terminal);
                        return false;
                    }
                    JobOutcome::Failed(_)
                    | JobOutcome::Cancelled
                    | JobOutcome::Panicked
                    | JobOutcome::Interrupted => {}
                }
            }
            snapshot.status = JobStatus::Complete(terminal);
            snapshot.revision.0 = snapshot.revision.0.saturating_add(1);
            true
        });
        if let Some(discarded) = discarded {
            // Keep Cancelling visible until cleanup finishes outside the lock.
            // If cleanup unwinds, Completion publishes Panicked instead.
            drop(discarded);
            self.complete(JobOutcome::Cancelled);
        }
    }
}

impl<P: Send + Sync, T: Send + Sync, E: Send + Sync> Cancel for State<P, T, E> {
    fn cancel(&self) -> CancelRequest {
        self.request_cancel()
    }
}

/// Ensures panic/runtime destruction is observable even without a live waiter.
pub(super) struct Completion<P, T, E>(pub(super) Arc<State<P, T, E>>);

impl<P, T, E> Drop for Completion<P, T, E> {
    fn drop(&mut self) {
        self.0.complete(if std::thread::panicking() {
            JobOutcome::Panicked
        } else {
            JobOutcome::Interrupted
        });
    }
}
