use std::sync::Arc;

use tokio::sync::watch;

use super::state::State;
use super::{CancelRequest, JobOutcome, JobSnapshot, JobStatus};

/// Cloneable observation/control capability. Drop only abandons observation.
pub struct JobHandle<P, T, E> {
    pub(super) state: Arc<State<P, T, E>>,
    pub(super) updates: watch::Receiver<JobSnapshot<P, T, E>>,
}

impl<P, T, E> Clone for JobHandle<P, T, E> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            updates: self.updates.clone(),
        }
    }
}

impl<P, T, E> JobHandle<P, T, E> {
    /// Latest retained state, without marking this observer's revision seen.
    #[must_use]
    pub fn snapshot(&self) -> JobSnapshot<P, T, E> {
        self.updates.borrow().clone()
    }

    /// Create an independent observer before triggering an action.
    ///
    /// Its first `changed` waits for a revision after the current snapshot.
    #[must_use]
    pub fn subscribe(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            updates: self.state.updates.subscribe(),
        }
    }

    /// Wait for a newer revision, marking it seen. Intermediate progress may coalesce.
    ///
    /// Cancel-safe: dropping this wait does not stop work or lose an unseen update.
    pub async fn changed(&mut self) -> JobSnapshot<P, T, E> {
        // The handle owns a sender; closure cannot occur while this wait lives.
        match self.updates.changed().await {
            Ok(()) | Err(_) => self.updates.borrow_and_update().clone(),
        }
    }

    /// Request cancellation. Exactly one request can precede terminal completion.
    ///
    /// When cancellation wins, a successful worker return becomes `Cancelled`.
    /// Application errors and panics are preserved instead of hidden.
    #[must_use]
    pub fn cancel(&self) -> CancelRequest {
        self.state.request_cancel()
    }

    /// Wait for the one retained terminal result, independently of other waiters.
    ///
    /// Cancel-safe; may wait forever if a worker ignores cancellation. Use the
    /// owner's bounded shutdown or a caller deadline when a bounded wait is needed.
    pub async fn wait(&self) -> Arc<JobOutcome<T, E>> {
        let mut updates = self.state.updates.subscribe();
        loop {
            match &updates.borrow_and_update().status {
                JobStatus::Complete(outcome) => return Arc::clone(outcome),
                JobStatus::Running | JobStatus::Cancelling => {}
            }
            match updates.changed().await {
                Ok(()) | Err(_) => {}
            }
        }
    }
}

/// Worker-only capabilities: report actual progress and observe cancellation.
///
/// No completion setter is exposed. Return `Result<T, E>` from the worker to
/// finish; the owner synchronizes that return with cancellation.
pub struct JobContext<P, T, E> {
    pub(super) state: Arc<State<P, T, E>>,
}

impl<P, T, E> JobContext<P, T, E> {
    /// Replace latest progress. False means cancellation/completion already won,
    /// or the finite revision space is exhausted.
    pub fn report(&self, progress: P) -> bool {
        self.state.progress(progress)
    }

    /// Whether a cancellation request has been committed.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.updates.borrow().cancellation_requested
    }

    /// Observe a retained request, including one made before this wait started.
    ///
    /// The worker should release its resources and return after this resolves.
    pub async fn cancelled(&self) {
        let mut updates = self.state.updates.subscribe();
        loop {
            match &updates.borrow_and_update().status {
                JobStatus::Running => {}
                JobStatus::Cancelling | JobStatus::Complete(_) => return,
            }
            match updates.changed().await {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
