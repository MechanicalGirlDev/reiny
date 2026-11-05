use std::{future::Future, time::Duration};

use tokio::time::Instant;

/// Absence is a pending future, never a distant timer.
#[derive(Clone, Copy)]
pub(super) struct Deadline(pub Option<Instant>);

impl Deadline {
    pub(super) fn after(timeout: Option<Duration>) -> Self {
        Self(timeout.map(|timeout| Instant::now() + timeout))
    }

    pub(super) fn checked_after(timeout: Option<Duration>) -> Option<Self> {
        match timeout {
            Some(timeout) if timeout.is_zero() => None,
            Some(timeout) => Instant::now().checked_add(timeout).map(|at| Self(Some(at))),
            None => Some(Self(None)),
        }
    }

    pub(super) fn expired(self) -> bool {
        self.0.is_some_and(|at| Instant::now() >= at)
    }

    pub(super) async fn elapsed(self) {
        match self.0 {
            Some(at) => tokio::time::sleep_until(at).await,
            None => std::future::pending().await,
        }
    }

    pub(super) async fn scope<F: Future>(self, future: F) -> Result<F::Output, ()> {
        match self.0 {
            Some(at) => tokio::time::timeout_at(at, future).await.map_err(|_| ()),
            None => Ok(future.await),
        }
    }
}
