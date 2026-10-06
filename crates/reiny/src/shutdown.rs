//! Cooperative shutdown. A shared flag raised by Ctrl+C or by an explicit trigger.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// A cloneable shared shutdown signal.
#[derive(Clone, Default)]
pub(crate) struct Shutdown {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    flag: AtomicBool,
    notify: Notify,
}

impl Shutdown {
    /// A fresh, untriggered signal.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Request shutdown (idempotent).
    pub(crate) fn trigger(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// Poll whether it has been triggered.
    pub(crate) fn is_triggered(&self) -> bool {
        self.inner.flag.load(Ordering::SeqCst)
    }

    /// Wait asynchronously until it is triggered (for tokio components).
    pub(crate) async fn wait(&self) {
        loop {
            if self.is_triggered() {
                return;
            }
            // Re-check after creating notified() so a concurrent trigger cannot be missed.
            let notified = self.inner.notify.notified();
            if self.is_triggered() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn starts_untriggered_then_triggers() {
        let s = Shutdown::new();
        assert!(!s.is_triggered());
        let clone = s.clone();
        clone.trigger();
        assert!(s.is_triggered());
    }

    #[tokio::test]
    async fn wait_returns_after_trigger() {
        // Given a registered waiter, with no timing dependency on the spawned trigger.
        let s = Shutdown::new();
        let waiting = s.wait();
        tokio::pin!(waiting);
        assert!(matches!(
            std::future::poll_fn(|cx| { std::task::Poll::Ready(waiting.as_mut().poll(cx)) }).await,
            std::task::Poll::Pending
        ));
        // When shutdown is triggered, the existing waiter is released.
        s.trigger();
        // Then the wait completes within its bounded failure deadline.
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("wait should return promptly after trigger");
    }
}
