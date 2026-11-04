//! Cooperative shutdown. A shared flag raised by Ctrl+C or by an explicit trigger.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::{Notify, watch};

use crate::managed::HostedEvent;

/// A cloneable shared shutdown signal.
#[derive(Clone, Default)]
pub(crate) struct Shutdown {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    flag: AtomicBool,
    notify: Notify,
    hosted: Option<watch::Sender<Option<HostedEvent>>>,
    step_gate: OnceLock<Arc<crate::component::step::control::Gate>>,
}

impl Shutdown {
    /// A fresh, untriggered signal.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn hosted() -> Self {
        Self {
            inner: Arc::new(Inner {
                hosted: Some(watch::channel(None).0),
                ..Inner::default()
            }),
        }
    }

    pub(crate) fn hosted_event(&self, event: HostedEvent) {
        if let Some(gate) = self.inner.step_gate.get() {
            gate.mark_stop();
        }
        if let Some(sender) = &self.inner.hosted {
            sender.send_if_modified(|current| {
                if current.is_some() {
                    return false;
                }
                *current = Some(event);
                true
            });
        }
    }

    pub(crate) fn attach_step_gate(&self, gate: Arc<crate::component::step::control::Gate>) {
        let _ = self.inner.step_gate.set(gate);
        // Context opening can already have received a transport stop. No step
        // can be admitted until after this attachment and managed activation.
        if (self.is_triggered()
            || self
                .inner
                .hosted
                .as_ref()
                .is_some_and(|events| events.borrow().is_some()))
            && let Some(gate) = self.inner.step_gate.get()
        {
            gate.mark_stop();
        }
    }

    pub(crate) fn hosted_events(&self) -> watch::Receiver<Option<HostedEvent>> {
        match &self.inner.hosted {
            Some(sender) => sender.subscribe(),
            None => watch::channel(None).1,
        }
    }

    /// Request shutdown (idempotent).
    pub(crate) fn trigger(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        self.hosted_event(HostedEvent::StopRequested);
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
