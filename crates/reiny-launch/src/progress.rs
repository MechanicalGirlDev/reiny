//! Preparation progress and latched, thread-safe cancellation.

use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A cancellation request shared by the caller, scheduler and owned processes.
#[derive(Clone, Default)]
pub struct CancelToken(Arc<Cancellation>);

#[derive(Default)]
struct Cancellation {
    cancelled: Mutex<bool>,
    changed: Condvar,
    notify: tokio::sync::Notify,
    children: Mutex<Vec<Weak<Cancellation>>>,
}

impl CancelToken {
    /// Latch cancellation and wake every current waiter.
    pub fn cancel(&self) {
        let mut cancelled = self
            .0
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *cancelled = true;
        self.0.changed.notify_all();
        self.0.notify.notify_waiters();
        for child in self
            .0
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(Weak::upgrade)
        {
            Self(child).cancel();
        }
    }

    pub(crate) fn child(&self) -> Self {
        let cancelled = self
            .0
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let child = Self(Arc::new(Cancellation {
            cancelled: Mutex::new(*cancelled),
            ..Cancellation::default()
        }));
        let mut children = self
            .0
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        children.retain(|child| child.strong_count() > 0);
        children.push(Arc::downgrade(&child.0));
        child
    }

    /// Whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self
            .0
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn check(&self) -> Result<(), PrepareError> {
        if self.is_cancelled() {
            Err(PrepareError::Cancelled)
        } else {
            Ok(())
        }
    }

    pub(crate) async fn cancelled(&self) {
        loop {
            let notified = self.0.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn wait_timeout(&self, duration: Duration) -> Result<(), PrepareError> {
        let state = self
            .0
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .0
            .changed
            .wait_timeout_while(state, duration, |cancelled| !*cancelled)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state {
            Err(PrepareError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Serialize a final publication with cancellation.
    ///
    /// The closure must be short and must not call this token or an observer.
    /// Cancellation winning the lock prevents publication; publication winning it
    /// completes successfully even if cancellation is requested immediately later.
    pub fn publish<T>(
        &self,
        publish: impl FnOnce() -> anyhow::Result<T>,
    ) -> Result<T, PrepareError> {
        let state = self
            .0
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state {
            return Err(PrepareError::Cancelled);
        }
        publish().map_err(PrepareError::Failed)
    }
}

/// A short, nonblocking progress callback. Calls within a request are serialized.
pub trait PrepareObserver: Sync {
    /// Receive one preparation event.
    fn event(&self, event: &PrepareEvent);
}

/// Controls one preparation transaction.
pub struct PrepareOptions<'a> {
    /// Cancellation shared with the caller.
    pub cancel: CancelToken,
    /// Optional serialized event sink.
    pub observer: Option<&'a dyn PrepareObserver>,
    /// Maximum concurrently executing groups; must be positive.
    pub jobs: usize,
    /// Persistent compiler cache base, independent of immutable snapshots.
    pub build_cache: Option<PathBuf>,
}

impl Default for PrepareOptions<'_> {
    fn default() -> Self {
        Self {
            cancel: CancelToken::default(),
            observer: None,
            jobs: 1,
            build_cache: None,
        }
    }
}

/// Failure of source resolution or materialization.
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    /// The caller cancelled the transaction.
    #[error("preparation cancelled")]
    Cancelled,
    /// Another preparation owns this deployment root.
    #[error("another preparation holds the deployment root lock")]
    Busy,
    /// A declaration, subprocess or filesystem operation failed.
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

/// Current materialization phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparePhase {
    /// Resolving declarations and pinned sources.
    Resolving,
    /// Determining exact compatible groups.
    Planning,
    /// Running declared builders.
    Building,
    /// Freezing runtime inputs.
    Freezing,
    /// Publishing the prepared result.
    Writing,
}

/// Work represented by a preparation group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupKind {
    /// Cargo incremental validation and artifact snapshot.
    Cargo,
    /// An explicit argv builder.
    Command,
    /// Installed executable snapshot.
    Prebuilt,
    /// Configuration and declared assets.
    Config,
    /// A managed file resource.
    Resource,
}

/// Lifecycle of one planned group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupState {
    /// Waiting for a worker.
    Queued,
    /// Executing or freezing.
    Running,
    /// Successfully reused fresh compiler artifacts or prebuilt bytes.
    Cached,
    /// Successfully completed.
    Done,
    /// Failed.
    Failed,
    /// Cancelled, including work never started.
    Cancelled,
}

/// Terminal preparation outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrepareStatus {
    /// All artifacts were frozen successfully.
    Success,
    /// Preparation failed.
    Failed,
    /// Preparation was cancelled.
    Cancelled,
}

/// Version-independent payload of the versioned preparation event protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum PrepareEvent {
    /// First event of every request.
    Started,
    /// A phase transition.
    Phase {
        /// New phase.
        phase: PreparePhase,
    },
    /// Complete group count, before any group event.
    Planned {
        /// Number of executable, config and resource groups.
        total_groups: usize,
    },
    /// A group transition.
    Group {
        /// Deterministic group identity.
        id: String,
        /// Work kind.
        kind: GroupKind,
        /// New state.
        state: GroupState,
        /// Affected module namespaces.
        nodes: Vec<String>,
        /// Requested executable names.
        bins: Vec<String>,
    },
    /// A Cargo compiler artifact, without a fictional total crate count.
    Compiled {
        /// Group identity.
        group: String,
        /// Cargo target name.
        crate_name: String,
        /// Cargo's actual freshness flag.
        fresh: bool,
        /// Number of compiler artifacts observed in this group.
        done: u32,
    },
    /// Builder or preparation diagnostics.
    Diagnostic {
        /// Group identity when applicable.
        group: Option<String>,
        /// Diagnostic severity or stream.
        level: String,
        /// Complete diagnostic text.
        text: String,
    },
    /// Exactly one terminal event, last in the request.
    Finished {
        /// Final outcome.
        status: PrepareStatus,
        /// Failure or cancellation detail.
        error: Option<String>,
    },
}

pub(crate) struct Events<'a> {
    observer: Option<&'a dyn PrepareObserver>,
    publication: Mutex<()>,
}

impl<'a> Events<'a> {
    pub(crate) fn new(observer: Option<&'a dyn PrepareObserver>) -> Self {
        Self {
            observer,
            publication: Mutex::new(()),
        }
    }

    pub(crate) fn emit(&self, event: PrepareEvent) {
        let _publication = self
            .publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(observer) = self.observer {
            observer.event(&event);
        } else if let PrepareEvent::Diagnostic { text, .. } = event {
            eprintln!("{text}");
        }
    }
}
