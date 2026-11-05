use std::{sync::Arc, time::Duration};

/// Explicit host resources and lifecycle deadlines.
pub struct HostOptions {
    /// The transport domain shared by this group.
    pub domain: String,
    /// A shared engine, commonly one `Local`.
    pub engine: Arc<dyn crate::engine::Engine>,
    /// Deadline for the whole startup, not per component.
    pub startup_timeout: Duration,
    /// Deadline for all cleanup and joins after shutdown begins.
    pub stop_timeout: Duration,
}

/// Local host resources with independently optional lifecycle deadlines.
///
/// `None` means no timer. In particular, an unbounded stop waits for all
/// callbacks, stop hooks, component destructors and the step driver to finish.
/// A callback that ignores cancellation can therefore keep stop pending forever.
pub struct LocalHostOptions {
    /// The transport domain shared by this group.
    pub domain: String,
    /// The actual in-process engine; remote transports cannot opt into this policy.
    pub engine: Arc<crate::engine::Local>,
    /// Whole-startup budget, or no deadline.
    pub startup_timeout: Option<Duration>,
    /// Whole-cleanup and join budget, or no deadline.
    pub stop_timeout: Option<Duration>,
}

pub(super) struct HostConfiguration {
    pub domain: String,
    pub engine: Arc<dyn crate::engine::Engine>,
    pub startup_timeout: Option<Duration>,
    pub stop_timeout: Option<Duration>,
}

impl From<HostOptions> for HostConfiguration {
    fn from(options: HostOptions) -> Self {
        Self {
            domain: options.domain,
            engine: options.engine,
            startup_timeout: Some(options.startup_timeout),
            stop_timeout: Some(options.stop_timeout),
        }
    }
}

impl From<LocalHostOptions> for HostConfiguration {
    fn from(options: LocalHostOptions) -> Self {
        Self {
            domain: options.domain,
            engine: options.engine,
            startup_timeout: options.startup_timeout,
            stop_timeout: options.stop_timeout,
        }
    }
}
