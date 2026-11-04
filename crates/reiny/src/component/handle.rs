use tokio::sync::watch;

use super::{HostError, HostErrorCode, HostOutcome, HostPhase, HostStatus};

/// Observe and stop a component group. Dropping this handle requests shutdown.
///
/// Cancellation of a wait does not cancel the supervisor. `Unresponsive` means
/// task completion is unconfirmed, not that native code or leaked handles stopped.
pub struct HostHandle {
    pub(super) status: watch::Receiver<HostStatus>,
    pub(super) stop: watch::Sender<bool>,
    pub(super) terminal: watch::Receiver<Option<HostOutcome>>,
    pub(super) step_gate: Option<std::sync::Arc<super::step::control::Gate>>,
}

impl HostHandle {
    /// Read the latest group snapshot without waiting.
    #[must_use]
    pub fn status(&self) -> HostStatus {
        self.status.borrow().clone()
    }

    /// Subscribe to retained status changes.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<HostStatus> {
        self.status.clone()
    }

    /// Wait for running, or return the terminal failure/cancellation.
    pub async fn wait_running(&self) -> Result<(), HostError> {
        let mut status = self.subscribe();
        loop {
            let phase = status.borrow_and_update().phase;
            match phase {
                HostPhase::Running => return Ok(()),
                HostPhase::Starting | HostPhase::Prepared | HostPhase::Committed => {}
                HostPhase::Stopping
                | HostPhase::Stopped
                | HostPhase::Failed
                | HostPhase::Unresponsive => break,
            }
            if status.changed().await.is_err() {
                break;
            }
        }
        let error = match self.wait_terminal().await {
            HostOutcome::Failed { primary, .. }
            | HostOutcome::Unresponsive {
                primary: Some(primary),
                ..
            } => primary,
            HostOutcome::Stopped => HostError::new(
                HostErrorCode::Cancelled,
                None,
                "host stopped before running",
            ),
            HostOutcome::Unresponsive { primary: None, .. } => HostError::new(
                HostErrorCode::Timeout,
                None,
                "host did not join before its stop deadline",
            ),
        };
        Err(error)
    }

    /// Request shutdown and await the group's shared terminal result.
    pub async fn stop(&self) -> HostOutcome {
        if let Some(gate) = &self.step_gate {
            gate.stop();
        }
        self.stop.send_replace(true);
        self.wait_terminal().await
    }

    /// Wait for the terminal result without requesting shutdown.
    pub async fn wait_terminal(&self) -> HostOutcome {
        let mut terminal = self.terminal.clone();
        loop {
            if let Some(outcome) = terminal.borrow_and_update().clone() {
                return outcome;
            }
            if terminal.changed().await.is_err() {
                return HostOutcome::Unresponsive {
                    primary: Some(HostError::new(
                        HostErrorCode::Panic,
                        None,
                        "host supervisor terminated",
                    )),
                    cleanup: Vec::new(),
                    instances: self.status().components.into_keys().collect(),
                };
            }
        }
    }
}

impl Drop for HostHandle {
    fn drop(&mut self) {
        if let Some(gate) = &self.step_gate {
            gate.stop();
        }
        self.stop.send_replace(true);
    }
}
