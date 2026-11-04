//! Trusted host-runtime control, deliberately not an application API.
//!
//! A process adapter inside this module tree may commit only on its authenticated
//! deployment owner's command, after that owner validates the complete graph.
//! Local launch uses the same stages but performs that validation here.

use tokio::sync::{mpsc, oneshot, watch};

use crate::bindings::{ModuleBindings, ModuleReport, ReportEntry, validate_graph};

use super::{HostError, HostErrorCode, HostOutcome};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Commit,
    Activate,
}

pub(super) enum Request {
    Advance(Stage, oneshot::Sender<()>),
    Abort(HostError),
}

/// The sole stage sender. Dropping it before activation aborts preparation.
pub(super) struct HostControl {
    pub requests: mpsc::Sender<Request>,
    pub reports: watch::Receiver<Option<Vec<ModuleReport>>>,
    pub terminal: watch::Receiver<Option<HostOutcome>>,
}

impl HostControl {
    /// Wait cancellation is harmless; reports are retained in namespace order.
    pub(super) async fn prepared(&mut self) -> Result<Vec<ModuleReport>, HostError> {
        tokio::select! {
            biased;
            error = terminal_error(&mut self.terminal) => Err(error),
            reports = async {
                self.reports.wait_for(Option::is_some).await.ok().and_then(|reports| reports.clone())
            } => {
                match reports {
                    Some(reports) => Ok(reports),
                    None => Err(terminal_error(&mut self.terminal).await),
                }
            }
        }
    }

    /// Requires whole-graph validation by the trusted controller. This method
    /// commits only this host's exact retained tickets, never replacement reports.
    ///
    /// Cancelling the wait does not retract an already submitted command. Do not
    /// retry it: observe retained host status or abort the admission attempt.
    pub(super) async fn commit(&mut self) -> Result<(), HostError> {
        self.advance(Stage::Commit).await
    }

    /// Release run only after every host's commit acknowledgement.
    /// The same cancellation rule as `commit` applies.
    pub(super) async fn activate(&mut self) -> Result<(), HostError> {
        self.advance(Stage::Activate).await
    }

    /// Retain the owner's failure and clean up prepared/ready tickets, including
    /// a partially activated distributed attempt whose local run was released.
    pub(super) async fn abort(&self, error: HostError) {
        if self.requests.send(Request::Abort(error)).await.is_err() {
            // A closed supervisor already has its own terminal outcome.
        }
    }

    async fn advance(&mut self, stage: Stage) -> Result<(), HostError> {
        let (reply, received) = oneshot::channel();
        if self
            .requests
            .send(Request::Advance(stage, reply))
            .await
            .is_err()
        {
            return Err(terminal_error(&mut self.terminal).await);
        }
        tokio::select! {
            biased;
            error = terminal_error(&mut self.terminal) => Err(error),
            result = received => match result {
                Ok(()) => Ok(()),
                Err(_) => Err(terminal_error(&mut self.terminal).await),
            }
        }
    }

    pub(super) async fn admit_local(mut self, bindings: Vec<ModuleBindings>) {
        let Ok(reports) = self.prepared().await else {
            return;
        };
        let entries = bindings
            .iter()
            .map(|bindings| {
                reports
                    .iter()
                    .find(|report| report.namespace == bindings.namespace)
                    .map(|report| ReportEntry { bindings, report })
                    .ok_or_else(|| {
                        HostError::new(
                            HostErrorCode::Admission,
                            Some(&bindings.namespace),
                            "missing prepared report",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>();
        let validated = entries.and_then(|entries| {
            validate_graph(&entries).map_err(|report| HostError {
                code: HostErrorCode::Graph,
                instance: report.module.clone(),
                message: report.to_string(),
                report: Some(Box::new(report)),
            })
        });
        if let Err(error) = validated {
            self.abort(error).await;
            return;
        }
        if self.commit().await.is_ok() {
            // A failed activation already causes coordinator-owned cleanup.
            match self.activate().await {
                Ok(()) | Err(_) => {}
            }
        }
    }
}

async fn terminal_error(terminal: &mut watch::Receiver<Option<HostOutcome>>) -> HostError {
    match terminal.wait_for(Option::is_some).await {
        Ok(outcome) => match outcome.as_ref() {
            Some(
                HostOutcome::Failed { primary, .. }
                | HostOutcome::Unresponsive {
                    primary: Some(primary),
                    ..
                },
            ) => primary.clone(),
            Some(HostOutcome::Stopped) => HostError::new(
                HostErrorCode::Cancelled,
                None,
                "host stopped before admission completed",
            ),
            Some(HostOutcome::Unresponsive { primary: None, .. }) => HostError::new(
                HostErrorCode::Timeout,
                None,
                "host did not join before its stop deadline",
            ),
            None => HostError::new(HostErrorCode::Panic, None, "missing terminal outcome"),
        },
        Err(_) => HostError::new(HostErrorCode::Panic, None, "host supervisor terminated"),
    }
}
