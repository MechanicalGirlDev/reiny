use tokio::time::{Instant, timeout_at};

use super::ProcessError;
use super::connection::SessionIo;
use super::protocol::{FailureCode, HostFailure, HostMessage, HostOutcome, HostPhase};
use crate::component::staged::HostControl;
use crate::component::{HostError, HostErrorCode, HostHandle, HostOutcome as LocalOutcome};

pub(super) fn code(code: HostErrorCode) -> FailureCode {
    match code {
        HostErrorCode::DuplicateFactory
        | HostErrorCode::UnknownFactory
        | HostErrorCode::Factory => FailureCode::Factory,
        HostErrorCode::EmptyGraph
        | HostErrorCode::DuplicateInstance
        | HostErrorCode::InvalidBindings
        | HostErrorCode::InvalidOptions
        | HostErrorCode::NoRuntime => FailureCode::Assignment,
        HostErrorCode::ContextOpen => FailureCode::ContextOpen,
        HostErrorCode::Prepare => FailureCode::Prepare,
        HostErrorCode::Admission => FailureCode::Admission,
        HostErrorCode::Graph => FailureCode::Report,
        HostErrorCode::Activate => FailureCode::Commit,
        // Step execution is local-only; the process protocol has one execution
        // failure category rather than exposing the local scheduler contract.
        HostErrorCode::Run | HostErrorCode::Step => FailureCode::Run,
        HostErrorCode::UnexpectedExit => FailureCode::UnexpectedExit,
        HostErrorCode::Panic => FailureCode::Panic,
        HostErrorCode::ReadinessLost => FailureCode::ReadinessLost,
        HostErrorCode::StopHook => FailureCode::StopHook,
        HostErrorCode::Timeout => FailureCode::Timeout,
        HostErrorCode::Cancelled => FailureCode::Cancelled,
    }
}

pub(super) fn outcome_code(outcome: &LocalOutcome) -> FailureCode {
    match outcome {
        LocalOutcome::Stopped => FailureCode::Cancelled,
        LocalOutcome::Failed { primary, .. }
        | LocalOutcome::Unresponsive {
            primary: Some(primary),
            ..
        } => code(primary.code),
        LocalOutcome::Unresponsive { primary: None, .. } => FailureCode::Timeout,
    }
}

fn failure(code: FailureCode, phase: HostPhase) -> HostFailure {
    HostFailure {
        code,
        phase,
        instance: None,
        message: format!("component host: {code:?}"),
    }
}

fn local_failure(error: HostError, phase: HostPhase) -> HostFailure {
    HostFailure {
        instance: error.instance,
        ..failure(code(error.code), phase)
    }
}

pub(super) async fn finish(
    local: Option<(HostHandle, HostControl)>,
    error: Option<ProcessError>,
    io: &mut SessionIo,
) -> Result<HostOutcome, ProcessError> {
    let SessionIo {
        guard,
        writer,
        phase,
    } = io;
    let phase = *phase;
    let deadline = Instant::now() + guard.cleanup_budget;
    let primary = match (error, guard.stop) {
        (_, Some(super::protocol::StopReason::Requested)) | (None, _) => None,
        (Some(error), _) => Some(failure(error.code, phase)),
    };
    let outcome = match local {
        Some((handle, control)) => {
            if let Some(primary) = &primary {
                let abort = control.abort(HostError::new(
                    HostErrorCode::Cancelled,
                    None,
                    primary.message.clone(),
                ));
                // A full control queue cannot consume more than the cleanup budget.
                match timeout_at(deadline, abort).await {
                    Ok(()) | Err(_) => {}
                }
            }
            let stopped = timeout_at(deadline, handle.stop()).await;
            match stopped {
                Ok(stopped) => convert(stopped, primary, phase),
                Err(_) => HostOutcome::Unresponsive {
                    primary,
                    cleanup: Vec::new(),
                    instances: handle.status().components.into_keys().collect(),
                },
            }
        }
        None => match primary {
            Some(primary) => HostOutcome::Failed {
                primary,
                cleanup: Vec::new(),
            },
            None => HostOutcome::Stopped {},
        },
    };
    // Cleanup completes even when reporting fails. Never claim network delivery.
    let reported = timeout_at(deadline, async {
        let primary = match &outcome {
            HostOutcome::Failed { primary, .. }
            | HostOutcome::Unresponsive {
                primary: Some(primary),
                ..
            } => Some(primary),
            HostOutcome::Stopped {} | HostOutcome::Unresponsive { primary: None, .. } => None,
        };
        if let Some(primary) = primary {
            writer.send(HostMessage::Failure(primary.clone())).await?;
        }
        writer.send(HostMessage::Outcome(outcome.clone())).await
    })
    .await;
    match reported {
        Ok(Ok(()) | Err(_)) | Err(_) => Ok(outcome),
    }
}

fn convert(outcome: LocalOutcome, primary: Option<HostFailure>, phase: HostPhase) -> HostOutcome {
    match outcome {
        LocalOutcome::Stopped => match primary {
            Some(primary) => HostOutcome::Failed {
                primary,
                cleanup: Vec::new(),
            },
            None => HostOutcome::Stopped {},
        },
        LocalOutcome::Failed {
            primary: local,
            cleanup,
        } => {
            let mut cleanup: Vec<_> = cleanup
                .into_iter()
                .map(|error| local_failure(error, HostPhase::Stopping))
                .collect();
            let local = local_failure(local, phase);
            let primary = match primary {
                Some(primary) if primary.code == local.code => local,
                Some(primary) => {
                    if local.code != FailureCode::Cancelled {
                        cleanup.insert(0, local);
                    }
                    primary
                }
                None => local,
            };
            HostOutcome::Failed { primary, cleanup }
        }
        LocalOutcome::Unresponsive {
            primary: local,
            cleanup,
            instances,
        } => {
            let mut cleanup: Vec<_> = cleanup
                .into_iter()
                .map(|error| local_failure(error, HostPhase::Stopping))
                .collect();
            let primary = match (primary, local) {
                (Some(primary), Some(local)) => {
                    if local.code != HostErrorCode::Cancelled {
                        cleanup.insert(0, local_failure(local, phase));
                    }
                    Some(primary)
                }
                (Some(primary), None) => Some(primary),
                (None, local) => local.map(|error| local_failure(error, phase)),
            };
            HostOutcome::Unresponsive {
                primary,
                cleanup,
                instances,
            }
        }
    }
}
