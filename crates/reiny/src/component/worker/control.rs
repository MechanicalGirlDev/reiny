use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Poll;

use tokio::sync::{mpsc, watch};

use super::super::{HostError, HostErrorCode, StopReason};
use super::{Command, Event};
use crate::managed::HostedEvent;

pub(super) enum Interrupted {
    Stop(StopReason),
    Hosted(HostedEvent),
    Failure(HostError),
}

pub(super) struct Monitor {
    pub(super) commands: watch::Receiver<Command>,
    pub(super) hosted: watch::Receiver<Option<HostedEvent>>,
    pub(super) step_gate: Option<std::sync::Arc<super::super::step::control::Gate>>,
}

impl Monitor {
    pub(super) async fn during<T>(
        &mut self,
        future: impl Future<Output = Result<T, HostError>>,
    ) -> Result<T, Interrupted> {
        let result = tokio::select! {
            biased;
            reason = stop_reason(&mut self.commands) => Err(Interrupted::Stop(reason)),
            event = self.hosted.wait_for(Option::is_some) => {
                let event = event.ok().and_then(|event| event.clone());
                Err(Interrupted::Hosted(event.unwrap_or(HostedEvent::StopRequested)))
            }
            result = future => result.map_err(Interrupted::Failure),
        };
        if result.is_err()
            && let Some(gate) = &self.step_gate
        {
            gate.stop();
        }
        result
    }

    pub(super) async fn activation(&mut self) -> Result<(), Interrupted> {
        tokio::select! {
            biased;
            event = self.hosted.wait_for(Option::is_some) => {
                let event = event.ok().and_then(|event| event.clone());
                Err(Interrupted::Hosted(event.unwrap_or(HostedEvent::StopRequested)))
            }
            command = self.commands.wait_for(|command| !matches!(command, Command::Prepare)) => {
                match command.map(|command| *command) {
                    Ok(Command::Run) => Ok(()),
                    Ok(Command::Stop(reason)) => Err(Interrupted::Stop(reason)),
                    Ok(Command::Prepare) | Err(_) => Err(Interrupted::Stop(StopReason::Requested)),
                }
            }
        }
    }
}

pub(super) async fn stop_reason(commands: &mut watch::Receiver<Command>) -> StopReason {
    match commands
        .wait_for(|command| matches!(command, Command::Stop(_)))
        .await
    {
        Ok(command) => match *command {
            Command::Stop(reason) => reason,
            Command::Prepare | Command::Run => StopReason::Requested,
        },
        Err(_) => StopReason::Requested,
    }
}

/// Catch unwinding from both hook construction and each poll, without a dependency.
pub(super) async fn guarded<T>(
    future: impl Future<Output = crate::Result<T>>,
    id: &str,
    code: HostErrorCode,
) -> Result<T, HostError> {
    let mut future = std::pin::pin!(future);
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(result)) => Poll::Ready(
                result.map_err(|error| HostError::new(code, Some(id), error.to_string())),
            ),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(HostError::new(
                HostErrorCode::Panic,
                Some(id),
                "component operation panicked",
            ))),
        },
    )
    .await
}

// Four inputs belong to the worker's failure boundary: the interruption, identity,
// event sink and stop subscription remain independent borrowed resources.
pub(super) async fn finish_interruption(
    interrupted: Interrupted,
    id: &str,
    events: &mpsc::Sender<Event>,
    commands: &mut watch::Receiver<Command>,
) -> StopReason {
    let event = match interrupted {
        Interrupted::Stop(reason) => return reason,
        Interrupted::Hosted(HostedEvent::StopRequested) => Event::StopRequested,
        Interrupted::Hosted(HostedEvent::ReadinessLost) => Event::Failure(HostError::new(
            HostErrorCode::ReadinessLost,
            Some(id),
            "a prepared or ready endpoint was dropped",
        )),
        Interrupted::Failure(error) => Event::Failure(error),
    };
    if events.send(event).await.is_err() {
        return StopReason::Requested;
    }
    stop_reason(commands).await
}
