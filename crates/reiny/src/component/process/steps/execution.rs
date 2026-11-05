//! Correlated callback dispatch and structured result encoding.

use super::super::{
    ProcessError,
    connection::SessionIo,
    protocol::{
        FailureCode, HostMessage, OwnerMessage,
        step::{BoundSteps, StepHostMessage, StepOwnerMessage, StepResult},
    },
    step_wire::{check_values, export_failure, export_values, import_values, invalid},
    terminal,
};
use crate::{
    component::{HostHandle, step::worker},
    step::{StepFailureKind, StepSchema},
};
use std::collections::BTreeMap;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

pub(super) async fn run(
    handle: &HostHandle,
    io: &mut SessionIo,
    bound: BoundSteps,
    schemas: BTreeMap<String, StepSchema>,
    calls: BTreeMap<String, mpsc::UnboundedSender<worker::Call>>,
) -> Result<(), ProcessError> {
    let mut reservations = super::super::step_replay::ReplayGuard::new(&bound, &schemas)?;
    loop {
        let command = tokio::select! {
            biased;
            outcome = handle.wait_terminal() => return Err(ProcessError::new(terminal::outcome_code(&outcome))),
            command = io.guard.next() => command?,
        };
        let invocation = match command {
            OwnerMessage::Steps(message) => match *message {
                StepOwnerMessage::Invoke(call) => call,
                _ => return Err(invalid()),
            },
            _ => return Err(invalid()),
        };
        let call = &invocation.call;
        let schema = schemas.get(&call.instance).ok_or_else(invalid)?;
        let inputs = import_values(invocation.inputs.clone())?;
        check_values(&schema.inputs, &inputs)?;
        let budget = invocation
            .remaining_budget
            .duration()
            .map_err(|_| invalid())?;
        if budget.is_zero() {
            return Err(ProcessError::new(FailureCode::Timeout));
        }
        let deadline = Instant::now().checked_add(budget).ok_or_else(invalid)?;
        let tick = reservations.reserve(&invocation)?;
        let (reply, received) = oneshot::channel();
        calls[&call.instance]
            .send(worker::Call {
                tick,
                inputs,
                reply,
            })
            .map_err(|_| ProcessError::new(FailureCode::Disconnected))?;
        io.guard.deadline = Some(deadline);
        let result = io
            .guard
            .during(async {
                received
                    .await
                    .map_err(|_| ProcessError::new(FailureCode::Disconnected))
            })
            .await?;
        let result = encode_result(result, schema);
        let failed = result.is_err();
        let result = StepHostMessage::Result(StepResult {
            call: invocation.call,
            result,
        });
        io.guard
            .during(io.writer.send(HostMessage::Steps(Box::new(result))))
            .await?;
        io.guard.deadline = None;
        if failed {
            return Err(ProcessError::new(FailureCode::Run));
        }
    }
}

fn encode_result(
    result: Result<crate::step::StepOutputs, (StepFailureKind, String)>,
    schema: &StepSchema,
) -> Result<
    BTreeMap<String, super::super::protocol::step::WireValue>,
    super::super::protocol::step::WireStepFailure,
> {
    match result {
        Ok(outputs) => check_values(&schema.outputs, &outputs)
            .and_then(|()| export_values(outputs))
            .map_err(|_| {
                export_failure(
                    StepFailureKind::InvalidOutputs,
                    "invalid or oversized outputs".into(),
                )
            }),
        Err((kind, message)) => Err(export_failure(kind, message)),
    }
}
