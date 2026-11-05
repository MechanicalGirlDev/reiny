//! Actual snapshot reports and the host's bind/commit/run barrier.

use super::super::{
    ProcessError,
    connection::SessionIo,
    protocol::{
        HostMessage, HostPhase, OwnerMessage,
        step::{BoundSteps, MAX_ENTRIES, StepHostMessage, StepOwnerMessage, StepPreparedReport},
    },
    step_wire::{export_values, invalid},
    terminal,
};
use crate::{
    component::{staged::HostControl, step::worker},
    step::StepSchema,
};
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn report_steps(
    assignments: &BTreeMap<String, super::super::protocol::step::StepAssignment>,
    snapshots: &BTreeMap<String, worker::Prepared>,
    schemas: &BTreeMap<String, StepSchema>,
    io: &mut SessionIo,
) -> Result<(), ProcessError> {
    for (instance, assignment) in assignments {
        let (outputs, contract) = snapshots[instance].get().ok_or_else(invalid)?;
        if contract != &assignment.contract {
            return Err(invalid());
        }
        if outputs
            .keys()
            .any(|p| !schemas[instance].outputs.contains_key(p))
            || outputs
                .iter()
                .any(|(p, v)| schemas[instance].outputs.get(p) != Some(v.step_type()))
        {
            return Err(invalid());
        }
        let outputs = assignment
            .seed_outputs
            .iter()
            .map(|p| {
                outputs
                    .get(p)
                    .cloned()
                    .map(|v| (p.clone(), v))
                    .ok_or_else(invalid)
            })
            .collect::<Result<_, _>>()?;
        let report = StepPreparedReport {
            instance: instance.clone(),
            schema: assignment.schema.clone(),
            contract: contract.clone(),
            outputs: export_values(outputs)?,
        };
        io.guard
            .during(
                io.writer
                    .send(HostMessage::Steps(Box::new(StepHostMessage::Report(
                        report,
                    )))),
            )
            .await?;
    }
    Ok(())
}

pub(super) async fn bind(
    control: &mut HostControl,
    schemas: &BTreeMap<String, StepSchema>,
    io: &mut SessionIo,
) -> Result<BoundSteps, ProcessError> {
    io.phase = HostPhase::Prepared;
    io.guard
        .during(io.writer.send(HostMessage::PreparedComplete {}))
        .await?;
    let bound = match io.guard.next().await? {
        OwnerMessage::Steps(message) => match *message {
            StepOwnerMessage::Bind(bound) => bound,
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    };
    let identity = &io.writer.identity;
    let graph = super::super::step_wire::import_graph(bound.graph.clone())?;
    let validated = crate::step::Plan::compile(graph.clone()).map_err(|_| invalid())?;
    if bound.session.owner_generation != identity.owner_generation
        || bound.session.graph_generation != identity.graph_generation
        || bound.session.graph_digest != identity.graph_digest
        || bound.order.len() > MAX_ENTRIES
        || bound.order.iter().collect::<BTreeSet<_>>().len() != bound.order.len()
        || schemas.keys().any(|id| !bound.order.contains(id))
        || schemas
            .iter()
            .any(|(id, schema)| graph.nodes.get(id) != Some(schema))
        || validated.order != bound.order
        || super::super::step_wire::plan_digest(&bound.graph, bound.initial_time)?
            != bound.session.plan_digest
    {
        return Err(invalid());
    }
    bound.initial_time.duration().map_err(|_| invalid())?;
    io.guard
        .during(
            io.writer
                .send(HostMessage::Steps(Box::new(StepHostMessage::Bound(
                    bound.clone(),
                )))),
        )
        .await?;
    if !matches!(io.guard.next().await?, OwnerMessage::Commit {}) {
        return Err(invalid());
    }
    io.guard
        .during(async {
            control
                .commit()
                .await
                .map_err(|e| ProcessError::new(terminal::code(e.code)))
        })
        .await?;
    io.phase = HostPhase::Committed;
    io.guard
        .during(io.writer.send(HostMessage::Committed {}))
        .await?;
    if !matches!(io.guard.next().await?, OwnerMessage::Run {}) {
        return Err(invalid());
    }
    io.guard
        .during(async {
            control
                .activate()
                .await
                .map_err(|e| ProcessError::new(terminal::code(e.code)))
        })
        .await?;
    io.phase = HostPhase::Running;
    io.guard
        .during(io.writer.send(HostMessage::Running {}))
        .await?;
    io.guard.deadline = None;
    Ok(bound)
}
