//! Negotiated process-step preparation on the existing staged host.

mod admission;
mod execution;

use super::{
    ProcessError, ProcessRegistry,
    connection::SessionIo,
    preparation,
    protocol::{
        Authenticate, Compatibility, FailureCode, HostMessage, HostPhase, Nonce, OwnerMessage,
        step::{MAX_ENTRIES, MAX_VALUE_BYTES, StepHostMessage, StepOwnerMessage, StepProtocol},
    },
    step_wire::{export_schema, invalid},
    terminal,
};
use crate::component::{
    ComponentHost, ComponentRegistry, HostHandle, staged::HostControl, step::worker,
};
use admission::{bind, report_steps};
use execution::run;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock},
};
use tokio::{sync::mpsc, time::Instant};

pub(super) async fn serve(
    nonce: Nonce,
    registry: ProcessRegistry,
    resources: &super::session::Resources,
    io: &mut SessionIo,
    local: &mut Option<(HostHandle, HostControl)>,
) -> Result<(), ProcessError> {
    let required = authenticate(nonce, &registry, io).await?;
    let (ordinary, assignments) = match io.guard.next().await? {
        OwnerMessage::Steps(message) => match *message {
            StepOwnerMessage::Prepare { ordinary, steps } => (ordinary, steps),
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    };
    validate_assignments(&ordinary, &assignments, &registry)?;
    let remaining = std::time::Duration::from_millis(ordinary.remaining_budget.0);
    let deadline = Instant::now().checked_add(remaining).ok_or_else(invalid)?;
    io.guard.deadline = Some(io.guard.deadline.map_or(deadline, |d| d.min(deadline)));
    io.phase = HostPhase::Preparing;
    let (mut options, mut instances) = io
        .guard
        .during(preparation::prepare(
            ordinary,
            &required,
            resources.limits,
            &io.writer.identity.host_id.0,
        ))
        .await?;
    options.startup_timeout = io.guard.deadline.map_or(std::time::Duration::ZERO, |d| {
        d.saturating_duration_since(Instant::now())
    });
    let mut adapted = ComponentRegistry::new();
    let mut calls = BTreeMap::new();
    let mut snapshots = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    for instance in &mut instances {
        let id = instance.bindings.namespace.clone();
        if let Some((schema, factory)) = registry.steps.get(&instance.factory_id) {
            let (sender, receiver) = mpsc::unbounded_channel();
            let prepared = Arc::new(OnceLock::new());
            adapted
                .register(
                    id.clone(),
                    Arc::new(worker::Factory {
                        implementation: factory.clone(),
                        calls: Mutex::new(Some(receiver)),
                        prepared: Some(prepared.clone()),
                    }),
                )
                .map_err(|_| invalid())?;
            snapshots.insert(id.clone(), prepared);
            calls.insert(id.clone(), sender);
            schemas.insert(id.clone(), schema.clone());
        } else {
            let factory = registry
                .local
                .factories
                .get(&instance.factory_id)
                .ok_or_else(invalid)?;
            adapted
                .register(id.clone(), factory.clone())
                .map_err(|_| invalid())?;
        }
        instance.factory_id = id;
    }
    {
        let _entered = resources.workers.enter();
        *local = Some(
            ComponentHost::prepare(Arc::new(adapted), options, instances)
                .map_err(|e| ProcessError::new(terminal::code(e.code)))?,
        );
    }
    let (handle, control) = local.as_mut().ok_or_else(invalid)?;
    let reports = io
        .guard
        .during(async {
            control
                .prepared()
                .await
                .map_err(|e| ProcessError::new(terminal::code(e.code)))
        })
        .await?;
    for report in reports {
        io.guard
            .during(io.writer.send(HostMessage::Report(report)))
            .await?;
    }
    report_steps(&assignments, &snapshots, &schemas, io).await?;
    drop(snapshots);
    let bound = bind(control, &schemas, io).await?;
    run(handle, io, bound, schemas, calls).await
}

async fn authenticate(
    nonce: Nonce,
    registry: &ProcessRegistry,
    io: &mut SessionIo,
) -> Result<Vec<super::protocol::FactoryRegistration>, ProcessError> {
    let proof = StepHostMessage::Authenticate {
        ordinary: Authenticate {
            nonce,
            compatibility: Compatibility::current(3),
            registrations: registry.registrations(),
        },
        version: StepProtocol,
        schemas: registry
            .steps
            .iter()
            .map(|(id, (schema, _))| (id.clone(), export_schema(schema)))
            .collect(),
    };
    io.guard
        .during(io.writer.send(HostMessage::Steps(Box::new(proof))))
        .await?;
    let required = match io.guard.next().await? {
        OwnerMessage::Steps(message) => match *message {
            StepOwnerMessage::Authenticated {
                compatibility,
                required_factories,
                ..
            } if compatibility == Compatibility::current(3) => required_factories,
            _ => return Err(ProcessError::new(FailureCode::Compatibility)),
        },
        _ => return Err(ProcessError::new(FailureCode::Compatibility)),
    };
    if required.is_empty()
        || required.iter().collect::<BTreeSet<_>>().len() != required.len()
        || required
            .iter()
            .any(|f| registry.revisions.get(&f.factory) != Some(&f.revision))
    {
        return Err(ProcessError::new(FailureCode::Factory));
    }
    Ok(required)
}

fn validate_assignments(
    ordinary: &super::protocol::Prepare,
    assignments: &BTreeMap<String, super::protocol::step::StepAssignment>,
    registry: &ProcessRegistry,
) -> Result<(), ProcessError> {
    let expected: BTreeMap<_, _> = ordinary
        .assignments
        .iter()
        .filter_map(|a| {
            registry
                .steps
                .get(&a.implementation.factory)
                .map(|(s, _)| (a.bindings.namespace.clone(), export_schema(s)))
        })
        .collect();
    if assignments.is_empty()
        || assignments.len() > MAX_ENTRIES
        || assignments.len() != expected.len()
        || assignments.iter().any(|(id, a)| {
            expected.get(id) != Some(&a.schema)
                || a.contract.len() > MAX_VALUE_BYTES
                || a.seed_outputs
                    .iter()
                    .any(|p| !a.schema.outputs.contains_key(p))
        })
    {
        return Err(invalid());
    }
    for instance in &ordinary.assignments {
        if let Some(assignment) = assignments.get(&instance.bindings.namespace)
            && assignment.seed_outputs.iter().any(|port| {
                !registry
                    .seed_outputs
                    .get(&instance.implementation.factory)
                    .is_some_and(|allowed| allowed.contains(port))
            })
        {
            return Err(invalid());
        }
    }
    Ok(())
}
