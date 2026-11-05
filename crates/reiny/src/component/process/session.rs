use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};

use super::connection::{Guard, Reader, SessionIo, Writer};
use super::protocol::{
    Authenticate, Compatibility, FailureCode, HostMessage, HostOutcome, HostPhase, OwnerMessage,
    Sequence,
};
use super::{Bootstrap, ProcessError, ProcessLimits, ProcessRegistry, preparation, terminal};
use crate::component::staged::HostControl;
use crate::component::{ComponentHost, HostHandle};

pub(super) struct Resources {
    pub limits: ProcessLimits,
    pub workers: tokio::runtime::Handle,
}

pub(super) async fn serve(
    bootstrap: Bootstrap,
    registry: ProcessRegistry,
    resources: Resources,
) -> Result<HostOutcome, ProcessError> {
    let deadline = Instant::now() + resources.limits.startup;
    let stream = timeout_at(deadline, TcpStream::connect(bootstrap.address))
        .await
        .map_err(|_| ProcessError::new(FailureCode::Timeout))?
        .map_err(ProcessError::io)?;
    stream.set_nodelay(true).map_err(ProcessError::io)?;
    let (read, write) = stream.into_split();
    let guard = Guard {
        reader: Reader::new(read, bootstrap.identity.clone(), resources.limits.frame),
        deadline: Some(deadline),
        stop: None,
        cleanup_budget: resources.limits.shutdown,
    };
    let writer = Writer {
        socket: write,
        identity: bootstrap.identity,
        sequence: Sequence(0),
        budget: resources.limits.frame,
        writable: true,
    };
    let mut io = SessionIo {
        guard,
        writer,
        phase: HostPhase::Authenticating,
    };
    let mut local = None;
    let result = async {
        io.guard
            .during(io.writer.send(HostMessage::Authenticate(Authenticate {
                nonce: bootstrap.nonce,
                compatibility: Compatibility::current(3),
                registrations: registry.registrations(),
            })))
            .await?;
        let required = authenticate(&mut io.guard, &registry).await?;
        let request = match io.guard.next().await? {
            OwnerMessage::Prepare(request) => request,
            OwnerMessage::Authenticated { .. }
            | OwnerMessage::Commit {}
            | OwnerMessage::Run {}
            | OwnerMessage::Stop { .. } => {
                return Err(ProcessError::new(FailureCode::Transition));
            }
        };
        let remaining = Duration::from_millis(request.remaining_budget.0);
        io.guard.deadline = Some(
            deadline.min(
                Instant::now()
                    .checked_add(remaining)
                    .ok_or_else(|| ProcessError::new(FailureCode::Timeout))?,
            ),
        );
        io.phase = HostPhase::Preparing;
        let (mut options, instances) = io
            .guard
            .during(preparation::prepare(
                request,
                &required,
                resources.limits,
                &io.writer.identity.host_id.0,
            ))
            .await?;
        options.startup_timeout = io.guard.deadline.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        });
        {
            let _entered = resources.workers.enter();
            local = Some(
                ComponentHost::prepare(Arc::new(registry.local), options, instances)
                    .map_err(|error| ProcessError::new(terminal::code(error.code)))?,
            );
        }
        let Some((handle, control)) = local.as_mut() else {
            return Err(ProcessError::new(FailureCode::Admission));
        };
        lifecycle(handle, control, &mut io).await
    }
    .await;
    terminal::finish(local, result.err(), &mut io).await
}

async fn authenticate(
    guard: &mut Guard,
    registry: &ProcessRegistry,
) -> Result<Vec<super::protocol::FactoryRegistration>, ProcessError> {
    match guard.next().await? {
        OwnerMessage::Authenticated {
            compatibility,
            required_factories,
        } => {
            if compatibility != Compatibility::current(3) {
                return Err(ProcessError::new(FailureCode::Compatibility));
            }
            if required_factories.is_empty()
                || required_factories.len() > registry.revisions.len()
                || required_factories
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != required_factories.len()
                || required_factories
                    .iter()
                    .any(|entry| registry.revisions.get(&entry.factory) != Some(&entry.revision))
            {
                return Err(ProcessError::new(FailureCode::Factory));
            }
            Ok(required_factories)
        }
        OwnerMessage::Prepare(_)
        | OwnerMessage::Commit {}
        | OwnerMessage::Run {}
        | OwnerMessage::Stop { .. } => Err(ProcessError::new(FailureCode::Transition)),
    }
}

async fn lifecycle(
    handle: &HostHandle,
    control: &mut HostControl,
    io: &mut SessionIo,
) -> Result<(), ProcessError> {
    let SessionIo {
        guard,
        writer,
        phase,
    } = io;
    let reports = guard
        .during(async {
            control
                .prepared()
                .await
                .map_err(|error| ProcessError::new(terminal::code(error.code)))
        })
        .await?;
    for report in reports {
        guard
            .during(writer.send(HostMessage::Report(report)))
            .await?;
    }
    *phase = HostPhase::Prepared;
    guard
        .during(writer.send(HostMessage::PreparedComplete {}))
        .await?;
    match command(handle, guard).await? {
        OwnerMessage::Commit {} => {}
        OwnerMessage::Authenticated { .. }
        | OwnerMessage::Prepare(_)
        | OwnerMessage::Run {}
        | OwnerMessage::Stop { .. } => return Err(ProcessError::new(FailureCode::Transition)),
    }
    guard
        .during(async {
            control
                .commit()
                .await
                .map_err(|error| ProcessError::new(terminal::code(error.code)))
        })
        .await?;
    *phase = HostPhase::Committed;
    guard.during(writer.send(HostMessage::Committed {})).await?;
    match command(handle, guard).await? {
        OwnerMessage::Run {} => {}
        OwnerMessage::Authenticated { .. }
        | OwnerMessage::Prepare(_)
        | OwnerMessage::Commit {}
        | OwnerMessage::Stop { .. } => return Err(ProcessError::new(FailureCode::Transition)),
    }
    guard
        .during(async {
            control
                .activate()
                .await
                .map_err(|error| ProcessError::new(terminal::code(error.code)))
        })
        .await?;
    *phase = HostPhase::Running;
    guard.during(writer.send(HostMessage::Running {})).await?;
    guard.deadline = None;
    command(handle, guard).await?;
    Err(ProcessError::new(FailureCode::Transition))
}

async fn command(handle: &HostHandle, guard: &mut Guard) -> Result<OwnerMessage, ProcessError> {
    tokio::select! {
        biased;
        outcome = handle.wait_terminal() => Err(ProcessError::new(terminal::outcome_code(&outcome))),
        command = guard.next() => command,
    }
}
