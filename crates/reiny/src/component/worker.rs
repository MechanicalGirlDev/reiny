use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::managed::{HostedOptions, PreparedAdmission};

use super::status::phase;
use super::{
    Component, ComponentContext, ComponentFactory, ComponentPhase, HostError, HostErrorCode,
    HostStatus, StopReason,
};

mod control;
use control::{Interrupted, Monitor, finish_interruption, guarded, stop_reason};

#[derive(Clone, Copy)]
pub(super) enum Command {
    Prepare,
    Run,
    Stop(StopReason),
}

pub(super) enum Event {
    Opened(Arc<ComponentContext>),
    Prepared(String, PreparedAdmission),
    Failure(HostError),
    StopRequested,
}

pub(super) struct Worker {
    pub factory: Arc<dyn ComponentFactory>,
    pub options: HostedOptions,
    pub commands: watch::Receiver<Command>,
    pub events: mpsc::Sender<Event>,
    pub status: watch::Sender<HostStatus>,
}

impl Worker {
    pub(super) async fn run(mut self) -> Option<HostError> {
        let id = self.options.bindings.namespace.clone();
        let context = tokio::select! {
            biased;
            _ = stop_reason(&mut self.commands) => return None,
            result = guarded(async {
                Ok(crate::ModuleContext::open_hosted(self.options).await?)
            }, &id, HostErrorCode::ContextOpen) => result,
        };
        let context = match context {
            Ok(inner) => Arc::new(ComponentContext { inner }),
            Err(error) => {
                if self.events.send(Event::Failure(error)).await.is_ok() {
                    stop_reason(&mut self.commands).await;
                }
                return None;
            }
        };
        let mut monitor = Monitor {
            commands: self.commands,
            hosted: context.inner.hosted_events(),
        };
        if self
            .events
            .send(Event::Opened(context.clone()))
            .await
            .is_err()
        {
            context.inner.shutdown_now();
            return None;
        }
        let constructed = monitor
            .during(guarded(
                async { self.factory.create(context.config_table()) },
                &id,
                HostErrorCode::Factory,
            ))
            .await;
        let component = match constructed {
            Ok(component) => component,
            Err(interrupted) => {
                finish_interruption(interrupted, &id, &self.events, &mut monitor.commands).await;
                context.inner.shutdown_now();
                return None;
            }
        };
        Lifecycle {
            context,
            monitor,
            events: self.events,
            status: self.status,
        }
        .run(component)
        .await
    }
}

struct Lifecycle {
    context: Arc<ComponentContext>,
    monitor: Monitor,
    events: mpsc::Sender<Event>,
    status: watch::Sender<HostStatus>,
}

impl Lifecycle {
    async fn run(mut self, mut component: Box<dyn Component>) -> Option<HostError> {
        let context = &self.context;
        let id = context.id();
        let monitor = &mut self.monitor;
        phase(&self.status, id, ComponentPhase::Preparing);
        let lifecycle = async {
            monitor
                .during(guarded(
                    async { component.prepare(context).await },
                    id,
                    HostErrorCode::Prepare,
                ))
                .await?;
            let prepared = monitor
                .during(guarded(
                    async { Ok(context.inner.prepare_admission()?) },
                    id,
                    HostErrorCode::Admission,
                ))
                .await?;
            phase(&self.status, id, ComponentPhase::Prepared);
            if self
                .events
                .send(Event::Prepared(id.to_owned(), prepared))
                .await
                .is_err()
            {
                return Err(Interrupted::Stop(StopReason::Requested));
            }
            monitor.activation().await?;
            phase(&self.status, id, ComponentPhase::Running);
            monitor
                .during(guarded(
                    async { component.run(context).await },
                    id,
                    HostErrorCode::Run,
                ))
                .await?;
            Err(Interrupted::Failure(HostError::new(
                HostErrorCode::UnexpectedExit,
                Some(id),
                "run returned before shutdown",
            )))
        }
        .await;
        let interrupted = match lifecycle {
            Err(interrupted) => interrupted,
            Ok(()) => Interrupted::Stop(StopReason::Requested),
        };
        let panicked = matches!(
            &interrupted, Interrupted::Failure(error) if error.code == HostErrorCode::Panic
        );
        let reason =
            finish_interruption(interrupted, id, &self.events, &mut monitor.commands).await;
        context.inner.shutdown_now();
        if panicked {
            // Component state is not reusable after unwind.
            return None;
        }
        phase(&self.status, id, ComponentPhase::Stopping);
        guarded(
            async { component.stop(context, reason).await },
            id,
            HostErrorCode::StopHook,
        )
        .await
        .err()
    }
}
