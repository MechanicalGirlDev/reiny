use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::{Id, JoinError, JoinSet};

use crate::bindings::{ModuleBindings, ModuleReport};
use crate::managed::{PreparedAdmission, ReadyAdmission};

use super::staged::Request;
use super::status::phase;
use super::worker::{Command, Event};
use super::{
    ComponentContext, ComponentPhase, HostError, HostErrorCode, HostOutcome, HostPhase, HostStatus,
    StopReason,
};
use super::{deadline::Deadline, options::HostConfiguration};

mod admission;
mod cleanup;

pub(super) struct Coordinator {
    pub status: watch::Sender<HostStatus>,
    pub stop: watch::Receiver<bool>,
    pub terminal: watch::Sender<Option<HostOutcome>>,
    pub commands: watch::Sender<Command>,
    pub events: mpsc::Receiver<Event>,
    pub tasks: JoinSet<Option<HostError>>,
    pub pending: BTreeMap<Id, String>,
    pub bindings: Vec<ModuleBindings>,
    pub contexts: BTreeMap<String, Arc<ComponentContext>>,
    pub prepared: BTreeMap<String, PreparedAdmission>,
    pub ready: Vec<ReadyAdmission>,
    pub options: HostConfiguration,
    pub control: mpsc::Receiver<Request>,
    pub reports: watch::Sender<Option<Vec<ModuleReport>>>,
    pub step_gate: Option<Arc<super::step::control::Gate>>,
    pub step_driver: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        if let Some(gate) = &self.step_gate {
            gate.stop();
        }
        if let Some(driver) = &self.step_driver {
            driver.abort();
        }
    }
}

impl Coordinator {
    pub(super) async fn run(mut self) {
        let startup = Deadline::after(self.options.startup_timeout)
            .scope(self.start())
            .await;
        let (primary, reason) = match startup {
            Ok(Ok(true)) => (self.serve().await, StopReason::ComponentFailure),
            Ok(Ok(false)) => (None, StopReason::Requested),
            Ok(Err(error)) => (Some(error), StopReason::StartupFailure),
            Err(()) => (
                Some(HostError::new(
                    HostErrorCode::Timeout,
                    None,
                    "startup deadline expired",
                )),
                StopReason::StartupFailure,
            ),
        };
        let reason = if primary.is_none() {
            StopReason::Requested
        } else {
            reason
        };
        let outcome = self.cleanup(primary, reason).await;
        self.contexts.clear();
        let final_phase = match &outcome {
            HostOutcome::Stopped => HostPhase::Stopped,
            HostOutcome::Failed { .. } => HostPhase::Failed,
            HostOutcome::Unresponsive { .. } => HostPhase::Unresponsive,
        };
        self.status.send_modify(|status| status.phase = final_phase);
        self.terminal.send_replace(Some(outcome));
    }

    async fn serve(&mut self) -> Option<HostError> {
        let mut controller_open = true;
        loop {
            tokio::select! {
                biased;
                _ = async { self.stop.wait_for(|stop| *stop).await.map(drop) } => return None,
                event = self.events.recv() => match self.observe(event) {
                    Ok(true) => return None,
                    Ok(false) => {}
                    Err(error) => return Some(error),
                },
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    if let Some(joined) = joined {
                        return Some(self.early_exit(joined));
                    }
                }
                request = self.control.recv(), if controller_open => match request {
                    Some(Request::Abort(error)) => return Some(error),
                    Some(Request::Advance(_, _)) => return Some(HostError::new(
                        HostErrorCode::Admission, None, "host is already running",
                    )),
                    // Local admission drops its controller after release; the
                    // independent public handle continues to own shutdown.
                    None => controller_open = false,
                },
            }
        }
    }

    fn observe(&mut self, event: Option<Event>) -> Result<bool, HostError> {
        match event {
            Some(Event::Opened(context)) => {
                self.contexts.insert(context.id().to_owned(), context);
            }
            Some(Event::Prepared(id, ticket)) => {
                self.prepared.insert(id, ticket);
            }
            Some(Event::Failure(error)) => return Err(error),
            Some(Event::StopRequested) => return Ok(true),
            None => {
                return Err(HostError::new(
                    HostErrorCode::UnexpectedExit,
                    None,
                    "all component event senders exited",
                ));
            }
        }
        Ok(false)
    }

    fn early_exit(&mut self, joined: Result<(Id, Option<HostError>), JoinError>) -> HostError {
        let error = match joined {
            Ok((id, error)) => {
                let instance = self.pending.remove(&id);
                error.unwrap_or_else(|| {
                    HostError::new(
                        HostErrorCode::UnexpectedExit,
                        instance.as_deref(),
                        "component task exited",
                    )
                })
            }
            Err(error) => {
                let instance = self.pending.remove(&error.id());
                HostError::new(HostErrorCode::Panic, instance.as_deref(), error.to_string())
            }
        };
        self.record(&error);
        error
    }

    fn record(&self, error: &HostError) {
        self.status.send_modify(|status| {
            if let Some(component) = error
                .instance
                .as_ref()
                .and_then(|id| status.components.get_mut(id))
            {
                component.phase = ComponentPhase::Failed;
                component.error.get_or_insert_with(|| error.clone());
            }
        });
    }
}
