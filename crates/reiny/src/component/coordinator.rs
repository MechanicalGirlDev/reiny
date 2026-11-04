use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::{Id, JoinError, JoinSet};

use crate::bindings::{ModuleBindings, ReportEntry, validate_graph};
use crate::managed::{HostedEvent, PreparedAdmission, ReadyAdmission};

use super::status::phase;
use super::worker::{Command, Event};
use super::{
    ComponentContext, ComponentPhase, HostError, HostErrorCode, HostOptions, HostOutcome,
    HostPhase, HostStatus, StopReason,
};

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
    pub options: HostOptions,
}

impl Coordinator {
    pub(super) async fn run(mut self) {
        let startup = tokio::time::timeout(self.options.startup_timeout, self.start()).await;
        let (primary, reason) = match startup {
            Ok(Ok(true)) => (self.serve().await, StopReason::ComponentFailure),
            Ok(Ok(false)) => (None, StopReason::Requested),
            Ok(Err(error)) => (Some(error), StopReason::StartupFailure),
            Err(_) => (
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

    async fn start(&mut self) -> Result<bool, HostError> {
        while self.prepared.len() < self.bindings.len() {
            tokio::select! {
                biased;
                _ = async { self.stop.wait_for(|stop| *stop).await.map(drop) } => return Ok(false),
                event = self.events.recv() => {
                    if self.observe(event)? {
                        return Ok(false);
                    }
                },
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    if let Some(joined) = joined {
                        return Err(self.early_exit(joined));
                    }
                }
            }
        }
        let entries = self
            .bindings
            .iter()
            .map(|bindings| {
                self.prepared
                    .get(&bindings.namespace)
                    .map(|ticket| ReportEntry {
                        bindings,
                        report: ticket.report(),
                    })
                    .ok_or_else(|| {
                        HostError::new(
                            HostErrorCode::Admission,
                            Some(&bindings.namespace),
                            "missing prepared ticket",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_graph(&entries).map_err(|report| HostError {
            code: HostErrorCode::Graph,
            instance: report.module.clone(),
            message: report.to_string(),
            report: Some(Box::new(report)),
        })?;
        self.status
            .send_modify(|status| status.phase = HostPhase::Prepared);
        for bindings in &self.bindings {
            if *self.stop.borrow() {
                return Ok(false);
            }
            let id = &bindings.namespace;
            let context = self.contexts.get(id).ok_or_else(|| {
                HostError::new(HostErrorCode::Admission, Some(id), "missing hosted context")
            })?;
            let ticket = self.prepared.remove(id).ok_or_else(|| {
                HostError::new(
                    HostErrorCode::Admission,
                    Some(id),
                    "missing prepared ticket",
                )
            })?;
            let ready = context.inner.commit_admission(ticket).map_err(|error| {
                HostError::new(HostErrorCode::Activate, Some(id), error.to_string())
            })?;
            self.ready.push(ready);
            phase(&self.status, id, ComponentPhase::Ready);
        }
        if *self.stop.borrow() {
            return Ok(false);
        }
        // A prior commit may have lost its port while a later commit ran. Check
        // retained events, not just queued worker notifications, before activation.
        for (id, context) in &self.contexts {
            match context.inner.hosted_events().borrow().as_ref() {
                Some(HostedEvent::ReadinessLost) => {
                    return Err(HostError::new(
                        HostErrorCode::ReadinessLost,
                        Some(id),
                        "readiness was lost before group activation",
                    ));
                }
                Some(HostedEvent::StopRequested) => return Ok(false),
                None => {}
            }
        }
        // A loss after this check can race another worker's first run poll.
        // Each worker monitors loss; serve() then stops the entire group.
        self.status
            .send_modify(|status| status.phase = HostPhase::Running);
        self.commands.send_replace(Command::Run);
        Ok(true)
    }

    async fn serve(&mut self) -> Option<HostError> {
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
