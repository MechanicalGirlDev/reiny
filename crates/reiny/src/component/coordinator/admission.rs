use tokio::sync::oneshot;

use crate::managed::HostedEvent;

use super::super::staged::{Request, Stage};
use super::{Command, ComponentPhase, Coordinator, HostError, HostErrorCode, HostPhase, phase};

impl Coordinator {
    pub(super) async fn start(&mut self) -> Result<bool, HostError> {
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
                request = self.control.recv() => return reject(request),
            }
        }
        if !self.check_admission()? {
            return Ok(false);
        }
        self.reports.send_replace(Some(
            self.prepared
                .values()
                .map(|ticket| ticket.report().clone())
                .collect(),
        ));
        self.status
            .send_modify(|status| status.phase = HostPhase::Prepared);
        let Some(committed) = self.wait_stage(Stage::Commit).await? else {
            return Ok(false);
        };
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
        if !self.check_admission()? {
            return Ok(false);
        }
        self.status
            .send_modify(|status| status.phase = HostPhase::Committed);
        // A cancelled acknowledgement wait does not cancel admission.
        if committed.send(()).is_err() {
            // The controller may still observe retained status or request stop.
        }
        let Some(activated) = self.wait_stage(Stage::Activate).await? else {
            return Ok(false);
        };
        if !self.check_admission()? {
            return Ok(false);
        }
        // Loss after this check can race a worker's first run poll. Activation
        // acknowledges release, not an atomic distributed transaction.
        if let Some(gate) = &self.step_gate {
            gate.activate();
        }
        self.status
            .send_modify(|status| status.phase = HostPhase::Running);
        self.commands.send_replace(Command::Run);
        if activated.send(()).is_err() {
            // The controller may still observe retained status or request stop.
        }
        Ok(true)
    }

    async fn wait_stage(
        &mut self,
        expected: Stage,
    ) -> Result<Option<oneshot::Sender<()>>, HostError> {
        loop {
            tokio::select! {
                biased;
                _ = async { self.stop.wait_for(|stop| *stop).await.map(drop) } => return Ok(None),
                event = self.events.recv() => {
                    if self.observe(event)? {
                        return Ok(None);
                    }
                },
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    if let Some(joined) = joined {
                        return Err(self.early_exit(joined));
                    }
                }
                request = self.control.recv() => match request {
                    Some(Request::Advance(stage, reply)) if stage == expected => return Ok(Some(reply)),
                    other => return reject(other).map(|_| None),
                },
            }
        }
    }

    fn check_admission(&mut self) -> Result<bool, HostError> {
        if *self.stop.borrow() {
            return Ok(false);
        }
        // Retained context events cover loss that has not reached the worker queue.
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
        while let Ok(event) = self.events.try_recv() {
            if self.observe(Some(event))? {
                return Ok(false);
            }
        }
        if let Some(joined) = self.tasks.try_join_next_with_id() {
            return Err(self.early_exit(joined));
        }
        Ok(true)
    }
}

fn reject(request: Option<Request>) -> Result<bool, HostError> {
    match request {
        Some(Request::Abort(error)) => Err(error),
        Some(Request::Advance(_, _)) => Err(HostError::new(
            HostErrorCode::Admission,
            None,
            "out-of-order host admission command",
        )),
        None => Ok(false),
    }
}
