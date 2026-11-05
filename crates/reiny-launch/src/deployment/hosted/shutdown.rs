use protocol::{HostOutcome, Milliseconds};

use super::{
    Admission, BTreeSet, Context, DeploymentPhase, HostPhase, Hosted, Instant, Owner, OwnerMessage,
    Phase, Result, STOP_GRACE, Session, StopReason, ensure, protocol, status,
};

impl Session {
    pub(super) fn outcome(&mut self, owner: &mut Owner<'_>, outcome: HostOutcome) -> Result<()> {
        let status = owner
            .status
            .component_hosts
            .get_mut(&self.identity.host_id.0)
            .context("missing host status")?;
        ensure!(status.outcome.is_none(), "duplicate terminal host outcome");
        match &outcome {
            HostOutcome::Stopped {} | HostOutcome::Failed { .. } => {}
            HostOutcome::Unresponsive { instances, .. } => {
                let unique: BTreeSet<_> = instances.iter().collect();
                ensure!(
                    unique.len() == instances.len()
                        && instances.iter().all(|namespace| self
                            .assignments
                            .iter()
                            .any(|entry| &entry.bindings.namespace == namespace)),
                    "invalid unresponsive instance set"
                );
            }
        }
        status.outcome = Some(outcome.clone());
        if let Some(record) = &mut self.remote {
            record.cooperative_outcome = Some(outcome);
            super::super::remote::persist(&owner.desired.root, record, false)?;
        }
        let stopping = self.phase == Phase::Stopping;
        self.phase = Phase::Terminal;
        if self.remote.is_some() {
            // SSH waits for forwarded channels even after command exit. Once
            // the terminal outcome is authenticated and retained, close our
            // half instead of forcing a healthy client to hit the kill deadline.
            self.channel = None;
        }
        self.stop_deadline
            .get_or_insert_with(|| Instant::now() + STOP_GRACE);
        if !stopping {
            self.failure
                .get_or_insert_with(|| "unexpected host terminal outcome".to_owned());
        }
        Ok(())
    }
}

impl Hosted {
    pub(super) fn fail(&mut self, owner: &mut Owner<'_>, name: &str, error: &str) -> Result<()> {
        let status = owner
            .status
            .component_hosts
            .get_mut(name)
            .context("failed host status missing")?;
        if status.error.is_none() {
            status.error = Some(error.to_owned());
        } else if !status
            .cleanup_errors
            .iter()
            .any(|existing| existing == error)
        {
            status.cleanup_errors.push(error.to_owned());
        }
        for instance in owner
            .status
            .components
            .values_mut()
            .filter(|instance| instance.host == name)
        {
            instance.error.get_or_insert_with(|| error.to_owned());
        }
        owner
            .status
            .error
            .get_or_insert_with(|| format!("{name}: {error}"));
        match self.admission {
            Admission::Preparing | Admission::Committing | Admission::Activating => {
                self.stop_all(owner, StopReason::AdmissionAborted, DeploymentPhase::Failed)
            }
            Admission::Active => {
                let policy = owner
                    .desired
                    .component_hosts
                    .get(name)
                    .context("failed host missing")?
                    .spec
                    .on_failure;
                match policy {
                    crate::FailurePolicy::Report => {
                        owner.status.phase = DeploymentPhase::Degraded;
                        self.stop_one(owner, name)
                    }
                    crate::FailurePolicy::SuspendDeployment => self.stop_all(
                        owner,
                        StopReason::DeploymentFailure,
                        DeploymentPhase::Suspended,
                    ),
                }
            }
            Admission::Aborting(_) | Admission::Finished => Ok(()),
        }
    }

    pub(super) fn stop_all(
        &mut self,
        owner: &mut Owner<'_>,
        reason: StopReason,
        final_phase: DeploymentPhase,
    ) -> Result<()> {
        self.admission = Admission::Aborting(final_phase);
        owner.status.phase = DeploymentPhase::Stopping;
        let deadline = Instant::now() + STOP_GRACE;
        let names: Vec<_> = owner.desired.component_hosts.keys().cloned().collect();
        for name in names {
            self.request_stop(owner, &name, (reason, deadline))?;
        }
        Ok(())
    }

    fn stop_one(&mut self, owner: &mut Owner<'_>, name: &str) -> Result<()> {
        self.request_stop(
            owner,
            name,
            (StopReason::DeploymentFailure, Instant::now() + STOP_GRACE),
        )
    }

    fn request_stop(
        &mut self,
        owner: &mut Owner<'_>,
        name: &str,
        stop: (StopReason, Instant),
    ) -> Result<()> {
        let Some(session) = self.sessions.get_mut(name) else {
            status::phase(owner, name, HostPhase::Stopped);
            return Ok(());
        };
        if session.stop_deadline.is_some() {
            return Ok(());
        }
        session.stop_deadline = Some(stop.1);
        session.listener = None;
        session.nonce = None;
        session.phase = Phase::Stopping;
        status::phase(owner, name, HostPhase::Stopping);
        if let Some(channel) = &mut session.channel {
            let result = channel.queue(OwnerMessage::Stop {
                reason: stop.0,
                remaining_budget: Milliseconds(u64::try_from(
                    stop.1.saturating_duration_since(Instant::now()).as_millis(),
                )?),
            });
            if let Err(error) = result {
                owner
                    .status
                    .component_hosts
                    .get_mut(name)
                    .context("stopping host status missing")?
                    .cleanup_errors
                    .push(format!("stop delivery failed: {error:#}"));
                session.channel = None;
            } else {
                session.stop_reason = Some(stop.0);
            }
        }
        Ok(())
    }

    pub(super) fn observe_exit(&mut self, owner: &mut Owner<'_>, name: &str) -> Result<()> {
        let session = self.sessions.get(name).context("missing host session")?;
        let process = owner.children.get_mut(name).context("missing owned host")?;
        let exit = process.child.inner_mut().inner_mut().try_wait()?;
        let expired = session
            .stop_deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if exit.is_none() && !expired {
            return Ok(());
        }
        let unexpected = session.stop_deadline.is_none();
        let remote = session.remote.is_some();
        let verified = session.gate.as_ref().is_some_and(|gate| gate.verified);
        let description = exit.map_or_else(
            || "owned process tree killed after shutdown deadline".to_owned(),
            |exit| format!("process exited: {exit}"),
        );
        // Always terminate/reap the tree, even when the process leader exited.
        process.terminate()?;
        owner.children.remove(name);
        if let Some(record) = self
            .sessions
            .get_mut(name)
            .and_then(|session| session.remote.as_mut())
        {
            record.local_client_reaped = true;
            record.remote_command_exit = exit
                .and_then(|exit| exit.code())
                .filter(|code| verified && *code != 255);
            super::super::remote::persist(&owner.desired.root, record, false)?;
        }
        if unexpected {
            self.fail(owner, name, &description)?;
        }
        let status = owner
            .status
            .component_hosts
            .get_mut(name)
            .context("missing exited host status")?;
        status.pid = None;
        status.exit = Some(if remote {
            format!("local SSH client: {description}; remote tree cleanup Unknown")
        } else {
            description
        });
        let phase = match &status.outcome {
            Some(HostOutcome::Stopped {})
                if status.error.is_none() && exit.is_some_and(|exit| exit.success()) =>
            {
                HostPhase::Stopped
            }
            Some(HostOutcome::Stopped {} | HostOutcome::Failed { .. }) => HostPhase::Failed,
            Some(HostOutcome::Unresponsive { .. }) | None => HostPhase::Unresponsive,
        };
        if status.outcome.is_none() {
            status.error.get_or_insert_with(|| {
                if remote {
                    "remote task cleanup unconfirmed; only local SSH client tree reaped".to_owned()
                } else {
                    "local task cleanup unconfirmed; owned process tree reaped".to_owned()
                }
            });
        }
        if remote {
            status.cleanup_errors.push(
                "remote tree cleanup Unknown; SSH exit is not descendant containment".to_owned(),
            );
        }
        status::phase(
            owner,
            name,
            if remote {
                HostPhase::Unresponsive
            } else {
                phase
            },
        );
        let session = self
            .sessions
            .get_mut(name)
            .context("missing reaped host session")?;
        session.channel = None;
        session.listener = None;
        session.gate = None;
        session.phase = Phase::Terminal;
        Ok(())
    }
}
