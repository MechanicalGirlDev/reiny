use super::{
    Context, DeploymentPhase, Duration, ErrorKind, FailurePolicy, Instant, ModuleReport, Owner,
    ProcessObservation, RestartPolicy, Result, RunKind, START_GRACE, bail, validate_report,
};

impl Owner<'_> {
    pub(super) fn observe(&mut self) -> Result<()> {
        if self.hosted.is_some() {
            return self.observe_hosted();
        }
        match self.status.phase {
            DeploymentPhase::Suspended
            | DeploymentPhase::Stopping
            | DeploymentPhase::Stopped
            | DeploymentPhase::Completed
            | DeploymentPhase::Failed => {
                return Ok(());
            }
            DeploymentPhase::Starting | DeploymentPhase::Ready | DeploymentPhase::Degraded => {}
        }
        let names: Vec<_> = self.children.keys().cloned().collect();
        for name in names {
            self.observe_process(&name)?;
            if self.status.phase == DeploymentPhase::Suspended {
                return Ok(());
            }
        }
        let due: Vec<_> = self
            .retries
            .iter()
            .filter(|(_, deadline)| Instant::now() >= **deadline)
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            self.retries.remove(&name);
            let node = self
                .desired
                .nodes
                .iter()
                .find(|node| node.namespace == name)
                .context("missing restart module")?
                .clone();
            if let Err(error) = self.spawn(&node) {
                self.failed(&name, &format!("{error:#}"), true)?;
            }
        }
        let all_ready = self.status.modules.values().all(|status| {
            matches!(
                status.phase,
                DeploymentPhase::Ready | DeploymentPhase::Completed
            )
        });
        if all_ready {
            if let Err(error) = self.validate_connections() {
                self.status.error = Some(format!("{error:#}"));
                let suspend = self
                    .desired
                    .nodes
                    .iter()
                    .any(|node| node.run.on_failure == FailurePolicy::SuspendDeployment);
                self.stop_all()?;
                self.status.phase = if suspend {
                    DeploymentPhase::Suspended
                } else {
                    DeploymentPhase::Failed
                };
            } else {
                self.status.phase = if !self.status.modules.is_empty()
                    && self
                        .status
                        .modules
                        .values()
                        .all(|status| status.phase == DeploymentPhase::Completed)
                {
                    DeploymentPhase::Completed
                } else {
                    DeploymentPhase::Ready
                };
            }
        } else if self
            .status
            .modules
            .values()
            .any(|status| status.phase == DeploymentPhase::Failed)
        {
            if self.retries.is_empty()
                && self.status.modules.values().all(|status| {
                    matches!(
                        status.phase,
                        DeploymentPhase::Failed | DeploymentPhase::Completed
                    )
                })
            {
                self.status.phase = DeploymentPhase::Failed;
                self.status.error = self
                    .status
                    .modules
                    .values()
                    .find_map(|status| status.error.clone());
            } else {
                self.status.phase = DeploymentPhase::Degraded;
            }
        } else {
            self.status.phase = DeploymentPhase::Starting;
        }
        Ok(())
    }

    pub(super) fn observe_process(&mut self, name: &str) -> Result<()> {
        let process = self
            .children
            .get_mut(name)
            .context("missing owned process")?;
        if let Some(exit) = process.child.inner_mut().inner_mut().try_wait()? {
            let success = exit.success();
            process.terminate()?;
            self.children.remove(name);
            let node = self
                .desired
                .nodes
                .iter()
                .find(|node| node.namespace == name)
                .context("missing desired module")?;
            if node.run.kind == RunKind::Task && success {
                let status = self
                    .status
                    .modules
                    .get_mut(name)
                    .context("missing task status")?;
                status.phase = DeploymentPhase::Completed;
                status.pid = None;
                status.actual_fingerprint = None;
                status.error = None;
                return Ok(());
            }
            let restart = match node.run.restart {
                RestartPolicy::Manual => false,
                RestartPolicy::OnFailure | RestartPolicy::Always => true,
            };
            return self.failed(name, &format!("process exited: {exit}"), restart);
        }
        let (path, started) = match &process.observation {
            ProcessObservation::Module {
                report_path,
                started,
            } => (report_path.clone(), *started),
            ProcessObservation::Hosted => bail!("hosted process entered legacy observation"),
        };
        let ready = self.observer.ready(name)?;
        let report = match std::fs::read(&path) {
            Ok(bytes) => Some(
                serde_json::from_slice::<ModuleReport>(&bytes)
                    .context("invalid module readiness report"),
            ),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(report) = report {
            let node = self
                .desired
                .nodes
                .iter()
                .find(|node| node.namespace == name)
                .context("missing desired module")?;
            match report.and_then(|report| {
                validate_report(node, &report)?;
                Ok(report)
            }) {
                Ok(report) => {
                    let status = self
                        .status
                        .modules
                        .get_mut(name)
                        .context("missing module status")?;
                    status.report = Some(report);
                    status.phase = if ready {
                        DeploymentPhase::Ready
                    } else {
                        DeploymentPhase::Starting
                    };
                    if started.elapsed() >= Duration::from_mins(1) {
                        self.attempts.remove(name);
                    }
                }
                Err(error) => {
                    self.stop_names(&[name.to_owned()])?;
                    return self.failed(name, &format!("{error:#}"), false);
                }
            }
        } else if started.elapsed() >= START_GRACE {
            self.stop_names(&[name.to_owned()])?;
            return self.failed(name, "application readiness report timed out", false);
        }
        if started.elapsed() >= START_GRACE
            && self
                .status
                .modules
                .get(name)
                .is_some_and(|status| status.phase == DeploymentPhase::Starting)
        {
            self.stop_names(&[name.to_owned()])?;
            self.failed(name, "application readiness timed out", false)?;
        }
        Ok(())
    }
}
