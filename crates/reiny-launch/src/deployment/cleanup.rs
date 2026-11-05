use super::{BTreeSet, Context, DeploymentPhase, Instant, Owner, Result, STOP_GRACE, TICK, ensure};

impl Owner<'_> {
    pub(super) fn stop_names(&mut self, names: &[String]) -> Result<()> {
        let mut errors = Vec::new();
        let stopping: Vec<_> = names
            .iter()
            .filter(|name| self.children.contains_key(*name))
            .collect();
        if !stopping.is_empty() {
            self.status.phase = DeploymentPhase::Stopping;
            for name in stopping {
                let status = self
                    .status
                    .modules
                    .get_mut(name)
                    .context("missing stopping module status")?;
                status.phase = DeploymentPhase::Stopping;
            }
            if let Err(error) = self.publish() {
                errors.push(format!("persisting stopping state: {error:#}"));
            }
        }
        for name in names {
            if self.children.contains_key(name)
                && let Err(error) = self.observer.request_stop(name)
            {
                tracing::warn!(%name, %error, "cooperative stop unavailable; enforcing owned-process deadline");
            }
        }
        let deadline = Instant::now() + STOP_GRACE;
        let mut pending: BTreeSet<_> = names
            .iter()
            .filter(|name| self.children.contains_key(*name))
            .cloned()
            .collect();
        while !pending.is_empty() {
            let mut done = Vec::new();
            for name in &pending {
                let process = self
                    .children
                    .get_mut(name)
                    .context("missing stopping process")?;
                // Bypass completion + JobObject and observe only the leader.
                // The native Unix child returns itself at the second layer.
                match process.child.inner_mut().inner_mut().try_wait() {
                    Ok(exit) if exit.is_some() || Instant::now() >= deadline => {
                        done.push(name.clone());
                    }
                    Ok(_) => {}
                    Err(error) => {
                        errors.push(format!("{name}: observing stopping process: {error}"));
                        done.push(name.clone());
                    }
                }
            }
            for name in done {
                let result = self
                    .children
                    .get_mut(&name)
                    .context("missing stopping process")?
                    .terminate();
                pending.remove(&name);
                if let Err(error) = result {
                    let error = format!("{name}: {error:#}");
                    if let Some(status) = self.status.modules.get_mut(&name) {
                        status.phase = DeploymentPhase::Failed;
                        status.error = Some(error.clone());
                    }
                    errors.push(error);
                    continue;
                }
                self.children.remove(&name);
                if let Some(status) = self.status.modules.get_mut(&name) {
                    status.pid = None;
                    status.actual_fingerprint = None;
                    status.report = None;
                    status.phase = DeploymentPhase::Stopped;
                }
            }
            if !pending.is_empty() {
                std::thread::park_timeout(TICK);
            }
        }
        ensure!(
            errors.is_empty(),
            "owned process cleanup failed: {}",
            errors.join("; ")
        );
        Ok(())
    }

    pub(super) fn stop_all(&mut self) -> Result<()> {
        if self.hosted.is_some() {
            return self.stop_hosted();
        }
        self.retries.clear();
        let names: Vec<_> = self.children.keys().cloned().collect();
        self.stop_names(&names)
    }
}
