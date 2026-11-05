use super::{
    Arc, Command, Context, DeploymentPhase, ErrorKind, File, Instant, OwnedProcess, Owner,
    PreparedDeployment, PreparedModule, ProcessObservation, Result, Stdio, atomic_json, ensure,
    validate_prepared,
};

impl Owner<'_> {
    pub(super) fn remove_endpoint(&self) -> Result<()> {
        match std::fs::remove_file(self.cache.join("control.json")) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| {
                format!(
                    "removing owner endpoint {}",
                    self.cache.join("control.json").display()
                )
            }),
        }
    }

    pub(super) fn apply(&mut self, mut prepared: PreparedDeployment) -> Result<()> {
        prepared.root = std::fs::canonicalize(&prepared.root)?;
        validate_prepared(&prepared)?;
        ensure!(
            prepared.root == self.desired.root
                && prepared.deployment == self.desired.deployment
                && prepared.domain == self.desired.domain,
            "apply cannot change owner root, deployment, or domain"
        );
        if self.hosted.is_some() || !prepared.components.is_empty() {
            if self.identical_hosted(&prepared)? {
                return Ok(());
            }
            ensure!(
                self.remote.is_empty(),
                "remote apply requires stopping and exact-generation reconciliation"
            );
            self.stop_all()?;
            super::remote::refuse_unresolved(&self.desired.root)?;
            self.hosted = None;
        }
        let removed: Vec<_> = self
            .children
            .keys()
            .filter(|name| {
                !prepared.nodes.iter().any(|node| {
                    &node.namespace == *name
                        && self
                            .status
                            .modules
                            .get(*name)
                            .and_then(|status| status.actual_fingerprint.as_ref())
                            == Some(&node.fingerprint)
                })
            })
            .cloned()
            .collect();
        self.stop_names(&removed)?;
        // Stop with the old observer registry: removed namespaces and replaced
        // providers may no longer be reachable through the new configuration.
        self.observer.configure(&prepared)?;
        atomic_json(&self.cache.join("prepared.json"), &prepared)?;
        self.desired = prepared;
        self.install_desired_status();
        self.retries.clear();
        self.attempts.clear();
        self.status.error = None;
        self.status.phase = DeploymentPhase::Starting;
        self.reconcile()?;
        self.observe()?;
        self.publish()
    }

    pub(super) fn reconcile(&mut self) -> Result<()> {
        if !self.desired.components.is_empty() {
            return self.start_hosted();
        }
        // Clone the small desired descriptions so spawning can mutate ownership.
        let missing: Vec<_> = self
            .desired
            .nodes
            .iter()
            .filter(|node| {
                !self.children.contains_key(&node.namespace)
                    && !self
                        .status
                        .modules
                        .get(&node.namespace)
                        .is_some_and(|status| status.phase == DeploymentPhase::Completed)
            })
            .cloned()
            .collect();
        for node in missing {
            if let Err(error) = self.spawn(&node) {
                self.failed(&node.namespace, &format!("{error:#}"), false)?;
                if self.status.phase == DeploymentPhase::Suspended {
                    break;
                }
            }
        }
        Ok(())
    }

    pub(super) fn spawn(&mut self, node: &PreparedModule) -> Result<()> {
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .context("process generation exhausted")?;
        let generation = self.next_generation;
        let directory = self
            .cache
            .join("generations")
            .join(&self.status.owner_generation)
            .join(generation.to_string());
        std::fs::create_dir_all(&directory)?;
        let bindings_path = directory.join("bindings.json");
        let report_path = directory.join("report.json");
        atomic_json(&bindings_path, &node.bindings)?;
        let stdout = File::create(directory.join("stdout.log"))?;
        let stderr = File::create(directory.join("stderr.log"))?;
        let mut command = Command::new(&node.executable);
        command
            .current_dir(&node.module_dir)
            .args(&node.run.args)
            .arg("--name")
            .arg(&node.namespace)
            .arg("--domain")
            .arg(&self.desired.domain)
            .arg("--module-bindings")
            .arg(&bindings_path)
            .env("REINY_MODULE_REPORT", &report_path)
            .env("REINY_BUNDLE_DIR", &node.bundle_dir)
            .env_remove("REINY_CONFIG_DIR")
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr);
        if let Some(directory) = &node.config_dir {
            command.env("REINY_CONFIG_DIR", directory);
        }
        if let Some(config) = &node.run.config {
            command.arg("--config").arg(config);
        }
        if let Some(config) = &node.provider.zenoh_config {
            command.arg("--zenoh-config").arg(config);
        }
        for endpoint in &node.provider.connect {
            command.arg("--connect").arg(endpoint);
        }
        let process = OwnedProcess::spawn(
            command,
            ProcessObservation::Module {
                report_path,
                started: Instant::now(),
            },
            Arc::clone(&self.runtime),
        )
        .with_context(|| format!("spawning module {}", node.namespace))?;
        let pid = process
            .child
            .id()
            .context("spawned child has no process ID")?;
        self.children.insert(node.namespace.clone(), process);
        let status = self
            .status
            .modules
            .get_mut(&node.namespace)
            .context("missing desired module status")?;
        status.pid = Some(pid);
        status.generation = generation;
        status.actual_fingerprint = Some(node.fingerprint.clone());
        status.phase = DeploymentPhase::Starting;
        status.error = None;
        status.report = None;
        Ok(())
    }
}
