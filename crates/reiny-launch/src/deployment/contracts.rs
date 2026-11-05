use super::{
    Context, DeploymentPhase, Duration, FailurePolicy, Instant, Owner, PortEndpoint, Replay,
    Result, Retention, ensure,
};

impl Owner<'_> {
    pub(super) fn validate_connections(&self) -> Result<()> {
        for node in &self.desired.nodes {
            if node.bindings.inputs.is_empty() {
                continue;
            }
            let report = self
                .status
                .modules
                .get(&node.namespace)
                .and_then(|status| status.report.as_ref())
                .context("missing ready module report")?;
            for (port, input) in &node.bindings.inputs {
                let actual = report.inputs.get(port).context("missing actual input")?;
                ensure!(
                    !input.sources.is_empty(),
                    "input {}/{port} has no sources",
                    node.namespace
                );
                for address in &input.sources {
                    let namespace = &address.namespace;
                    let source = self
                        .status
                        .modules
                        .get(namespace)
                        .and_then(|status| status.report.as_ref())
                        .with_context(|| {
                            format!(
                                "input {}/{port} has no ready source {namespace}",
                                node.namespace
                            )
                        })?;
                    let output = source.outputs.get(&address.port).with_context(|| {
                        format!(
                            "source {namespace} does not implement output {}",
                            address.port
                        )
                    })?;
                    ensure!(
                        output.endpoint
                            == PortEndpoint::Output {
                                address: address.clone()
                            },
                        "source output identity mismatch for {namespace}/{}",
                        address.port
                    );
                    ensure!(
                        actual.type_name == output.type_name
                            && actual.schema == output.schema
                            && actual.response == output.response,
                        "schema mismatch for {}/{port} from {namespace}/{}",
                        node.namespace,
                        address.port
                    );
                    ensure!(
                        actual.contract.kind == output.contract.kind
                            && (actual.contract.replay != Replay::Last
                                || output.contract.retention == Retention::Last),
                        "effective policy mismatch for {}/{port} from {namespace}/{}",
                        node.namespace,
                        address.port
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) fn failed(&mut self, name: &str, error: &str, restart: bool) -> Result<()> {
        let status = self
            .status
            .modules
            .get_mut(name)
            .context("missing failed module status")?;
        status.phase = DeploymentPhase::Failed;
        status.pid = None;
        status.actual_fingerprint = None;
        status.report = None;
        status.error = Some(error.to_owned());
        let policy = self
            .desired
            .nodes
            .iter()
            .find(|node| node.namespace == name)
            .context("missing failed desired module")?
            .run
            .on_failure;
        match policy {
            FailurePolicy::SuspendDeployment => {
                self.status.error = Some(format!("{name}: {error}"));
                self.retries.clear();
                self.stop_all()?;
                self.status.phase = DeploymentPhase::Suspended;
            }
            FailurePolicy::Report => {
                if restart {
                    let attempt = self.attempts.entry(name.to_owned()).or_default();
                    let delay =
                        Duration::from_millis(200_u64.saturating_mul(1_u64 << (*attempt).min(7)));
                    *attempt = attempt.saturating_add(1);
                    self.retries.insert(name.to_owned(), Instant::now() + delay);
                }
            }
        }
        Ok(())
    }
}
