use super::{Fault, Fixture};
use anyhow::{Context, Result};
use reiny_launch::{DeploymentPlan, PreparedDeployment};

pub(super) fn build(
    fault: Fault,
    suspend: bool,
    address: std::net::SocketAddr,
) -> Result<(tempfile::TempDir, PreparedDeployment)> {
    let executable = std::env::current_exe()?;
    let root = tempfile::Builder::new()
        .prefix("hosted-owner-")
        .tempdir_in(executable.parent().context("test executable parent")?)?;
    std::fs::create_dir(root.path().join("bin"))?;
    std::fs::copy(
        std::env::current_exe()?,
        root.path()
            .join("bin")
            .join(format!("peer{}", std::env::consts::EXE_SUFFIX)),
    )?;
    std::fs::write(
        root.path().join("peer.json"),
        serde_json::to_vec(&Fixture { address, fault })?,
    )?;
    for name in ["left", "right"] {
        std::fs::create_dir(root.path().join(name))?;
        std::fs::write(
            root.path().join(name).join("main.yaml"),
            "version: 4\ncomponent: {factory: peer, factory_revision: '1'}\nout: {value: {type: wire.Value}}\n",
        )?;
    }
    let domain = root
        .path()
        .file_name()
        .context("fixture root name")?
        .to_string_lossy();
    let policy = if suspend {
        "suspend_deployment"
    } else {
        "report"
    };
    let graph_input = if matches!(fault, Fault::GraphSchema) {
        std::fs::write(
            root.path().join("right/main.yaml"),
            "version: 4\ncomponent: {factory: peer, factory_revision: '1'}\nin: {value: {type: wire.Value}}\nout: {value: {type: wire.Value}}\n",
        )?;
        ", in: {value: {from: left.value}}"
    } else {
        ""
    };
    std::fs::write(
        root.path().join("main.yaml"),
        format!(
            r"version: 4
deployment: wire
domain: {domain}
providers:
  local: {{type: process, bin_dir: bin}}
component_hosts:
  failing: {{provider: local, bin: peer, on_failure: {policy}}}
  healthy: {{provider: local, bin: peer}}
placement:
  wire/left: failing
  wire/right: healthy
modules:
  left: {{source: left}}
  right: {{source: right{graph_input}}}
"
        ),
    )?;
    let prepared = DeploymentPlan::load(root.path(), false)?.prepare()?;
    Ok((root, prepared))
}
