//! Real SDK cleanup when the foreground lease or exact owner process disappears.

use std::process::Stdio;

use anyhow::{Context, Result, ensure};
use reiny_launch::{DeploymentClient, DeploymentPhase, PreparedDeployment};

use super::fixture::{DEADLINE, Events, Fixture, Hosts};

const BIN: &str = env!("CARGO_BIN_EXE_reiny");

pub(super) async fn startup_stdin_eof(hosts: &Hosts, endpoint: &str) -> Result<()> {
    let mut events = Events::listen().await?;
    let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
    fixture.block_prepare("receiver")?;
    fixture.prepare().await?;
    let mut apply = tokio::process::Command::new(BIN)
        .args(["apply", "--prepared"])
        .arg(fixture.path().join("prepared.json"))
        .args(["--control-stdin", "--ready-timeout", "25"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    events.wait("receiver", "prepared").await?;
    events.wait("sender", "prepared").await?;
    let starting = fixture.status().await?;
    assert_eq!(starting["phase"], "starting");
    // EOF revokes the CLI's foreground lease while the owner is still preparing.
    drop(apply.stdin.take().context("controlled stdin")?);
    let output = tokio::time::timeout(DEADLINE, apply.wait_with_output())
        .await
        .context("controlled apply did not reap its owner")??;
    ensure!(
        !output.status.success(),
        "controlling stdin EOF was ignored"
    );
    let status = fixture.status().await?;
    super::assert_reaped(&status)?;
    assert_eq!(status["owner_alive"], false);
    for role in ["sender", "receiver"] {
        ensure!(
            !fixture.path().join(format!("{role}.ran")).exists(),
            "{role} entered run after startup cancellation"
        );
    }
    events.assert_no_run()?;
    Ok(())
}

pub(super) async fn owner_process_eof(hosts: &Hosts, endpoint: &str) -> Result<()> {
    let mut events = Events::listen().await?;
    let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
    fixture.prepare().await?;
    let prepared = PreparedDeployment::load_file(&fixture.path().join("prepared.json"))?;
    // The hidden supervisor's input is the verified raw desired state, not the
    // public envelope. This test owns its actual Child, never signals a status PID.
    let desired = fixture.path().join("owner-input.json");
    std::fs::write(&desired, serde_json::to_vec(&prepared)?)?;
    let mut owner = tokio::process::Command::new(BIN)
        .arg("__supervise")
        .arg(&desired)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    events.wait("sender", "run").await?;
    events.wait("receiver", "run").await?;
    let client = DeploymentClient::find(fixture.path())?.context("owned supervisor endpoint")?;
    let ready = tokio::task::spawn_blocking(move || client.wait_ready(DEADLINE)).await??;
    assert_eq!(ready.phase, DeploymentPhase::Ready);
    owner.start_kill()?;
    tokio::time::timeout(DEADLINE, owner.wait())
        .await
        .context("owner reap deadline")??;
    // A dead owner cannot reap or publish a new child outcome. The real SDK
    // control threads observe EOF, withdraw readiness, and run local cleanup.
    events.wait("sender", "stop").await?;
    events.wait("receiver", "stop").await?;
    ensure!(
        DeploymentClient::find(fixture.path())?.is_none(),
        "killed owner remained live"
    );
    let retained =
        reiny_launch::last_status(fixture.path())?.context("retained owner observation")?;
    assert!(!retained.owner_alive);
    assert!(
        retained
            .component_hosts
            .values()
            .all(|host| host.pid.is_some())
    );
    Ok(())
}
