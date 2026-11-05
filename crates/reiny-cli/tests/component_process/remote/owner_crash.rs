//! Owner crash retains remote uncertainty while fixture children are reaped.
use super::*;

pub(super) async fn owner_crash(hosts: &Hosts, ssh: &Ssh, endpoint: &str) -> Result<()> {
    eprintln!("SSH CASE owner-crash");
    let source = hosts
        .example
        .parent()
        .and_then(Path::parent)
        .context("fixture source")?;
    let target = hosts.binaries.parent().context("fixture target")?;
    let output = tokio::time::timeout(
        std::time::Duration::from_mins(5),
        tokio::process::Command::new("cargo")
            .args([
                "build",
                "--offline",
                "-p",
                "reiny-launch",
                "--example",
                "ssh_owner",
                "--manifest-path",
            ])
            .arg(source.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(target)
            .env("CARGO_PROFILE_DEV_DEBUG", "0")
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        output.status.success(),
        "owner fixture build: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut events = Events::listen().await?;
    let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
    fixture.prepare().await?;
    let prepared_path = fixture.path().join("prepared.json");
    let prepared = PreparedDeployment::load_file(&prepared_path)?;
    let targets = ssh.targets(&prepared, fixture.path())?;
    let target_file = fixture.path().join("ssh.json");
    std::fs::write(&target_file, serde_json::to_vec(&targets)?)?;
    let mut child = tokio::process::Command::new(hosts.binaries.join("examples/ssh_owner"))
        .arg(&prepared_path)
        .arg(&target_file)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let mut lines = BufReader::new(child.stdout.take().context("owner stdout")?).lines();
    let line = tokio::time::timeout(DEADLINE, lines.next_line()).await??;
    ensure!(
        line.as_deref() == Some("OWNER_LISTENING"),
        "owner did not listen"
    );
    let client = DeploymentClient::find(fixture.path())?.context("owner endpoint")?;
    let waiter = client.clone();
    let state = tokio::task::spawn_blocking(move || waiter.wait_ready(DEADLINE)).await??;
    let mut native_exits = Vec::new();
    for role in ["sender", "receiver"] {
        let event = events.wait(role, "delivered").await?;
        native_exits.push(remote_pid::Exit::subscribe(
            event["pid"].as_u64().context("native PID")?,
        )?);
    }
    let mut client_exits = Vec::new();
    for host in state.component_hosts.values() {
        let pid = u64::from(host.pid.context("SSH client PID")?);
        client_exits.push((pid, remote_pid::Exit::subscribe(pid)?));
    }
    let before = reiny_launch::remote_ownership(fixture.path())?;
    assert_eq!(before.len(), 2);
    child.kill().await?;
    child.wait().await?;
    for exit in native_exits {
        exit.exited().await?;
    }
    for (pid, exit) in client_exits {
        exit.exited().await?;
        remote_pid::reap_adopted(pid)?;
    }
    let retained = reiny_launch::remote_ownership(fixture.path())?;
    assert_eq!(before, retained);
    assert!(retained.iter().all(|record| !record.local_client_reaped
        && record.cooperative_outcome.is_none()
        && record.remote_command_exit.is_none()
        && record.tree_cleanup == reiny_launch::RemoteTreeCleanup::Unknown));
    assert!(reiny_launch::serve(prepared, &Observer, &AtomicBool::new(true), &|| Ok(())).is_err());
    for record in retained {
        reiny_launch::reconcile_remote(
            fixture.path(),
            &record.identity,
            &record.target_digest,
            "fixture separately waited on owned native and SSH-client pidfds",
        )?;
    }
    eprintln!("SSH CASE owner-crash: passed");
    Ok(())
}
