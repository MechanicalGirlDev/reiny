//! Graph, trust and failure contract scenarios.
use super::owner_crash::owner_crash;
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ssh_graph_and_failure_contracts() -> Result<()> {
    let _fixture = super::super::PROCESS_FIXTURE.lock().await;
    let _adoption = remote_pid::adopt_orphans()?;
    let hosts = Hosts::build().await?;
    let ssh = Ssh::start().await?;
    let network = hub().await?;
    let endpoint = network
        .info()
        .locators()
        .wait()
        .into_iter()
        .find(|locator| locator.to_string().starts_with("tcp/127.0.0.1:"))
        .context("Zenoh locator")?
        .to_string();
    for case in [
        "normal",
        "revision",
        "fingerprint",
        "host-key",
        "bundle",
        "config",
        "eof",
        "stop-prepare",
        "deadline",
    ] {
        exercise_case(&hosts, &ssh, &endpoint, case).await?;
    }
    owner_crash(&hosts, &ssh, &endpoint).await?;
    network.close().await.map_err(anyhow::Error::msg)?;
    ssh.finish().await?;
    Ok(())
}

async fn exercise_case(hosts: &Hosts, ssh: &Ssh, endpoint: &str, case: &str) -> Result<()> {
    eprintln!("SSH CASE {case}");
    let mut events = Events::listen().await?;
    let fixture = Fixture::new(
        hosts,
        endpoint,
        &events.address,
        case == "fingerprint",
        if case == "revision" { "missing" } else { "1" },
    )?;
    if matches!(case, "normal" | "stop-prepare" | "deadline") {
        fixture.block_prepare("receiver")?;
    }
    fixture.prepare().await?;
    let prepared = PreparedDeployment::load_file(&fixture.path().join("prepared.json"))?;
    let mut targets = ssh.targets(&prepared, fixture.path())?;
    corrupt(case, ssh, &prepared, &mut targets)?;
    let mut owner = Owner::start(prepared.clone(), targets)?;
    let exits = match exercise_running(case, &owner, &mut events).await {
        Ok(exits) => exits,
        Err(error) => {
            dump_failure(&owner, &fixture)?;
            return Err(error);
        }
    };
    owner.finish()?;
    for exit in exits {
        exit.exited().await?;
    }
    retained(fixture.path())?;
    let state = reiny_launch::last_status(fixture.path())?.context("retained status")?;
    assert_eq!(state.phase, reiny_launch::DeploymentPhase::Failed);
    assert!(
        state
            .component_hosts
            .values()
            .all(|host| host.pid.is_none())
    );
    // Even the unchanged old local API must refuse replacement after Unknown.
    let rejected = prepared.clone();
    assert!(
        tokio::task::spawn_blocking(move || {
            reiny_launch::serve(rejected, &Observer, &AtomicBool::new(true), &|| Ok(()))
        })
        .await?
        .is_err()
    );
    for record in reiny_launch::remote_ownership(fixture.path())? {
        if case == "normal" {
            assert!(matches!(
                record.cooperative_outcome,
                Some(reiny::component::process::protocol::HostOutcome::Stopped {})
            ));
            assert_eq!(record.remote_command_exit, Some(0));
        }
        reiny_launch::reconcile_remote(
            fixture.path(),
            &record.identity,
            &record.target_digest,
            "fixture has separately observed owned processes terminate",
        )?;
    }
    tokio::task::spawn_blocking(move || {
        reiny_launch::serve(prepared, &Observer, &AtomicBool::new(true), &|| Ok(()))
    })
    .await??;
    eprintln!("SSH CASE {case}: passed");
    Ok(())
}

fn dump_failure(owner: &Owner, fixture: &Fixture) -> Result<()> {
    let state = owner.client.status()?;
    eprintln!("SSH fixture failure state: {state:?}");
    for host in state.component_hosts.values() {
        let log = fixture
            .path()
            .join(".reiny/generations")
            .join(&state.owner_generation)
            .join(host.generation.to_string())
            .join("stderr.log");
        if let Ok(text) = std::fs::read_to_string(log) {
            eprintln!("SSH fixture stderr: {text}");
        }
    }
    Ok(())
}

fn corrupt(
    case: &str,
    ssh: &Ssh,
    prepared: &PreparedDeployment,
    targets: &mut BTreeMap<String, SshTarget>,
) -> Result<()> {
    match case {
        "host-key" => {
            targets.get_mut("producer").context("producer")?.host_key =
                std::fs::read_to_string(ssh.root.path().join("wrong.pub"))?
                    .split_whitespace()
                    .take(2)
                    .collect::<Vec<_>>()
                    .join(" ");
        }
        "bundle" => std::fs::write(&targets["producer"].executable, b"corrupt")?,
        "config" => {
            let target = &targets["producer"];
            let component = prepared
                .components
                .iter()
                .find(|component| component.host == "producer")
                .context("producer component")?;
            let local = component
                .config_bundle_dir
                .as_ref()
                .context("config bundle")?;
            let remote_file = target.configs[local].join(
                component
                    .component
                    .config
                    .as_ref()
                    .context("config")?
                    .strip_prefix(local)?,
            );
            std::fs::write(remote_file, b"corrupt")?;
        }
        _ => {}
    }
    Ok(())
}

async fn exercise_running(
    case: &str,
    owner: &Owner,
    events: &mut Events,
) -> Result<Vec<remote_pid::Exit>> {
    let mut exits = Vec::new();
    if case == "normal" {
        for role in ["sender", "receiver"] {
            events.wait(role, "prepared").await?;
        }
        events.assert_no_run()?;
        events.release_prepare("receiver").await?;
    }
    if matches!(case, "normal" | "eof") {
        let client = owner.client.clone();
        tokio::task::spawn_blocking(move || client.wait_ready(DEADLINE)).await??;
        let receiver = events.wait("receiver", "delivered").await?;
        let sender = events.wait("sender", "delivered").await?;
        for event in [&receiver, &sender] {
            exits.push(remote_pid::Exit::subscribe(
                event["pid"].as_u64().context("native PID")?,
            )?);
        }
        assert_eq!(receiver["sequence"], 42);
        assert_eq!(receiver["source"], "placement/sender");
        assert_eq!(sender["source"], "placement/receiver");
        let live = owner.client.status()?;
        // SSH client PIDs must not masquerade as native command PIDs.
        assert_ne!(
            live.component_hosts["producer"].pid.map(u64::from),
            sender["pid"].as_u64()
        );
        if case == "eof" {
            let pid = live.component_hosts["producer"]
                .pid
                .context("owned SSH PID")?;
            let client_exit = remote_pid::Exit::subscribe(u64::from(pid))?;
            client_exit.kill()?;
            client_exit.exited().await?;
            events.wait("sender", "stop").await?;
        }
    } else if case == "stop-prepare" {
        for role in ["sender", "receiver"] {
            let event = events.wait(role, "prepared").await?;
            exits.push(remote_pid::Exit::subscribe(
                event["pid"].as_u64().context("native PID")?,
            )?);
        }
        events.assert_no_run()?;
    } else {
        let client = owner.client.clone();
        let failed = tokio::task::spawn_blocking(move || {
            let deadline = std::time::Instant::now() + DEADLINE;
            let mut state = client.status()?;
            while state.phase != reiny_launch::DeploymentPhase::Failed {
                state = client.wait_for_revision(
                    state.revision,
                    deadline
                        .checked_duration_since(std::time::Instant::now())
                        .context("failure deadline")?,
                )?;
            }
            Ok::<_, anyhow::Error>(state)
        })
        .await??;
        assert!(
            failed
                .component_hosts
                .values()
                .all(|host| host.pid.is_none())
        );
        events.assert_no_run()?;
    }
    Ok(exits)
}
