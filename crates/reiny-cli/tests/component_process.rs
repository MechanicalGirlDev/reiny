//! Real SDK process hosts, real CLI ownership, and real loopback Zenoh delivery.

#[path = "component_process/fixture.rs"]
mod fixture;
#[path = "component_process/local.rs"]
mod local;
#[path = "component_process/owner_control.rs"]
mod owner_control;
#[path = "component_process/remote.rs"]
mod remote;
#[path = "component_process/semantics.rs"]
mod semantics;

use anyhow::{Context, Result, ensure};
use fixture::{DEADLINE, Events, Fixture, Hosts, success};
use reiny::zenoh::{self, Wait};
use reiny_launch::DeploymentClient;
use serde_json::Value;
use std::time::Instant;

// The SSH crash fixture temporarily adopts orphaned local clients. Serialize
// these process fixtures so that setting cannot affect the legacy EOF test.
static PROCESS_FIXTURE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn hub() -> Result<zenoh::Session> {
    let mut config = zenoh::Config::default();
    // An explicit router carries traffic between the two outbound-only peers.
    // Loopback locators are not advertised by Zenoh peer gossip.
    config
        .insert_json5("mode", r#""router""#)
        .map_err(anyhow::Error::msg)?;
    config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
        .map_err(anyhow::Error::msg)?;
    config
        .insert_json5("connect/endpoints", "[]")
        .map_err(anyhow::Error::msg)?;
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .map_err(anyhow::Error::msg)?;
    zenoh::open(config).await.map_err(anyhow::Error::msg)
}

fn assert_reaped(status: &Value) -> Result<()> {
    let hosts = status["component_hosts"]
        .as_object()
        .context("host status map")?;
    ensure!(hosts.len() == 2, "expected two owned hosts: {status}");
    for (name, host) in hosts {
        ensure!(host["pid"].is_null(), "{name} is not reaped: {status}");
        ensure!(
            !host["exit"].is_null(),
            "{name} has no exit result: {status}"
        );
    }
    Ok(())
}

async fn check_plan(fixture: &Fixture) -> Result<()> {
    // The CLI must render actual hosted route endpoints, not index only legacy
    // nodes. Both human and machine surfaces operate on the same real graph.
    let root = fixture.path().to_str().context("fixture path")?;
    let output = fixture.cli(&["plan", root]).await?;
    success(&output, "hosted plan")?;
    let output = fixture.cli(&["plan", root, "--json"]).await?;
    success(&output, "hosted JSON plan")?;
    let plan: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        plan["components"]
            .as_array()
            .context("planned components")?
            .len(),
        2
    );
    assert_eq!(
        plan["connections"]
            .as_array()
            .context("planned connections")?
            .len(),
        2
    );
    let bundled = fixture.path().join("unsupported-bundle");
    let output = fixture
        .cli(&[
            "bundle",
            root,
            "--out",
            bundled.to_str().context("bundle output path")?,
        ])
        .await?;
    ensure!(!output.status.success(), "hosted bundle must fail closed");
    ensure!(!bundled.exists(), "rejected hosted bundle created output");
    Ok(())
}

fn check_ready(ready: &Value) -> Result<(u64, u64)> {
    assert_eq!(ready["version"], 3);
    assert_eq!(ready["phase"], "ready");
    let producer = &ready["component_hosts"]["producer"];
    let consumer = &ready["component_hosts"]["consumer"];
    let producer_pid = producer["pid"].as_u64().context("producer PID")?;
    let consumer_pid = consumer["pid"].as_u64().context("consumer PID")?;
    assert_ne!(producer_pid, consumer_pid);
    for host in [producer, consumer] {
        assert_eq!(host["phase"], "running");
        ensure!(host["generation"].as_u64().context("host generation")? > 0);
        ensure!(
            host["graph_generation"]
                .as_u64()
                .context("graph generation")?
                > 0
        );
        assert!(host["error"].is_null());
    }
    for (role, host) in [("sender", "producer"), ("receiver", "consumer")] {
        let namespace = format!("placement/{role}");
        let component = &ready["components"][&namespace];
        assert_eq!(component["host"], host);
        assert_eq!(component["phase"], "running");
        assert_eq!(component["report"]["namespace"], namespace);
        assert_eq!(component["report"]["endpoint_namespace"], namespace);
        assert!(component["error"].is_null());
    }
    Ok((producer_pid, consumer_pid))
}

async fn normal_delivery(hosts: &Hosts, endpoint: &str) -> Result<()> {
    let mut events = Events::listen().await?;
    let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
    fixture.block_prepare("receiver")?;
    check_plan(&fixture).await?;
    fixture.prepare().await?;
    let prepared: Value =
        serde_json::from_slice(&std::fs::read(fixture.path().join("prepared.json"))?)?;
    assert_eq!(prepared["version"], 2);
    let (applied, released) = tokio::join!(fixture.apply(), async {
        let _sender = events.wait("sender", "prepared").await?;
        let _receiver = events.wait("receiver", "prepared").await?;
        for role in ["sender", "receiver"] {
            ensure!(
                !fixture.path().join(format!("{role}.ran")).exists(),
                "{role} ran while remote preparation was blocked"
            );
        }
        events.release_prepare("receiver").await
    });
    released?;
    success(&applied?, "hosted apply")?;

    let ready = fixture.status().await?;
    let (producer_pid, consumer_pid) = check_ready(&ready)?;
    let received = events.wait("receiver", "delivered").await?;
    assert_eq!(received["pid"], consumer_pid);
    assert_eq!(received["sequence"], 42);
    assert_eq!(received["source"], "placement/sender");
    assert_eq!(received["source_port"], "data");
    let acknowledged = events.wait("sender", "delivered").await?;
    assert_eq!(acknowledged["pid"], producer_pid);
    assert_eq!(acknowledged["sequence"], 42);
    assert_eq!(acknowledged["source"], "placement/receiver");
    assert_eq!(acknowledged["source_port"], "acknowledgement");

    // Unchanged apply is a real CLI no-op, preserving both owned generations.
    success(&fixture.apply().await?, "identical hosted reapply")?;
    let unchanged = fixture.status().await?;
    for name in ["producer", "consumer"] {
        assert_eq!(
            unchanged["component_hosts"][name]["pid"],
            ready["component_hosts"][name]["pid"]
        );
        assert_eq!(
            unchanged["component_hosts"][name]["generation"],
            ready["component_hosts"][name]["generation"]
        );
    }
    let stopped = fixture.stop().await?;
    assert_eq!(stopped["phase"], "stopped");
    assert_reaped(&stopped)?;
    for role in ["sender", "receiver"] {
        let _stopped = events.wait(role, "stop").await?;
    }
    for host in ["producer", "consumer"] {
        assert_eq!(stopped["component_hosts"][host]["phase"], "stopped");
        assert_eq!(
            stopped["component_hosts"][host]["outcome"]["kind"],
            "stopped"
        );
    }
    Ok(())
}

async fn abnormal_exit(hosts: &Hosts, endpoint: &str) -> Result<()> {
    // One actual host exits without reporting a local outcome. The other host
    // remains running under report, and the instance cannot claim clean stop.
    let mut events = Events::listen().await?;
    let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
    fixture.prepare().await?;
    success(&fixture.apply().await?, "abnormal-exit setup")?;
    let _received = events.wait("receiver", "delivered").await?;
    let _acknowledged = events.wait("sender", "delivered").await?;
    let before = fixture.status().await?;
    let client = DeploymentClient::find(fixture.path())?.context("live owner")?;
    events.exit("sender").await?;
    let deadline = Instant::now() + DEADLINE;
    let mut changed = client.status()?;
    while changed.component_hosts["producer"].exit.is_none() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .context("owner did not observe actual process exit")?;
        let revision = changed.revision;
        let observer = client.clone();
        changed =
            tokio::task::spawn_blocking(move || observer.wait_for_revision(revision, remaining))
                .await??;
    }
    let failed = fixture.status().await?;
    assert_eq!(failed["phase"], "degraded");
    assert!(failed["component_hosts"]["producer"]["pid"].is_null());
    assert!(!failed["component_hosts"]["producer"]["error"].is_null());
    assert_ne!(failed["components"]["placement/sender"]["phase"], "stopped");
    assert!(!failed["components"]["placement/sender"]["error"].is_null());
    assert_eq!(failed["component_hosts"]["consumer"]["phase"], "running");
    assert_eq!(
        failed["component_hosts"]["consumer"]["pid"],
        before["component_hosts"]["consumer"]["pid"]
    );
    let stopped = fixture.stop().await?;
    assert_reaped(&stopped)?;
    let _stopped = events.wait("receiver", "stop").await?;
    Ok(())
}

async fn rejection(hosts: &Hosts, endpoint: &str) -> Result<()> {
    // Same type name with a different compiled fingerprint must fail the whole
    // graph before either run hook. A wrong factory revision fails admission too.
    for (wrong_type, revision) in [(true, "1"), (false, "missing-revision")] {
        let mut events = Events::listen().await?;
        let fixture = Fixture::new(hosts, endpoint, &events.address, wrong_type, revision)?;
        fixture.prepare().await?;
        let output = fixture.apply().await?;
        ensure!(
            !output.status.success(),
            "incompatible hosted graph was admitted"
        );
        let status = fixture.status().await?;
        assert_reaped(&status)?;
        for role in ["sender", "receiver"] {
            ensure!(
                !fixture.path().join(format!("{role}.ran")).exists(),
                "rejected graph entered {role} run"
            );
        }
        events.assert_no_run()?;
        ensure!(
            !status["error"].is_null()
                || status["component_hosts"]
                    .as_object()
                    .context("host map")?
                    .values()
                    .any(|host| !host["error"].is_null()),
            "rejection has no retained failure: {status}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosted_cli_placement_delivery_rejection_and_process_cleanup() -> Result<()> {
    let _fixture = PROCESS_FIXTURE.lock().await;
    let hosts = Hosts::build().await?;
    let network = hub().await?;
    let endpoint = network
        .info()
        .locators()
        .wait()
        .into_iter()
        .find(|locator| locator.to_string().starts_with("tcp/127.0.0.1:"))
        .context("ephemeral loopback Zenoh locator")?
        .to_string();
    normal_delivery(&hosts, &endpoint).await?;
    local::delivery_and_rejection(&hosts, &endpoint).await?;
    abnormal_exit(&hosts, &endpoint).await?;
    rejection(&hosts, &endpoint).await?;
    semantics::admission(&hosts, &endpoint).await?;
    owner_control::startup_stdin_eof(&hosts, &endpoint).await?;
    owner_control::owner_process_eof(&hosts, &endpoint).await?;
    network.close().await.map_err(anyhow::Error::msg)?;
    Ok(())
}
