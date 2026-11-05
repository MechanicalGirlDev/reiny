//! Owner-boundary fault peers. Real SDK traffic lives in the CLI placement test.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[path = "hosted_owner/peer.rs"]
mod peer;
#[path = "hosted_owner/support.rs"]
mod support;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Fault {
    Nonce,
    Digest,
    Generation,
    Sequence,
    Compatibility,
    DuplicateJson,
    NestedDuplicateJson,
    Oversize,
    UnknownField,
    FirstStatus,
    ReportIdentity,
    GraphSchema,
    DuplicateReport,
    PrepareEof,
    Commit,
    UnrequestedCancel,
    BlockPrepare,
    Healthy,
}

#[derive(Deserialize, Serialize)]
struct Fixture {
    address: std::net::SocketAddr,
    fault: Fault,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum EventKind {
    Connected,
    Prepared,
    Run,
}

#[derive(Deserialize, Serialize)]
struct Event {
    host: String,
    kind: EventKind,
}

fn main() -> Result<()> {
    if std::env::current_exe()?
        .file_stem()
        .is_some_and(|name| name == "peer")
    {
        return peer::run();
    }
    for (fault, reason) in [
        (Fault::Nonce, "authentication"),
        (Fault::Digest, "identity"),
        (Fault::Generation, "identity"),
        (Fault::Sequence, "sequence"),
        (Fault::Compatibility, "compatibility"),
        (Fault::DuplicateJson, "duplicate"),
        (Fault::NestedDuplicateJson, "duplicate"),
        (Fault::Oversize, "frame"),
        (Fault::UnknownField, "schema"),
        (Fault::FirstStatus, "authenticate"),
        (Fault::ReportIdentity, "report"),
        (Fault::DuplicateReport, "report"),
        (Fault::PrepareEof, "disconnected"),
        (Fault::Commit, "Commit"),
        (Fault::UnrequestedCancel, "Cancelled"),
    ] {
        let mut harness = support::Harness::start(fault, false)?;
        let failed = harness.phase(reiny_launch::DeploymentPhase::Failed)?;
        assert!(
            failed
                .error
                .as_deref()
                .context("missing admission failure")?
                .contains(reason),
            "wrong rejection: {:?}",
            failed.error
        );
        assert!(
            failed
                .component_hosts
                .values()
                .all(|host| host.pid.is_none())
        );
        harness.stop()?;
        assert!(harness.events().all(|event| event.kind != EventKind::Run));
        println!("owner rejects {reason} before Run: passed");
    }
    {
        let mut harness = support::Harness::start(Fault::BlockPrepare, false)?;
        harness.event("failing", EventKind::Prepared)?;
        assert_eq!(
            harness.client.status()?.phase,
            reiny_launch::DeploymentPhase::Starting
        );
        let stopped = harness.stop()?;
        assert!(
            stopped
                .component_hosts
                .values()
                .all(|host| host.pid.is_none())
        );
        assert!(harness.events().all(|event| event.kind != EventKind::Run));
        println!("owner stop remains responsive during preparation: passed");
    }
    {
        let mut harness = support::Harness::start(Fault::Healthy, true)?;
        harness.client.wait_ready(support::DEADLINE)?;
        harness.event("failing", EventKind::Run)?;
        harness.send("failing", "exit\n")?;
        let suspended = harness.phase(reiny_launch::DeploymentPhase::Suspended)?;
        assert!(
            suspended
                .component_hosts
                .values()
                .all(|host| host.pid.is_none())
        );
        assert!(suspended.components["wire/left"].error.is_some());
        harness.stop()?;
        println!("post-activation suspend policy stops the whole owned graph: passed");
    }
    graph_schema_abort()
}

fn graph_schema_abort() -> Result<()> {
    let mut harness = support::Harness::start(Fault::GraphSchema, false)?;
    harness.event("failing", EventKind::Prepared)?;
    let deadline = std::time::Instant::now() + support::DEADLINE;
    let mut status = harness.client.status()?;
    while status.component_hosts["healthy"].phase != reiny_core::host_protocol::HostPhase::Prepared
    {
        status = harness.client.wait_for_revision(
            status.revision,
            deadline
                .checked_duration_since(std::time::Instant::now())
                .context("healthy preparation deadline")?,
        )?;
    }
    harness.send("failing", "report\n")?;
    let failed = harness.phase(reiny_launch::DeploymentPhase::Failed)?;
    assert!(
        failed
            .error
            .as_deref()
            .context("missing schema failure")?
            .contains("fingerprint")
    );
    assert!(
        failed
            .component_hosts
            .values()
            .all(|host| host.pid.is_none())
    );
    let cancelled = &failed.component_hosts["failing"];
    assert!(
        cancelled.cleanup_errors.is_empty(),
        "owner-requested admission cancellation is not cleanup failure: {:?}",
        cancelled.cleanup_errors
    );
    assert!(matches!(
        cancelled.outcome,
        Some(reiny_core::host_protocol::HostOutcome::Failed {
            ref primary,
            ref cleanup,
        }) if primary.code == reiny_core::host_protocol::FailureCode::Cancelled
            && cleanup.is_empty()
    ));
    assert!(cancelled.exit.is_some());
    harness.stop()?;
    assert!(harness.events().all(|event| event.kind != EventKind::Run));
    println!("owner retains admission cancellation without false cleanup failure: passed");
    Ok(())
}
