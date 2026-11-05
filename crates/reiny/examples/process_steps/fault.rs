//! Retained fault outcomes and explicit direct-child reaping.

use super::{
    BUDGET, Duration, ProcessStepOwner, Result, StepFailureKind, ensure, request, timeout,
};

pub(super) async fn fault_fixture(
    owner: ProcessStepOwner,
    steps: reiny::component::process::ProcessStepClient,
    mut children: Vec<tokio::process::Child>,
    mode: &str,
) -> Result<()> {
    use reiny::component::step::{CancelStatus, StepRunError};
    use tokio::io::AsyncBufReadExt;
    let stdout = children[1]
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing mutation event pipe"))?;
    let mut events = tokio::io::BufReader::new(stdout).lines();
    let run = steps.try_step(request(), BUDGET)?;
    ensure!(
        timeout(BUDGET, events.next_line()).await??.as_deref() == Some("MUTATED"),
        "missing mutation barrier"
    );
    if mode == "--kill-blocked" {
        children[1].kill().await?;
    } else {
        ensure!(run.cancel() == CancelStatus::Requested, "cancellation lost");
    }
    let result = timeout(BUDGET, run.wait()).await?;
    let Err(StepRunError::Failed(failure)) = result else {
        anyhow::bail!("expected retained fault");
    };
    let expected = if mode == "--kill-blocked" {
        StepFailureKind::WorkerLost
    } else {
        StepFailureKind::Cancelled
    };
    ensure!(
        failure.kind == expected && failure.attempted == ["a", "b"] && failure.completed == ["a"],
        "wrong precise failure/prefix"
    );
    ensure!(
        steps.clock().time == Duration::ZERO && steps.clock().next_index == 0,
        "failed tick committed"
    );
    ensure!(
        steps.try_step(request(), BUDGET).is_err(),
        "faulted epoch reused"
    );
    let cleanup = owner.stop().await?;
    println!(
        "failure={:?} attempted={:?} completed={:?} clock=0 cleanup={cleanup:?}",
        failure.kind, failure.attempted, failure.completed
    );
    for (index, mut child) in children.into_iter().enumerate() {
        let status = timeout(BUDGET, child.wait()).await??;
        ensure!(
            status.success() || (index == 1 && mode == "--kill-blocked"),
            "unexpected process failure"
        );
        println!("reaped direct child {index}: {status}");
    }
    println!("local process containment observed separately; remote containment remains Unknown");
    Ok(())
}
