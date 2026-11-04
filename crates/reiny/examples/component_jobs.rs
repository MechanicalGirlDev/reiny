//! Run with `cargo run -p reiny --no-default-features --example component_jobs`.
//! A component can keep this owner as a field and call shutdown from its stop hook.

use std::num::NonZeroUsize;
use std::time::Duration;

use reiny::job::{JobContext, JobOutcome, JobOwner, JobStatus};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (acknowledge, mut acknowledged) = mpsc::channel::<()>(1);
    let mut job = owner.start(|ctx: JobContext<usize, u64, anyhow::Error>| async move {
        let samples = [2_u64, 3, 5, 7, 11, 13];
        let mut energy = 0;
        for (index, sample) in samples.into_iter().enumerate() {
            if ctx.is_cancelled() {
                return Ok(energy);
            }
            energy += sample * sample;
            ctx.report(index + 1);
            // Backpressure lets the demonstration render each actual result
            // before continuing; cancellation remains selectable during the wait.
            tokio::select! {
                () = ctx.cancelled() => return Ok(energy),
                ack = acknowledged.recv() => anyhow::ensure!(ack.is_some(), "observer left"),
            }
        }
        Ok(energy)
    })?;
    loop {
        let snapshot = job.changed().await;
        match snapshot.status {
            JobStatus::Running => {
                if let Some(processed) = snapshot.progress {
                    println!("job {}: processed {processed}/6", snapshot.id.get());
                    acknowledge.send(()).await?;
                }
            }
            JobStatus::Cancelling => {}
            JobStatus::Complete(outcome) => {
                match outcome.as_ref() {
                    JobOutcome::Succeeded(energy) => println!("sum of squares: {energy}"),
                    JobOutcome::Failed(error) => anyhow::bail!("job failed: {error}"),
                    JobOutcome::Cancelled => println!("cancelled after cleanup"),
                    JobOutcome::Panicked => anyhow::bail!("worker panicked"),
                    JobOutcome::Interrupted => anyhow::bail!("runtime interrupted worker"),
                }
                break;
            }
        }
    }
    let shutdown = owner.shutdown(Duration::from_secs(3)).await?;
    anyhow::ensure!(
        shutdown.unresponsive.is_empty() && shutdown.join_failures.is_empty(),
        "shutdown incomplete: {shutdown:?}"
    );
    Ok(())
}
