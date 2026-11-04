use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::timeout;

use super::{
    CancelRequest, JobContext, JobHandle, JobOutcome, JobOwner, JobStatus, ShutdownError,
    StartError,
};

mod cleanup;
mod lifecycle;
mod observation;
mod teardown;

const DEADLINE: Duration = Duration::from_secs(3);
type TestJob = JobHandle<u64, u64, &'static str>;

async fn terminal(job: &TestJob) -> anyhow::Result<Arc<JobOutcome<u64, &'static str>>> {
    Ok(timeout(DEADLINE, job.wait()).await?)
}

async fn joined(owner: &mut JobOwner) -> anyhow::Result<()> {
    let report = timeout(DEADLINE, owner.shutdown(DEADLINE)).await??;
    assert!(report.unresponsive.is_empty(), "{report:?}");
    assert!(report.join_failures.is_empty(), "{report:?}");
    Ok(())
}
