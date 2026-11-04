use super::*;

struct GatedDrop {
    entered: flume::Sender<()>,
    released: flume::Receiver<()>,
}

impl Drop for GatedDrop {
    fn drop(&mut self) {
        assert!(self.entered.send(()).is_ok());
        assert!(self.released.recv_timeout(DEADLINE).is_ok());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_success_waits_for_result_cleanup() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (entered, entry) = flume::bounded(1);
    let (release, released) = flume::bounded(1);
    let job = owner.start(|ctx: JobContext<(), GatedDrop, ()>| async move {
        ctx.cancelled().await;
        Ok(GatedDrop { entered, released })
    })?;
    let mut observer = job.subscribe();

    assert_eq!(job.cancel(), CancelRequest::Requested);
    let entry_result = timeout(DEADLINE, entry.recv_async()).await;
    let during_cleanup = observer.snapshot();
    let mut waiter = std::pin::pin!(job.wait());
    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(waiter.as_mut(), cx))
    })
    .await;
    // Release and join before asserting so a failure cannot strand the worker.
    release.send(())?;
    joined(&mut owner).await?;

    entry_result??;
    assert!(matches!(during_cleanup.status, JobStatus::Cancelling));
    assert_eq!(during_cleanup.revision.get(), 1);
    assert!(polled.is_pending());
    assert!(matches!(
        *timeout(DEADLINE, waiter).await?,
        JobOutcome::Cancelled
    ));
    let terminal = timeout(DEADLINE, observer.changed()).await?;
    assert!(matches!(
        terminal.status,
        JobStatus::Complete(outcome) if matches!(*outcome, JobOutcome::Cancelled)
    ));
    assert_eq!(terminal.revision.get(), 2);
    Ok(())
}

struct PanickingDrop;

impl Drop for PanickingDrop {
    fn drop(&mut self) {
        std::panic::resume_unwind(Box::new("result cleanup panic"));
    }
}

#[tokio::test]
async fn cancelled_success_preserves_result_cleanup_panic() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let job = owner.start(|ctx: JobContext<(), PanickingDrop, ()>| async move {
        ctx.cancelled().await;
        Ok(PanickingDrop)
    })?;
    let observer = job.subscribe();

    assert_eq!(job.cancel(), CancelRequest::Requested);
    let outcome = timeout(DEADLINE, observer.wait()).await?;
    let report = owner.shutdown(DEADLINE).await?;

    assert!(matches!(*outcome, JobOutcome::Panicked));
    assert!(observer.snapshot().cancellation_requested);
    assert_eq!(report.join_failures, vec![job.snapshot().id]);
    assert_eq!(report.unresponsive, []);
    Ok(())
}
