use super::*;

#[test]
fn runtime_teardown_is_observable_as_interruption() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (entered, entry) = oneshot::channel();
    let job: TestJob = runtime.block_on(async {
        let job = owner.start(|_: JobContext<u64, u64, &'static str>| async move {
            entered.send(()).map_err(|()| "entry observer gone")?;
            std::future::pending::<Result<u64, &'static str>>().await
        })?;
        timeout(DEADLINE, entry).await??;
        anyhow::Ok(job)
    })?;

    drop(runtime);

    match job.snapshot().status {
        JobStatus::Complete(outcome) => assert_eq!(*outcome, JobOutcome::Interrupted),
        JobStatus::Running | JobStatus::Cancelling => anyhow::bail!("runtime exit went unobserved"),
    }
    Ok(())
}

#[tokio::test]
async fn owner_drop_requests_cancellation_without_dropping_observation() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let job: TestJob = owner.start(|ctx| async move {
        ctx.cancelled().await;
        Ok(7)
    })?;

    drop(owner);

    assert_eq!(*terminal(&job).await?, JobOutcome::Cancelled);
    Ok(())
}

#[tokio::test]
async fn shutdown_timeout_retains_unresponsive_worker_until_it_really_exits() -> anyhow::Result<()>
{
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (entered, entry) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let job: TestJob = owner.start(|_| async move {
        entered.send(()).map_err(|()| "entry observer gone")?;
        released.await.map_err(|_| "release gate closed")?;
        Ok(11)
    })?;
    timeout(DEADLINE, entry).await??;

    let report = owner.shutdown(Duration::ZERO).await?;

    assert_eq!(report.unresponsive, vec![job.snapshot().id]);
    assert_eq!(report.join_failures, []);
    assert!(matches!(job.snapshot().status, JobStatus::Cancelling));
    release
        .send(())
        .map_err(|()| anyhow::anyhow!("worker gone"))?;
    joined(&mut owner).await?;
    assert_eq!(*terminal(&job).await?, JobOutcome::Cancelled);
    Ok(())
}

#[tokio::test]
async fn abandoning_shutdown_wait_keeps_workers_owned_for_retry() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (release, released) = oneshot::channel();
    let job: TestJob = owner.start(|_| async move {
        released.await.map_err(|_| "release gate closed")?;
        Ok(13)
    })?;
    let mut shutdown = Box::pin(owner.shutdown(DEADLINE));
    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(shutdown.as_mut(), cx))
    })
    .await;
    assert!(polled.is_pending());

    drop(shutdown);
    release
        .send(())
        .map_err(|()| anyhow::anyhow!("worker gone"))?;

    joined(&mut owner).await?;
    assert_eq!(*terminal(&job).await?, JobOutcome::Cancelled);
    Ok(())
}

#[tokio::test]
async fn maximal_grace_preserves_cancellation_and_worker_ownership() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (entered, entry) = oneshot::channel();
    let (cancelled, cancellation_seen) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let job: TestJob = owner.start(|ctx| async move {
        entered.send(()).map_err(|()| "entry observer gone")?;
        ctx.cancelled().await;
        cancelled.send(()).map_err(|()| "cancel observer gone")?;
        released.await.map_err(|_| "release gate closed")?;
        Ok(17)
    })?;
    timeout(DEADLINE, entry).await??;
    let representable = tokio::time::Instant::now()
        .checked_add(Duration::MAX)
        .is_some();
    let mut shutdown = Box::pin(owner.shutdown(Duration::MAX));

    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(shutdown.as_mut(), cx))
    })
    .await;

    drop(shutdown);
    // A platform able to represent MAX must still wait for the gated worker.
    if representable {
        assert!(polled.is_pending());
    } else {
        assert_eq!(
            polled,
            std::task::Poll::Ready(Err(ShutdownError::InvalidDeadline))
        );
    }
    timeout(DEADLINE, cancellation_seen).await??;
    assert!(job.snapshot().cancellation_requested);
    assert!(matches!(job.snapshot().status, JobStatus::Cancelling));
    let rejected = owner.start(|_: JobContext<(), (), ()>| async { Ok(()) });
    assert!(matches!(rejected, Err(StartError::Closed)));
    let retained = owner.shutdown(Duration::ZERO).await?;
    assert_eq!(retained.unresponsive, vec![job.snapshot().id]);
    assert_eq!(retained.join_failures, []);
    release
        .send(())
        .map_err(|()| anyhow::anyhow!("worker gone"))?;
    joined(&mut owner).await?;
    assert_eq!(*terminal(&job).await?, JobOutcome::Cancelled);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_work_is_not_claimed_stopped_by_shutdown_timeout() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (entered, entry) = oneshot::channel();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let job: TestJob = owner.start(|_| async move {
        tokio::task::spawn_blocking(move || {
            entered.send(()).map_err(|()| "entry observer gone")?;
            released.recv().map_err(|_| "native gate closed")?;
            Ok(19)
        })
        .await
        .map_err(|_| "native worker failed")?
    })?;
    let entry_result = timeout(DEADLINE, entry).await;
    let report = owner.shutdown(Duration::ZERO).await;
    let status = job.snapshot().status;
    // Always release the native thread before any assertion can fail.
    release.send(())?;
    let eventual = owner.shutdown(DEADLINE).await?;

    entry_result??;
    let report = report?;
    assert_eq!(report.unresponsive, vec![job.snapshot().id]);
    assert!(matches!(status, JobStatus::Cancelling));
    assert_eq!(eventual.unresponsive, []);
    assert_eq!(eventual.join_failures, []);
    assert_eq!(*terminal(&job).await?, JobOutcome::Cancelled);
    Ok(())
}
