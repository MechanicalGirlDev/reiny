use super::*;

#[test]
fn starting_without_runtime_returns_a_typed_error() {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);

    let rejected = owner.start(|_: JobContext<(), (), ()>| async { Ok(()) });

    assert!(matches!(rejected, Err(StartError::RuntimeUnavailable)));
}

#[tokio::test]
async fn completed_jobs_release_admission_capacity() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let first: TestJob = owner.start(|_| async { Ok(1) })?;
    assert_eq!(*terminal(&first).await?, JobOutcome::Succeeded(1));

    let second: TestJob = owner.start(|_| async { Ok(2) })?;

    assert_eq!(*terminal(&second).await?, JobOutcome::Succeeded(2));
    assert_ne!(first.snapshot().id, second.snapshot().id);
    joined(&mut owner).await
}

#[tokio::test]
async fn cancellation_wins_before_worker_success_and_is_idempotent() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (signal, cancellation_seen) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let job: TestJob = owner.start(|ctx| async move {
        ctx.cancelled().await;
        assert!(ctx.is_cancelled());
        assert!(!ctx.report(99));
        signal.send(()).map_err(|()| "observer gone")?;
        released.await.map_err(|_| "release gate closed")?;
        Ok(42)
    })?;
    let mut observer = job.subscribe();

    assert_eq!(job.cancel(), CancelRequest::Requested);
    assert_eq!(job.cancel(), CancelRequest::AlreadyRequested);
    timeout(DEADLINE, cancellation_seen).await??;
    let cancelling = timeout(DEADLINE, observer.changed()).await?;

    assert_eq!(cancelling.revision.get(), 1);
    assert!(matches!(cancelling.status, JobStatus::Cancelling));
    release
        .send(())
        .map_err(|()| anyhow::anyhow!("worker gone"))?;
    assert_eq!(*terminal(&job).await?, JobOutcome::Cancelled);
    assert_eq!(job.cancel(), CancelRequest::Finished);
    assert_eq!(job.snapshot().revision.get(), 2);
    joined(&mut owner).await
}

#[tokio::test]
async fn success_committed_before_cancellation_stays_successful() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let job: TestJob = owner.start(|_| async { Ok(42) })?;
    let outcome = terminal(&job).await?;
    let revision = job.snapshot().revision;

    assert_eq!(job.cancel(), CancelRequest::Finished);

    assert_eq!(*outcome, JobOutcome::Succeeded(42));
    assert_eq!(job.snapshot().revision, revision);
    assert!(!job.snapshot().cancellation_requested);
    joined(&mut owner).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_cancel_and_return_agree_on_one_terminal_result() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let job: TestJob = owner.start(|_| async move {
        worker_barrier.wait().await;
        Ok(23)
    })?;
    let observer = job.subscribe();
    let requester = tokio::spawn(async move {
        barrier.wait().await;
        job.cancel()
    });

    let request = timeout(DEADLINE, requester).await??;
    let outcome = terminal(&observer).await?;

    match request {
        CancelRequest::Requested => assert_eq!(*outcome, JobOutcome::Cancelled),
        CancelRequest::Finished => assert_eq!(*outcome, JobOutcome::Succeeded(23)),
        CancelRequest::AlreadyRequested => anyhow::bail!("only one cancellation requester"),
    }
    assert!(Arc::ptr_eq(&outcome, &terminal(&observer).await?));
    joined(&mut owner).await
}

#[tokio::test]
async fn worker_error_survives_prior_cancellation() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let job: TestJob = owner.start(|ctx| async move {
        ctx.cancelled().await;
        Err("cleanup failed")
    })?;

    assert_eq!(job.cancel(), CancelRequest::Requested);

    assert_eq!(*terminal(&job).await?, JobOutcome::Failed("cleanup failed"));
    assert!(job.snapshot().cancellation_requested);
    joined(&mut owner).await
}

#[tokio::test]
async fn factory_and_future_panics_become_observable_results() -> anyhow::Result<()> {
    for factory_panic in [true, false] {
        let mut owner = JobOwner::new(NonZeroUsize::MIN);
        let job: TestJob = owner.start(move |_| {
            if factory_panic {
                std::panic::resume_unwind(Box::new("factory panic"));
            }
            async { std::panic::resume_unwind(Box::new("future panic")) }
        })?;
        let id = job.snapshot().id;

        let outcome = terminal(&job).await?;
        let report = owner.shutdown(DEADLINE).await?;

        assert_eq!(*outcome, JobOutcome::Panicked);
        assert_eq!(report.join_failures, vec![id]);
        assert_eq!(report.unresponsive, []);
    }
    Ok(())
}

#[tokio::test]
async fn capacity_and_closed_owner_reject_work_without_invoking_it() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let first: TestJob = owner.start(|ctx| async move {
        ctx.cancelled().await;
        Ok(0)
    })?;

    let rejected = owner.start(|_: JobContext<(), (), ()>| async {
        std::panic::resume_unwind(Box::new("rejected work ran"));
    });
    assert!(matches!(rejected, Err(StartError::AtCapacity)));
    joined(&mut owner).await?;
    let rejected = owner.start(|_: JobContext<(), (), ()>| async {
        std::panic::resume_unwind(Box::new("closed owner ran work"));
    });

    assert!(matches!(rejected, Err(StartError::Closed)));
    assert_eq!(*terminal(&first).await?, JobOutcome::Cancelled);
    Ok(())
}

#[tokio::test]
async fn operation_ids_are_unique_across_owners() -> anyhow::Result<()> {
    let mut first = JobOwner::new(NonZeroUsize::MIN);
    let mut second = JobOwner::new(NonZeroUsize::MIN);

    let a: TestJob = first.start(|_| async { Ok(1) })?;
    let b: TestJob = second.start(|_| async { Ok(2) })?;

    assert_ne!(a.snapshot().id, b.snapshot().id);
    joined(&mut first).await?;
    joined(&mut second).await
}
