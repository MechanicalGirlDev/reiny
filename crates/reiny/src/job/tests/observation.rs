use super::*;

#[tokio::test]
async fn real_progress_is_visible_before_release_and_retained_after_success() -> anyhow::Result<()>
{
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (begin, started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let job: TestJob = owner.start(|ctx| async move {
        started.await.map_err(|_| "start gate closed")?;
        let sum = [3_u64, 5, 7].into_iter().sum();
        assert!(ctx.report(sum));
        released.await.map_err(|_| "release gate closed")?;
        Ok(sum * 2)
    })?;
    let mut first = job.subscribe();
    let mut second = job.subscribe();

    begin
        .send(())
        .map_err(|()| anyhow::anyhow!("worker gone"))?;
    let progress = timeout(DEADLINE, first.changed()).await?;
    let other_progress = timeout(DEADLINE, second.changed()).await?;

    assert_eq!(progress.progress.as_deref(), Some(&15));
    assert_eq!(other_progress.progress.as_deref(), Some(&15));
    assert_eq!(progress.revision.get(), 1);
    assert!(matches!(progress.status, JobStatus::Running));
    let mut waiters = Box::pin(async { tokio::join!(job.wait(), second.wait()) });
    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(waiters.as_mut(), cx))
    })
    .await;
    assert!(polled.is_pending());
    release
        .send(())
        .map_err(|()| anyhow::anyhow!("worker gone"))?;
    let (first_result, second_result) = timeout(DEADLINE, waiters).await?;
    assert_eq!(*first_result, JobOutcome::Succeeded(30));
    assert!(Arc::ptr_eq(&first_result, &second_result));
    let final_state = timeout(DEADLINE, first.changed()).await?;
    assert_eq!(final_state.progress.as_deref(), Some(&15));
    assert_eq!(final_state.revision.get(), 2);
    assert!(matches!(final_state.status, JobStatus::Complete(_)));
    joined(&mut owner).await
}

#[tokio::test]
async fn dropped_pending_waiter_and_handle_do_not_cancel_work() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let (release, released) = oneshot::channel();
    let job: TestJob = owner.start(|ctx| async move {
        released.await.map_err(|_| "release gate closed")?;
        assert!(!ctx.is_cancelled());
        Ok(17)
    })?;
    let observer = job.subscribe();
    let mut waiter = Box::pin(job.wait());
    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(waiter.as_mut(), cx))
    })
    .await;
    assert!(polled.is_pending());

    drop(waiter);
    drop(job);
    release
        .send(())
        .map_err(|()| anyhow::anyhow!("worker gone"))?;

    assert_eq!(*terminal(&observer).await?, JobOutcome::Succeeded(17));
    joined(&mut owner).await
}

#[tokio::test]
async fn retained_payloads_are_released_with_their_explicit_owners() -> anyhow::Result<()> {
    let mut owner = JobOwner::new(NonZeroUsize::MIN);
    let progress = Arc::new(4_u64);
    let result = Arc::new(9_u64);
    let weak_progress = Arc::downgrade(&progress);
    let weak_result = Arc::downgrade(&result);
    let job = owner.start(move |ctx: JobContext<Arc<u64>, Arc<u64>, ()>| async move {
        assert!(ctx.report(progress));
        Ok(result)
    })?;
    let outcome = timeout(DEADLINE, job.wait()).await?;
    joined(&mut owner).await?;
    let snapshot = job.snapshot();

    drop(job);
    assert!(weak_progress.upgrade().is_some());
    assert!(weak_result.upgrade().is_some());
    drop(snapshot);
    assert!(weak_progress.upgrade().is_none());
    assert!(weak_result.upgrade().is_some());
    drop(outcome);
    assert!(weak_result.upgrade().is_none());
    Ok(())
}
