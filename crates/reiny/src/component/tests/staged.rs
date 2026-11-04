use super::*;
use crate::engine::{Key, READY_CHUNK};
use std::future::{Future, poll_fn};
use std::task::Poll;

#[tokio::test]
async fn cancelled_report_wait_preserves_preparation_and_explicit_run_barrier() {
    let (registry, mut records, gate) = fixture(Behavior::PrepareWait);
    let engine = Arc::new(Local::new());
    let (handle, mut control) = ComponentHost::prepare(
        registry,
        options(engine.clone()),
        vec![instance("app/wait", "test")],
    )
    .expect("prepare");
    stage(&mut records, Stage::Prepare).await;
    {
        let mut reports = std::pin::pin!(control.prepared());
        poll_fn(|cx| {
            assert!(reports.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    gate.send_replace(true);
    let reports = timeout(PATIENCE, control.prepared())
        .await
        .expect("report deadline")
        .expect("reports");
    assert_eq!(
        reports
            .iter()
            .map(|report| report.namespace.as_str())
            .collect::<Vec<_>>(),
        ["app/wait"]
    );
    assert_eq!(handle.status().phase, HostPhase::Prepared);
    assert_eq!(
        engine
            .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
            .await
            .expect("prepared presence"),
        Vec::<Key>::new()
    );

    control.commit().await.expect("commit");
    assert_eq!(handle.status().phase, HostPhase::Committed);
    assert_eq!(
        handle.status().components["app/wait"].phase,
        ComponentPhase::Ready
    );
    assert_eq!(
        engine
            .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
            .await
            .expect("committed presence"),
        [Key::module("components", Some("app/wait"), READY_CHUNK)]
    );
    assert!(records.try_recv().is_err());
    control.activate().await.expect("activate");
    assert_eq!(stage(&mut records, Stage::Run).await.id, "app/wait");
    assert!(matches!(handle.stop().await, HostOutcome::Stopped));
}

#[tokio::test]
async fn abort_of_prepared_or_committed_host_releases_tickets_without_running() {
    for commit in [false, true] {
        let (registry, mut records, _gate) = fixture(Behavior::Idle);
        let engine = Arc::new(Local::new());
        let (handle, mut control) = ComponentHost::prepare(
            registry,
            options(engine.clone()),
            vec![
                instance("app/second", "test"),
                instance("app/first", "peer"),
            ],
        )
        .expect("prepare");
        let reports = control.prepared().await.expect("reports");
        assert_eq!(
            reports
                .iter()
                .map(|report| report.namespace.as_str())
                .collect::<Vec<_>>(),
            ["app/first", "app/second"]
        );
        if commit {
            control.commit().await.expect("commit");
        }
        let rejected = HostError::new(HostErrorCode::Graph, None, "owner rejected whole graph");

        control.abort(rejected.clone()).await;
        let (primary, cleanup) = failure(terminal(&handle).await);

        assert_eq!(primary, rejected);
        assert_eq!(cleanup, Vec::<HostError>::new());
        let mut stopped = Vec::new();
        let mut dropped = Vec::new();
        while let Ok(record) = records.try_recv() {
            assert_ne!(record.stage, Stage::Run);
            match record.stage {
                Stage::Stop(reason) => {
                    assert_eq!(reason, StopReason::StartupFailure);
                    stopped.push(record.id);
                }
                Stage::Dropped => dropped.push(record.id),
                Stage::Prepare | Stage::Run => {}
            }
        }
        stopped.sort();
        dropped.sort();
        assert_eq!(stopped, ["app/first", "app/second"]);
        assert_eq!(dropped, stopped);
        assert_eq!(
            engine
                .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
                .await
                .expect("withdrawn"),
            Vec::<Key>::new()
        );
    }
}

#[tokio::test]
async fn activation_without_commit_rejects_the_host_and_never_runs() {
    let (registry, mut records, _gate) = fixture(Behavior::Idle);
    let (handle, mut control) = ComponentHost::prepare(
        registry,
        options(Arc::new(Local::new())),
        vec![instance("app/wait", "test")],
    )
    .expect("prepare");
    control.prepared().await.expect("reports");

    let error = control.activate().await.expect_err("commit required");

    assert_eq!(error.code, HostErrorCode::Admission);
    assert_eq!(failure(terminal(&handle).await).0, error);
    while let Ok(record) = records.try_recv() {
        assert_ne!(record.stage, Stage::Run);
    }
}

#[tokio::test]
async fn dropping_staged_controller_cleans_committed_readiness() {
    let (registry, mut records, _gate) = fixture(Behavior::Idle);
    let engine = Arc::new(Local::new());
    let (handle, mut control) = ComponentHost::prepare(
        registry,
        options(engine.clone()),
        vec![instance("app/wait", "test")],
    )
    .expect("prepare");
    control.prepared().await.expect("reports");
    control.commit().await.expect("commit");

    drop(control);

    assert!(matches!(terminal(&handle).await, HostOutcome::Stopped));
    assert_eq!(
        engine
            .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
            .await
            .expect("withdrawn"),
        Vec::<Key>::new()
    );
    while let Ok(record) = records.try_recv() {
        assert_ne!(record.stage, Stage::Run);
    }
}

#[tokio::test]
async fn owner_abort_cancels_a_pending_prepare_hook() {
    let (registry, mut records, _gate) = fixture(Behavior::PrepareWait);
    let (handle, control) = ComponentHost::prepare(
        registry,
        options(Arc::new(Local::new())),
        vec![instance("app/wait", "test")],
    )
    .expect("prepare");
    stage(&mut records, Stage::Prepare).await;
    let error = HostError::new(HostErrorCode::Cancelled, None, "owner cancelled startup");

    control.abort(error.clone()).await;

    assert_eq!(failure(terminal(&handle).await).0, error);
    assert_eq!(
        stage(&mut records, Stage::Stop(StopReason::StartupFailure))
            .await
            .id,
        "app/wait"
    );
    assert_eq!(stage(&mut records, Stage::Dropped).await.id, "app/wait");
}

#[tokio::test]
async fn owner_silence_is_bounded_before_commit_and_before_activation() {
    for commit in [false, true] {
        let (registry, mut records, _gate) = fixture(Behavior::Idle);
        let engine = Arc::new(Local::new());
        let mut options = options(engine.clone());
        options.startup_timeout = Duration::from_secs(1);
        let (handle, mut control) =
            ComponentHost::prepare(registry, options, vec![instance("app/wait", "test")])
                .expect("prepare");
        control.prepared().await.expect("reports");
        if commit {
            control.commit().await.expect("commit");
        }

        let (primary, cleanup) = failure(terminal(&handle).await);

        assert_eq!(primary.code, HostErrorCode::Timeout);
        assert_eq!(cleanup, Vec::<HostError>::new());
        while let Ok(record) = records.try_recv() {
            assert_ne!(record.stage, Stage::Run);
        }
        assert_eq!(
            engine
                .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
                .await
                .expect("withdrawn"),
            Vec::<Key>::new()
        );
    }
}

#[tokio::test]
async fn owner_abort_after_run_release_stops_the_running_group() {
    let (registry, mut records, _gate) = fixture(Behavior::Idle);
    let (handle, mut control) = ComponentHost::prepare(
        registry,
        options(Arc::new(Local::new())),
        vec![instance("app/run", "test")],
    )
    .expect("prepare");
    control.prepared().await.expect("reports");
    control.commit().await.expect("commit");
    control.activate().await.expect("release");
    stage(&mut records, Stage::Run).await;
    let error = HostError::new(
        HostErrorCode::Activate,
        None,
        "peer host lost during release",
    );

    control.abort(error.clone()).await;

    assert_eq!(failure(terminal(&handle).await).0, error);
    assert_eq!(
        stage(&mut records, Stage::Stop(StopReason::ComponentFailure))
            .await
            .id,
        "app/run"
    );
    assert_eq!(stage(&mut records, Stage::Dropped).await.id, "app/run");
}
