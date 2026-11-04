use super::*;

#[tokio::test]
async fn failures_stop_peers_and_skip_panicked_components_cleanup() {
    for (behavior, code, panicked, startup) in [
        (Behavior::FactoryError, HostErrorCode::Factory, false, true),
        (Behavior::FactoryPanic, HostErrorCode::Panic, true, true),
        (Behavior::PrepareError, HostErrorCode::Prepare, false, true),
        (Behavior::PreparePanic, HostErrorCode::Panic, true, true),
        (Behavior::RunError, HostErrorCode::Run, false, false),
        (Behavior::RunPanic, HostErrorCode::Panic, true, false),
        (
            Behavior::RunReturn,
            HostErrorCode::UnexpectedExit,
            false,
            false,
        ),
    ] {
        let (registry, mut records, _gate) = fixture(behavior);
        let handle = ComponentHost::launch(
            registry,
            options(Arc::new(Local::new())),
            vec![instance("app/peer", "peer"), instance("app/fault", "test")],
        )
        .expect("launch");

        let (primary, cleanup) = failure(terminal(&handle).await);

        assert_eq!(primary.code, code, "{behavior:?}");
        assert_eq!(primary.instance.as_deref(), Some("app/fault"));
        assert!(cleanup.is_empty(), "{cleanup:?}");
        assert_eq!(handle.status().phase, HostPhase::Failed);
        let mut observed = Vec::new();
        while let Ok(record) = records.try_recv() {
            observed.push(record);
        }
        if startup {
            assert!(observed.iter().all(|record| record.stage != Stage::Run));
        }
        if panicked {
            assert!(observed.iter().all(|record| {
                record.id != "app/fault" || !matches!(record.stage, Stage::Stop(_))
            }));
        }
        for id in ["app/fault", "app/peer"] {
            if observed
                .iter()
                .any(|record| record.id == id && record.stage == Stage::Prepare)
            {
                assert!(
                    observed
                        .iter()
                        .any(|record| record.id == id && record.stage == Stage::Dropped)
                );
                if id == "app/peer" || !panicked {
                    let reason = if startup {
                        StopReason::StartupFailure
                    } else {
                        StopReason::ComponentFailure
                    };
                    assert!(
                        observed
                            .iter()
                            .any(|record| record.id == id && record.stage == Stage::Stop(reason))
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn concurrent_stop_cancels_preparation_and_shares_terminal_result() {
    let (registry, mut records, _gate) = fixture(Behavior::PrepareWait);
    let handle = ComponentHost::launch(
        registry,
        options(Arc::new(Local::new())),
        vec![instance("app/wait", "test")],
    )
    .expect("launch");
    stage(&mut records, Stage::Prepare).await;

    let (first, second, running) = timeout(PATIENCE, async {
        tokio::join!(handle.stop(), handle.stop(), handle.wait_running())
    })
    .await
    .expect("concurrent stop deadline");

    assert!(matches!(first, HostOutcome::Stopped));
    assert!(matches!(second, HostOutcome::Stopped));
    assert_eq!(
        running.expect_err("startup cancelled").code,
        HostErrorCode::Cancelled
    );
    assert_eq!(
        stage(&mut records, Stage::Stop(StopReason::Requested))
            .await
            .id,
        "app/wait"
    );
    assert_eq!(
        handle.status().components["app/wait"].phase,
        ComponentPhase::Stopped
    );
}

#[tokio::test]
async fn stop_hook_failure_is_not_reported_as_clean_stop() {
    for (behavior, code) in [
        (Behavior::StopError, HostErrorCode::StopHook),
        (Behavior::StopPanic, HostErrorCode::Panic),
    ] {
        let (registry, _records, _gate) = fixture(behavior);
        let handle = ComponentHost::launch(
            registry,
            options(Arc::new(Local::new())),
            vec![instance("app/stop", "test")],
        )
        .expect("launch");
        handle.wait_running().await.expect("running");

        let (primary, cleanup) = failure(timeout(PATIENCE, handle.stop()).await.expect("stop"));

        assert_eq!(primary.code, code);
        assert_eq!(cleanup, Vec::<HostError>::new());
        assert_eq!(
            handle.status().components["app/stop"].phase,
            ComponentPhase::Failed
        );
    }
}

#[tokio::test]
async fn primary_failure_survives_peer_cleanup_failure() {
    let (records, _rx) = mpsc::channel(128);
    let (_gate, gate_rx) = watch::channel(false);
    let mut registry = ComponentRegistry::new();
    for (name, behavior) in [("run", Behavior::RunError), ("stop", Behavior::StopError)] {
        registry
            .register(
                name.to_owned(),
                Arc::new(Factory {
                    behavior,
                    records: records.clone(),
                    gate: gate_rx.clone(),
                }),
            )
            .expect("register");
    }
    let handle = ComponentHost::launch(
        Arc::new(registry),
        options(Arc::new(Local::new())),
        vec![
            instance("app/fault", "run"),
            instance("app/cleanup", "stop"),
        ],
    )
    .expect("launch");

    let (primary, cleanup) = failure(terminal(&handle).await);

    assert_eq!(primary.code, HostErrorCode::Run);
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].code, HostErrorCode::StopHook);
    assert_eq!(cleanup[0].instance.as_deref(), Some("app/cleanup"));
}

#[tokio::test]
async fn dropping_handle_requests_cleanup_without_an_explicit_stop() {
    let (registry, mut records, _gate) = fixture(Behavior::Idle);
    let handle = ComponentHost::launch(
        registry,
        options(Arc::new(Local::new())),
        vec![instance("app/drop", "test")],
    )
    .expect("launch");
    handle.wait_running().await.expect("running");
    let mut status = handle.subscribe();

    drop(handle);

    stage(&mut records, Stage::Stop(StopReason::Requested)).await;
    timeout(
        PATIENCE,
        status.wait_for(|status| status.phase == HostPhase::Stopped),
    )
    .await
    .expect("drop cleanup deadline")
    .expect("final status");
}
