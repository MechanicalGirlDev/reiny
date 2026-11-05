use super::*;

#[tokio::test]
async fn semantic_assertions_are_reported_and_admitted_before_run() {
    for compatible in [true, false] {
        let mut graph = graph(Role::Source);
        let semantics = std::collections::BTreeMap::from([
            ("units".into(), "example.si.v1".into()),
            ("identity".into(), "example.target-a.v1".into()),
            ("ordering".into(), "example.xy.v1".into()),
            ("time".into(), "example.monotonic-observation.v1".into()),
            ("update".into(), "example.every-step.v1".into()),
        ]);
        graph.instances[0]
            .bindings
            .outputs
            .get_mut("out")
            .expect("output")
            .contract
            .semantics = semantics.clone();
        graph.instances[1]
            .bindings
            .inputs
            .get_mut("in")
            .expect("input")
            .contract
            .semantics = semantics.clone();
        if !compatible {
            graph.instances[1]
                .bindings
                .inputs
                .get_mut("in")
                .expect("input")
                .contract
                .semantics
                .insert("units".into(), "example.other.v1".into());
        }
        let engine = Arc::new(Local::new());
        let handle =
            ComponentHost::launch(graph.registry, options(engine.clone()), graph.instances)
                .expect("launch");
        if compatible {
            handle.wait_running().await.expect("admitted");
            let delivery = timeout(PATIENCE, graph.deliveries.recv())
                .await
                .expect("deadline")
                .expect("delivery");
            assert_eq!(delivery.value.value, 42);
            assert_eq!(delivery.source_port, "out");
            assert!(matches!(handle.stop().await, HostOutcome::Stopped));
        } else {
            let (primary, cleanup) = failure(terminal(&handle).await);
            assert_eq!(
                primary.report.expect("graph error").code,
                ReportErrorCode::Semantics
            );
            assert_eq!(cleanup, Vec::<HostError>::new());
            while let Ok(record) = graph.records.try_recv() {
                assert!(!matches!(record.stage, Stage::Run));
            }
            assert_eq!(
                engine
                    .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
                    .await
                    .expect("presence"),
                Vec::<Key>::new()
            );
        }
    }
}

#[tokio::test]
async fn two_components_deliver_shared_payload_only_from_the_named_source() {
    let mut graph = graph(Role::Source);
    let engine = Arc::new(Local::new());
    let handle = ComponentHost::launch(graph.registry, options(engine.clone()), graph.instances)
        .expect("launch");

    let delivery = timeout(PATIENCE, graph.deliveries.recv())
        .await
        .expect("delivery deadline")
        .expect("delivery");

    assert!(Arc::ptr_eq(&delivery.value, &graph.payload));
    assert_eq!(delivery.value.value, 42);
    assert_eq!(delivery.source, "app/source");
    assert_eq!(delivery.source_port, "out");
    let ready = engine
        .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
        .await
        .expect("ready presence");
    assert_eq!(ready.len(), 2);
    assert!(matches!(handle.stop().await, HostOutcome::Stopped));
    assert_eq!(
        engine
            .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
            .await
            .expect("withdrawn readiness"),
        Vec::<Key>::new()
    );
}

#[tokio::test]
async fn graph_errors_prevent_every_run_and_withdraw_all_readiness() {
    for (role, namespace, port, expected) in [
        (
            Role::WrongSchema,
            "app/source",
            "out",
            ReportErrorCode::Schema,
        ),
        (
            Role::Source,
            "app/absent",
            "out",
            ReportErrorCode::MissingSourceModule,
        ),
        (
            Role::Source,
            "app/source",
            "absent",
            ReportErrorCode::MissingSourcePort,
        ),
    ] {
        let mut graph = graph(role);
        graph.instances[1]
            .bindings
            .inputs
            .get_mut("in")
            .expect("input")
            .sources[0] = EndpointAddress {
            namespace: namespace.to_owned(),
            port: port.to_owned(),
        };
        let engine = Arc::new(Local::new());
        let handle =
            ComponentHost::launch(graph.registry, options(engine.clone()), graph.instances)
                .expect("launch");

        let (primary, cleanup) = failure(terminal(&handle).await);

        assert_eq!(primary.code, HostErrorCode::Graph);
        assert_eq!(
            primary.report.expect("structured graph failure").code,
            expected
        );
        assert_eq!(cleanup, Vec::<HostError>::new());
        while let Ok(record) = graph.records.try_recv() {
            assert!(!matches!(record.stage, Stage::Run));
        }
        assert_eq!(
            engine
                .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
                .await
                .expect("readiness"),
            Vec::<Key>::new()
        );
    }
}

#[tokio::test]
async fn missing_live_port_fails_admission_before_activation() {
    let graph = graph(Role::MissingOutput);
    let handle = ComponentHost::launch(
        graph.registry,
        options(Arc::new(Local::new())),
        graph.instances,
    )
    .expect("launch");

    let (primary, _) = failure(terminal(&handle).await);

    assert_eq!(primary.code, HostErrorCode::Admission);
    assert_eq!(primary.instance.as_deref(), Some("app/source"));
}

#[tokio::test]
async fn losing_an_admitted_port_stops_the_group() {
    let mut graph = graph(Role::LoseOutput);
    let handle = ComponentHost::launch(
        graph.registry,
        options(Arc::new(Local::new())),
        graph.instances,
    )
    .expect("launch");
    handle.wait_running().await.expect("running");
    stage(&mut graph.records, Stage::Run).await;

    graph.gate.send_replace(true);

    let (primary, _) = failure(terminal(&handle).await);
    assert_eq!(primary.code, HostErrorCode::ReadinessLost);
    assert_eq!(primary.instance.as_deref(), Some("app/source"));
    assert_eq!(
        stage(
            &mut graph.records,
            Stage::Stop(StopReason::ComponentFailure)
        )
        .await
        .stage,
        Stage::Stop(StopReason::ComponentFailure)
    );
}
