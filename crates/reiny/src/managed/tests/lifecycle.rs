use super::*;

#[tokio::test]
async fn readiness_requires_every_declared_port_to_have_a_live_handle() {
    // Given a declared input that has not been opened.
    let bus = Arc::new(Local::new());
    let cloudy = consumer(bus.clone()).await;
    let ready = Key::topic(DOMAIN, Some(RECEIVER), "@ready");
    assert_eq!(
        bus.alive(&ready, PATIENCE).await.expect("alive"),
        Vec::<Key>::new()
    );

    // When ready is called before creating the port, then no ready token is published.
    assert!(cloudy.ready().is_err());
    assert_eq!(
        bus.alive(&ready, PATIENCE).await.expect("alive"),
        Vec::<Key>::new()
    );
}

#[tokio::test]
async fn readiness_rejects_a_port_dropped_during_initialization() {
    // Given a declared input whose handle has already been dropped.
    let bus = Arc::new(Local::new());
    let cloudy = consumer(bus).await;
    drop(cloudy.input::<Probe>("incoming").expect("input"));

    // When ready is called, then historical creation is not mistaken for an active port.
    assert!(cloudy.ready().is_err());
}

#[tokio::test]
async fn readiness_requires_declared_outputs_as_well_as_inputs() {
    // Given a module that declared an output but has only opened its input.
    let bus = Arc::new(Local::new());
    let mut contract = bindings(RECEIVER);
    contract
        .inputs
        .insert("incoming".to_string(), input(SENDER));
    contract.outputs.insert(
        "outgoing".to_string(),
        OutputBinding {
            contract: super::contract("test.Probe"),
        },
    );
    let cloudy = Cloudy::open(options(bus, contract)).await.expect("module");
    let _incoming = cloudy.input::<Probe>("incoming").expect("input");

    // When readiness is requested, then the uncreated output is still mandatory.
    assert!(cloudy.ready().is_err());
}

#[tokio::test]
async fn ready_publishes_report_and_stop_only_acknowledges_request() {
    // Given a live named port and an observer registered before the readiness transition.
    let bus = Arc::new(Local::new());
    let directory = Directory::new();
    let report_path = directory.0.join("report.json");
    let mut contract = bindings(RECEIVER);
    contract
        .inputs
        .insert("incoming".to_string(), input(SENDER));
    contract.outputs.insert(
        "outgoing".to_string(),
        OutputBinding {
            contract: super::contract("Probe"),
        },
    );
    let mut opts = options(bus.clone(), contract);
    opts.module_report_path = Some(report_path.clone());
    let cloudy = Cloudy::open(opts).await.expect("managed module");
    let mut incoming = cloudy.input::<Probe>("incoming").expect("input");
    let _outgoing = cloudy.output::<Probe>("outgoing").expect("output");
    let ready = Key::topic(DOMAIN, Some(RECEIVER), "@ready");
    let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let _watch = bus
        .watch_alive(
            &ready,
            Box::new(move |event| {
                events.send(event).expect("observer remains alive");
            }),
        )
        .expect("watch before ready");

    // When initialization completes and a stop request follows the readiness notification.
    cloudy.ready().expect("ready");
    assert_eq!(
        timeout(PATIENCE, observed.recv())
            .await
            .expect("ready event"),
        Some(Presence::Joined(ready.clone()))
    );
    let report: ModuleReport =
        serde_json::from_slice(&std::fs::read(&report_path).expect("atomic report"))
            .expect("report JSON");
    assert_eq!(report.version, CONTRACT_VERSION);
    assert_eq!(report.namespace, RECEIVER);
    assert_eq!(report.inputs["incoming"].type_name, "test.Probe");
    assert_eq!(report.inputs["incoming"].schema, 0x1234);
    assert_eq!(report.outputs["outgoing"].type_name, "test.Probe");
    assert_eq!(report.outputs["outgoing"].schema, 0x1234);
    let mut replies = bus
        .query(
            &Key::topic(DOMAIN, Some(RECEIVER), "@stop"),
            QueryParams {
                payload: Some(Vec::new()),
                attachment: None,
                timeout: PATIENCE,
            },
        )
        .expect("stop request");
    let ack = timeout(PATIENCE, replies.next())
        .await
        .expect("bounded acknowledgement")
        .expect("acknowledgement exists")
        .expect("successful acknowledgement");

    // Then cooperative shutdown is visible, but only dropping the owner removes readiness.
    assert_eq!(ack.payload, b"ack");
    timeout(PATIENCE, cloudy.shutdown())
        .await
        .expect("shutdown");
    assert_eq!(
        timeout(PATIENCE, incoming.recv())
            .await
            .expect("input shutdown"),
        None
    );
    assert_eq!(
        bus.alive(&ready, PATIENCE).await.expect("still ready"),
        std::slice::from_ref(&ready)
    );
    assert!(cloudy.ready().is_err());
    drop(cloudy);
    assert_eq!(
        timeout(PATIENCE, observed.recv())
            .await
            .expect("owner departed"),
        Some(Presence::Left(ready))
    );
}

#[tokio::test]
async fn report_failure_prevents_readiness() {
    // Given an absolute report path whose parent does not exist.
    let bus = Arc::new(Local::new());
    let directory = Directory::new();
    let mut opts = options(bus.clone(), bindings(RECEIVER));
    opts.module_report_path = Some(directory.0.join("absent").join("report.json"));
    let cloudy = Cloudy::open(opts).await.expect("module");

    // When report publication fails, then the supervisor cannot see a ready token.
    assert!(cloudy.ready().is_err());
    assert_eq!(
        bus.alive(&Key::topic(DOMAIN, Some(RECEIVER), "@ready"), PATIENCE)
            .await
            .expect("alive"),
        Vec::<Key>::new()
    );
}

#[tokio::test]
async fn dropping_a_named_handle_after_ready_withdraws_readiness() {
    let bus = Arc::new(Local::new());
    let cloudy = producer(bus.clone(), SENDER).await;
    let output = cloudy.output::<Probe>("outgoing").expect("output");
    let key = Key::topic(DOMAIN, Some(SENDER), "@ready");
    let (tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    let _watch = bus
        .watch_alive(
            &key,
            Box::new(move |event| {
                let _ = tx.send(event);
            }),
        )
        .expect("watch");
    cloudy.ready().expect("ready");
    assert_eq!(
        timeout(PATIENCE, events.recv()).await.expect("join"),
        Some(Presence::Joined(key.clone()))
    );
    drop(output);
    assert_eq!(
        timeout(PATIENCE, events.recv()).await.expect("leave"),
        Some(Presence::Left(key))
    );
    timeout(PATIENCE, cloudy.shutdown())
        .await
        .expect("shutdown");
    assert!(cloudy.ready().is_err());
}
