use super::*;

#[tokio::test]
async fn named_input_receives_only_its_exact_forwarded_namespace() {
    // Given two same-type modules, one at the resolved forwarded source.
    let bus = Arc::new(Local::new());
    let receiver = consumer(bus.clone()).await;
    let selected = producer(bus.clone(), SENDER).await;
    let other = producer(bus, "deployment/other/device").await;
    let selected = selected
        .output::<Probe>("outgoing")
        .expect("selected output");
    let other = other.output::<Probe>("outgoing").expect("other output");
    let mut incoming = receiver.input::<Probe>("incoming").expect("named input");

    // When the other producer sends before the selected producer on the ordered bus.
    other.send(Probe { value: 10 }).await.expect("other send");
    selected
        .send(Probe { value: 20 })
        .await
        .expect("selected send");

    // Then the first delivered envelope identifies only the configured producer.
    let envelope = timeout(PATIENCE, incoming.recv_envelope())
        .await
        .expect("bounded receive")
        .expect("open input");
    assert_eq!(
        (envelope.source.as_str(), envelope.value.value),
        (SENDER, 20)
    );
    assert_eq!(incoming.stats().received, 1);
}

#[tokio::test]
async fn same_type_fanout_inputs_keep_separate_source_bindings() {
    // Given three same-type named inputs, including a parent and nested child namespace.
    let bus = Arc::new(Local::new());
    let sources = [
        "deployment/pong",
        "deployment/pong/child",
        "deployment/third",
    ];
    let mut contract = bindings(RECEIVER);
    for (name, source) in ["first", "second", "third"].into_iter().zip(sources) {
        contract.inputs.insert(name.to_string(), input(source));
    }
    let receiver = Cloudy::open(options(bus.clone(), contract))
        .await
        .expect("receiver");
    let mut inputs = ["first", "second", "third"]
        .map(|name| receiver.input::<Probe>(name).expect("named input"));
    let mut producers = Vec::new();
    for source in sources {
        let producer = producer(bus.clone(), source).await;
        producers.push(producer.output::<Probe>("outgoing").expect("output"));
    }

    // When every producer sends its own value.
    for (publisher, value) in producers.iter().zip(1..=3) {
        publisher.send(Probe { value }).await.expect("send");
    }

    // Then each named port receives exactly its configured source, not another same-type input.
    for ((incoming, source), value) in inputs.iter_mut().zip(sources).zip(1..=3) {
        let envelope = timeout(PATIENCE, incoming.recv_envelope())
            .await
            .expect("bounded receive")
            .expect("open input");
        assert_eq!(
            (envelope.source.as_str(), envelope.value.value),
            (source, value)
        );
    }
}

#[tokio::test]
async fn managed_input_rejects_missing_malformed_and_mismatching_schema() {
    // Given a raw publisher at the exact bound namespace and a verified named input.
    let bus = Arc::new(Local::new());
    let receiver = consumer(bus.clone()).await;
    let mut incoming = receiver.input::<Probe>("incoming").expect("input");
    let publisher = bus
        .publisher(
            &Key::topic(DOMAIN, Some(SENDER), Probe::TYPE),
            &Qos::DEFAULT,
        )
        .expect("raw publisher");

    // When unverified samples precede the verified sentinel on the same ordered publisher.
    for attachment in [None, Some(vec![1]), Some(0x9999_u64.to_le_bytes().to_vec())] {
        publisher
            .put(Probe { value: 1 }.encode_to_vec(), attachment)
            .expect("invalid sample");
    }
    publisher
        .put(
            Probe { value: 2 }.encode_to_vec(),
            Some(0x1234_u64.to_le_bytes().to_vec()),
        )
        .expect("verified sample");

    // Then only the sample with the actual compiled schema is decoded.
    assert_eq!(
        timeout(PATIENCE, incoming.recv())
            .await
            .expect("bounded receive"),
        Some(Probe { value: 2 })
    );
}

#[tokio::test]
async fn named_ports_fail_without_bindings_instead_of_using_wildcards() {
    // Given a standalone Cloudy with no managed contract.
    let mut standalone = RuntimeOptions::new(RECEIVER);
    standalone.engine = Some(Arc::new(Local::new()));
    standalone.module_report_path = None;
    let cloudy = Cloudy::open(standalone).await.expect("standalone");

    // When named ports are requested, then both directions reject the absent mapping.
    assert!(cloudy.input::<Probe>("incoming").is_err());
    assert!(cloudy.output::<Probe>("outgoing").is_err());
}

#[tokio::test]
async fn undefined_and_wrong_type_ports_fail_before_registration() {
    // Given one declared input whose protobuf FQN is not Probe.
    let mut contract = bindings(RECEIVER);
    contract.inputs.insert(
        "incoming".to_string(),
        InputBinding {
            contract: super::contract("other.Probe"),
            sources: vec![SENDER.to_string()],
        },
    );
    let cloudy = Cloudy::open(options(Arc::new(Local::new()), contract))
        .await
        .expect("cloudy");

    // When an undefined name or the wrong FQN is used, then no port can satisfy readiness.
    assert!(cloudy.input::<Probe>("undefined").is_err());
    assert!(cloudy.input::<Probe>("incoming").is_err());
    assert!(cloudy.ready().is_err());
}

#[tokio::test]
async fn managed_ports_reject_compiled_types_without_fingerprints() {
    // Given a declared short transport name, matching a hand-written unverified type.
    let mut contract = bindings(RECEIVER);
    contract.inputs.insert(
        "incoming".to_string(),
        InputBinding {
            contract: super::contract("Probe"),
            sources: vec![SENDER.to_string()],
        },
    );
    contract.outputs.insert(
        "outgoing".to_string(),
        OutputBinding {
            contract: super::contract("Probe"),
        },
    );
    let cloudy = Cloudy::open(options(Arc::new(Local::new()), contract))
        .await
        .expect("cloudy");

    // When both directions use an unverified type, then neither is accepted.
    assert!(cloudy.input::<Bare>("incoming").is_err());
    assert!(cloudy.output::<Bare>("outgoing").is_err());
}

#[tokio::test]
async fn duplicate_transport_outputs_fail_even_when_declared_names_differ() {
    // Given distinct short and qualified declarations that resolve to the same transport TYPE.
    let mut contract = bindings(SENDER);
    for (name, ty) in [("short", "Probe"), ("qualified", "test.Probe")] {
        contract.outputs.insert(
            name.to_string(),
            OutputBinding {
                contract: super::contract(ty),
            },
        );
    }
    let cloudy = Cloudy::open(options(Arc::new(Local::new()), contract))
        .await
        .expect("cloudy");
    let _first = cloudy.output::<Probe>("short").expect("first output");

    // When the second alias is created, then the executable cannot publish ambiguously.
    assert!(cloudy.output::<Probe>("qualified").is_err());
}
