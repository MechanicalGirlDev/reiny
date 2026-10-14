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
    let receiver = ModuleContext::open(options(bus.clone(), contract))
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
            &Key::output(DOMAIN, SENDER, "outgoing", Probe::TYPE_NAME),
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
    // Given a standalone ModuleContext with no managed contract.
    let mut standalone = RuntimeOptions::new(RECEIVER);
    standalone.engine = Some(Arc::new(Local::new()));
    standalone.module_report_path = None;
    // When the context opens, then the absent contract is rejected before any port exists.
    assert!(ModuleContext::open(standalone).await.is_err());
}

#[tokio::test]
async fn undefined_and_wrong_type_ports_fail_before_registration() {
    // Given one declared input whose protobuf FQN is not Probe.
    let mut contract = bindings(RECEIVER);
    contract.inputs.insert(
        "incoming".to_string(),
        InputBinding {
            contract: super::contract("other.Probe"),
            sources: vec![endpoint(SENDER, "outgoing")],
        },
    );
    let cloudy = ModuleContext::open(options(Arc::new(Local::new()), contract))
        .await
        .expect("cloudy");

    // When an undefined name or the wrong FQN is used, then no port can satisfy readiness.
    assert!(cloudy.input::<Probe>("undefined").is_err());
    assert!(cloudy.input::<Probe>("incoming").is_err());
    assert!(cloudy.ready().is_err());
}

#[tokio::test]
async fn managed_ports_reject_incompatible_compiled_topic_metadata() {
    // All Topics now carry fingerprints; incompatible type metadata still cannot bind.
    let mut contract = bindings(RECEIVER);
    contract.inputs.insert(
        "incoming".to_string(),
        InputBinding {
            contract: super::contract("test.Probe"),
            sources: vec![endpoint(SENDER, "outgoing")],
        },
    );
    contract.outputs.insert(
        "outgoing".to_string(),
        OutputBinding {
            contract: super::contract("test.Probe"),
        },
    );
    let cloudy = ModuleContext::open(options(Arc::new(Local::new()), contract))
        .await
        .expect("cloudy");

    assert!(cloudy.input::<OtherProbe>("incoming").is_err());
    assert!(cloudy.output::<OtherProbe>("outgoing").is_err());
}

#[tokio::test]
async fn same_type_outputs_keep_exact_names_and_source_provenance() {
    // Given two output names with the same exact transport type.
    let mut contract = bindings(SENDER);
    for (name, ty) in [("first", "test.Probe"), ("second", "test.Probe")] {
        contract.outputs.insert(
            name.to_string(),
            OutputBinding {
                contract: super::contract(ty),
            },
        );
    }
    let bus = Arc::new(Local::new());
    let source = ModuleContext::open(options(bus.clone(), contract))
        .await
        .expect("source");
    let first = source.output::<Probe>("first").expect("first output");
    let second = source.output::<Probe>("second").expect("second output");
    let mut binding = bindings(RECEIVER);
    for name in ["first", "second"] {
        binding.inputs.insert(
            name.into(),
            InputBinding {
                contract: super::contract(Probe::TYPE_NAME),
                sources: vec![endpoint(SENDER, name)],
            },
        );
    }
    let receiver = ModuleContext::open(options(bus, binding))
        .await
        .expect("receiver");
    let mut first_in = receiver.input::<Probe>("first").expect("first input");
    let mut second_in = receiver
        .raw_input("second", Probe::TYPE_NAME, Probe::SCHEMA_FINGERPRINT)
        .expect("second input");
    source.ready().expect("source ready");
    receiver.ready().expect("receiver ready");
    second.send(Probe { value: 22 }).await.expect("second send");
    first.send(Probe { value: 11 }).await.expect("first send");
    let a = timeout(PATIENCE, first_in.recv_envelope())
        .await
        .expect("first deadline")
        .expect("first");
    let b = timeout(PATIENCE, second_in.recv())
        .await
        .expect("second deadline")
        .expect("second");
    assert_eq!(
        (a.source.as_str(), a.source_port.as_str(), a.value.value),
        (SENDER, "first", 11)
    );
    assert_eq!(
        (
            b.source.as_str(),
            b.source_port.as_str(),
            Probe::decode(b.payload.as_slice()).expect("decode").value
        ),
        (SENDER, "second", 22)
    );
    assert_eq!(first_in.stats().received, 1);
    assert_eq!(second_in.stats().received, 1);
}

#[tokio::test]
async fn retained_fan_in_and_discovery_keep_same_namespace_ports_distinct() {
    let bus = Arc::new(Local::new());
    let mut binding = bindings(SENDER);
    for name in ["first", "second"] {
        binding.outputs.insert(
            name.into(),
            OutputBinding {
                contract: EndpointContract {
                    retention: crate::bindings::Retention::Last,
                    ..contract(Probe::TYPE_NAME)
                },
            },
        );
    }
    let sender = ModuleContext::open(options(bus.clone(), binding))
        .await
        .expect("sender");
    let first = sender.output::<Probe>("first").expect("first");
    let second = sender.output::<Probe>("second").expect("second");
    first
        .send(Probe { value: 10 })
        .await
        .expect("first retained");
    second
        .send(Probe { value: 20 })
        .await
        .expect("second retained");
    sender.ready().expect("ready");
    let mut binding = bindings(RECEIVER);
    binding.inputs.insert(
        "both".into(),
        InputBinding {
            contract: EndpointContract {
                replay: crate::bindings::Replay::Last,
                ..contract(Probe::TYPE_NAME)
            },
            sources: vec![endpoint(SENDER, "first"), endpoint(SENDER, "second")],
        },
    );
    let receiver = ModuleContext::open(options(bus.clone(), binding))
        .await
        .expect("receiver");
    let mut incoming = receiver.input::<Probe>("both").expect("fan in");
    let mut ready = receiver
        .watch_input_ready("both")
        .expect("module ready watch");
    assert_eq!(
        timeout(PATIENCE, ready.recv()).await.expect("ready event"),
        Some(Presence::Joined(Key::module(
            DOMAIN,
            Some(SENDER),
            "@ready"
        )))
    );
    drop(first);
    assert_eq!(
        timeout(PATIENCE, ready.recv())
            .await
            .expect("one module departure"),
        Some(Presence::Left(Key::module(DOMAIN, Some(SENDER), "@ready")))
    );
    // The vanished first source must not suppress the other port's retained value.
    let retained = timeout(PATIENCE, incoming.recv_envelope())
        .await
        .expect("retained")
        .expect("sample");
    assert_eq!(
        (retained.source_port.as_str(), retained.value.value),
        ("second", 20)
    );
    assert_eq!(
        receiver.publishers::<Probe>().await.expect("publishers"),
        vec![endpoint(SENDER, "second")]
    );
    let schemas = receiver.schemas(Probe::TYPE_NAME).await.expect("schemas");
    assert!(
        schemas
            .iter()
            .any(|schema| schema.source == SENDER && schema.source_port == "second")
    );
    assert!(
        schemas
            .iter()
            .any(|schema| schema.source == RECEIVER && schema.source_port == "both")
    );
    drop(second);
}

#[tokio::test]
async fn both_retained_outputs_in_one_namespace_replay_without_deduplicating_ports() {
    let bus = Arc::new(Local::new());
    let mut binding = bindings(SENDER);
    for name in ["first", "second"] {
        binding.outputs.insert(
            name.into(),
            OutputBinding {
                contract: EndpointContract {
                    retention: crate::bindings::Retention::Last,
                    ..contract(Probe::TYPE_NAME)
                },
            },
        );
    }
    let sender = ModuleContext::open(options(bus.clone(), binding))
        .await
        .expect("sender");
    let first = sender.output::<Probe>("first").expect("first");
    let second = sender.output::<Probe>("second").expect("second");
    first.send(Probe { value: 10 }).await.expect("send");
    second.send(Probe { value: 20 }).await.expect("send");
    let mut binding = bindings(RECEIVER);
    binding.inputs.insert(
        "both".into(),
        InputBinding {
            contract: EndpointContract {
                replay: crate::bindings::Replay::Last,
                ..contract(Probe::TYPE_NAME)
            },
            sources: vec![endpoint(SENDER, "first"), endpoint(SENDER, "second")],
        },
    );
    let receiver = ModuleContext::open(options(bus, binding))
        .await
        .expect("receiver");
    let mut incoming = receiver.input::<Probe>("both").expect("fan in");
    let mut actual = BTreeMap::new();
    for _ in 0..2 {
        let sample = timeout(PATIENCE, incoming.recv_envelope())
            .await
            .expect("replay")
            .expect("sample");
        actual.insert(sample.source_port, sample.value.value);
    }
    assert_eq!(
        actual,
        BTreeMap::from([("first".into(), 10), ("second".into(), 20)])
    );
    assert_eq!(
        receiver.publishers::<Probe>().await.expect("publishers"),
        vec![endpoint(SENDER, "first"), endpoint(SENDER, "second")]
    );
}
