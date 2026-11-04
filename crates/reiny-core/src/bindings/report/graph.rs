use super::*;

#[test]
fn closed_graph_accepts_fan_in_rpc_and_aggregated_owned_ports() {
    let [mut source, receiver] = stream_graph();
    source.bindings.children.insert(
        "worker".into(),
        PortDelegation {
            inputs: Vec::new(),
            outputs: vec!["right".into()],
        },
    );
    source.bindings.executables.push("worker".into());
    let cases = [[source, receiver], rpc_graph()];

    for fixtures in cases {
        let result = check(&fixtures);

        assert_eq!(result, Ok(()));
    }
}

#[test]
fn empty_graph_has_no_module_or_port_context() {
    let error = validate_graph(&[]).unwrap_err();

    assert_eq!(error.code, ReportErrorCode::EmptyGraph);
    assert_eq!(
        (error.module, error.side, error.port, error.source),
        (None, None, None, None)
    );
}

#[test]
fn graph_validates_every_report_before_resolving_any_edges() {
    let [mut source, mut receiver] = stream_graph();
    source.bindings.namespace = "robot/other".into();
    source.bindings.endpoint_namespace = source.bindings.namespace.clone();
    source = fixture(source.bindings);
    receiver.report.version = 0;

    let error = check(&[source, receiver]).unwrap_err();

    assert_eq!(error.code, ReportErrorCode::ReportVersion);
    assert_eq!(error.module.as_deref(), Some("robot/control"));
}

#[test]
fn duplicate_namespaces_are_rejected_even_with_different_compiled_schemas() {
    for schema in [17, 99] {
        let [source, _] = stream_graph();
        let mut duplicate = fixture(source.bindings.clone());
        duplicate.report.outputs.get_mut("left").unwrap().schema = schema;

        let error = check(&[source, duplicate]).unwrap_err();

        assert_eq!(error.code, ReportErrorCode::DuplicateNamespace);
        assert_eq!(error.module.as_deref(), Some("robot/sensor"));
    }
}

#[test]
fn owned_child_is_not_a_top_level_graph_member() {
    let [mut source, _] = stream_graph();
    source.bindings.namespace = "robot/sensor/owned/worker".into();
    let child = fixture(source.bindings);

    let error = check(&[child]).unwrap_err();

    assert_eq!(error.code, ReportErrorCode::UnsupportedGraphMember);
    assert_eq!(error.module.as_deref(), Some("robot/sensor/owned/worker"));
}

#[test]
fn matching_type_and_schema_cannot_replace_the_exact_module_or_output() {
    for namespace in [true, false] {
        let [source, mut receiver] = stream_graph();
        let expected = if namespace {
            EndpointAddress {
                namespace: "robot/missing".into(),
                port: "left".into(),
            }
        } else {
            address("missing")
        };
        receiver.bindings.inputs.get_mut("state").unwrap().sources[0] = expected.clone();
        receiver = fixture(receiver.bindings);

        let error = check(&[source, receiver]).unwrap_err();

        assert_eq!(
            error.code,
            if namespace {
                ReportErrorCode::MissingSourceModule
            } else {
                ReportErrorCode::MissingSourcePort
            }
        );
        assert_eq!(error.module.as_deref(), Some("robot/control"));
        assert_eq!(error.side, Some(ReportPortSide::Input));
        assert_eq!(error.port.as_deref(), Some("state"));
        assert_eq!(error.source.as_deref(), Some(&expected));
    }
}

#[test]
fn graph_checks_request_and_response_names_and_fingerprints_independently() {
    let cases: [(ReportErrorCode, Mutation<Fixture>); 4] = [
        (ReportErrorCode::Type, |source| {
            source
                .bindings
                .outputs
                .get_mut("left")
                .unwrap()
                .contract
                .type_name = "other.State".into();
            source
                .report
                .outputs
                .get_mut("left")
                .unwrap()
                .contract
                .type_name = "other.State".into();
            source.report.outputs.get_mut("left").unwrap().type_name = "other.State".into();
        }),
        (ReportErrorCode::Schema, |source| {
            source.report.outputs.get_mut("left").unwrap().schema = 999;
        }),
        (ReportErrorCode::Response, |source| {
            source
                .bindings
                .outputs
                .get_mut("left")
                .unwrap()
                .contract
                .response = Some("other.Result".into());
            source
                .report
                .outputs
                .get_mut("left")
                .unwrap()
                .contract
                .response = Some("other.Result".into());
            source
                .report
                .outputs
                .get_mut("left")
                .unwrap()
                .response
                .as_mut()
                .unwrap()
                .type_name = "other.Result".into();
        }),
        (ReportErrorCode::Schema, |source| {
            source
                .report
                .outputs
                .get_mut("left")
                .unwrap()
                .response
                .as_mut()
                .unwrap()
                .schema = 999;
        }),
    ];
    for (expected, mutate) in cases {
        let [mut source, receiver] = rpc_graph();
        mutate(&mut source);

        let error = check(&[source, receiver]).unwrap_err();

        assert_eq!(error.code, expected);
        assert_eq!(error.module.as_deref(), Some("robot/control"));
        assert_eq!(error.source.as_deref(), Some(&address("left")));
    }
}

#[test]
fn same_named_request_cannot_connect_stream_and_rpc_endpoints() {
    for rpc_source in [true, false] {
        let [stream_source, mut stream_receiver] = stream_graph();
        stream_receiver
            .bindings
            .inputs
            .get_mut("state")
            .unwrap()
            .sources
            .truncate(1);
        stream_receiver = fixture(stream_receiver.bindings);
        let [rpc_server, rpc_client] = rpc_graph();
        let fixtures = if rpc_source {
            [rpc_server, stream_receiver]
        } else {
            [stream_source, rpc_client]
        };

        let error = check(&fixtures).unwrap_err();

        assert_eq!(error.code, ReportErrorCode::Policy);
        assert_eq!(error.source.as_deref(), Some(&address("left")));
    }
}

#[test]
fn replay_requires_retention_on_every_exact_source_not_matching_qos() {
    for replay in [Replay::Live, Replay::Last] {
        for retention in [Retention::Volatile, Retention::Last] {
            for qos in [QosProfile::Default, QosProfile::Sensor, QosProfile::Command] {
                let [mut source, mut receiver] = stream_graph();
                source
                    .bindings
                    .outputs
                    .get_mut("left")
                    .unwrap()
                    .contract
                    .retention = Retention::Last;
                let output = &mut source.bindings.outputs.get_mut("right").unwrap().contract;
                output.retention = retention;
                output.qos = qos;
                receiver
                    .bindings
                    .inputs
                    .get_mut("state")
                    .unwrap()
                    .contract
                    .replay = replay;
                let fixtures = [fixture(source.bindings), fixture(receiver.bindings)];

                let result = check(&fixtures);

                if replay == Replay::Last && retention == Retention::Volatile {
                    let error = result.unwrap_err();
                    assert_eq!(error.code, ReportErrorCode::Policy);
                    assert_eq!(error.source.as_deref(), Some(&address("right")));
                } else {
                    assert_eq!(result, Ok(()));
                }
            }
        }
    }
}

#[test]
fn fan_in_compares_each_schema_not_only_the_first_source() {
    let [mut source, receiver] = stream_graph();
    source.report.outputs.get_mut("right").unwrap().schema = 999;

    let error = check(&[source, receiver]).unwrap_err();

    assert_eq!(error.code, ReportErrorCode::Schema);
    assert_eq!(error.source.as_deref(), Some(&address("right")));
}
