use super::*;

#[test]
fn report_rejects_invalid_bindings_before_report_identity() {
    let [mut source, _] = stream_graph();
    source.bindings.version = 0;
    source.report.version = 0;

    let error = validate_report(&source.bindings, &source.report).unwrap_err();

    assert_eq!(error.code, ReportErrorCode::InvalidBindings);
    assert_eq!(error.module.as_deref(), Some("robot/sensor"));
    assert_eq!((error.side, error.port, error.source), (None, None, None));
}

#[test]
fn report_rejects_each_identity_mismatch() {
    let cases: [(ReportErrorCode, Mutation<ModuleReport>); 4] = [
        (ReportErrorCode::ReportVersion, |report| report.version = 0),
        (ReportErrorCode::FingerprintVersion, |report| {
            report.fingerprint_version = 0;
        }),
        (ReportErrorCode::Namespace, |report| {
            report.namespace = "robot/other".into();
        }),
        (ReportErrorCode::EndpointNamespace, |report| {
            report.endpoint_namespace = "robot/other".into();
        }),
    ];
    for (expected, mutate) in cases {
        let [mut source, _] = stream_graph();
        mutate(&mut source.report);

        let error = validate_report(&source.bindings, &source.report).unwrap_err();

        assert_eq!(error.code, expected);
        assert_eq!(error.module.as_deref(), Some("robot/sensor"));
        assert_eq!(error.port, None);
    }
}

#[test]
fn report_requires_exact_input_and_output_sets() {
    for input in [true, false] {
        for (remove, add) in [(true, false), (false, true), (true, true)] {
            let [source, receiver] = stream_graph();
            let mut fixture = if input { receiver } else { source };
            let ports = if input {
                &mut fixture.report.inputs
            } else {
                &mut fixture.report.outputs
            };
            let name = if input { "state" } else { "left" };
            let port = ports[name].clone();
            if remove {
                ports.remove(name);
            }
            if add {
                ports.insert("extra".into(), port);
            }

            let error = validate_report(&fixture.bindings, &fixture.report).unwrap_err();

            assert_eq!(error.code, ReportErrorCode::PortSet);
            assert_eq!(
                error.side,
                Some(if input {
                    ReportPortSide::Input
                } else {
                    ReportPortSide::Output
                })
            );
            assert_eq!(
                error.port.as_deref(),
                Some(if add { "extra" } else { name })
            );
        }
    }
}

#[test]
fn report_rejects_changed_exact_sources_and_output_addresses() {
    let cases: [(bool, Mutation<PortEndpoint>); 8] = [
        (true, |endpoint| {
            *endpoint = PortEndpoint::Input {
                sources: vec![address("left")],
            }
        }),
        (true, |endpoint| {
            *endpoint = PortEndpoint::Input {
                sources: vec![address("right"), address("left")],
            }
        }),
        (true, |endpoint| {
            *endpoint = PortEndpoint::Input {
                sources: vec![address("left"), address("missing")],
            }
        }),
        (true, |endpoint| {
            *endpoint = PortEndpoint::Input {
                sources: vec![
                    EndpointAddress {
                        namespace: "robot/other".into(),
                        port: "left".into(),
                    },
                    address("right"),
                ],
            }
        }),
        (true, |endpoint| {
            *endpoint = PortEndpoint::Output {
                address: address("state"),
            }
        }),
        (false, |endpoint| {
            *endpoint = PortEndpoint::Input {
                sources: vec![address("left")],
            }
        }),
        (false, |endpoint| {
            *endpoint = PortEndpoint::Output {
                address: address("right"),
            }
        }),
        (false, |endpoint| {
            *endpoint = PortEndpoint::Output {
                address: EndpointAddress {
                    namespace: "robot/other".into(),
                    port: "left".into(),
                },
            }
        }),
    ];
    for (input, mutate) in cases {
        let [source, receiver] = stream_graph();
        let mut fixture = if input { receiver } else { source };
        let name = if input { "state" } else { "left" };
        let ports = if input {
            &mut fixture.report.inputs
        } else {
            &mut fixture.report.outputs
        };
        mutate(&mut ports.get_mut(name).unwrap().endpoint);

        let error = validate_report(&fixture.bindings, &fixture.report).unwrap_err();

        assert_eq!(error.code, ReportErrorCode::Endpoint);
        assert_eq!(
            error.side,
            Some(if input {
                ReportPortSide::Input
            } else {
                ReportPortSide::Output
            })
        );
        assert_eq!(error.port.as_deref(), Some(name));
    }
}

#[test]
fn report_rejects_changed_effective_contract_fields() {
    let cases: [Mutation<EndpointContract>; 7] = [
        |contract| contract.type_name = "other.State".into(),
        |contract| contract.kind = PortKind::Rpc,
        |contract| contract.response = Some("robot.Result".into()),
        |contract| contract.qos = QosProfile::Sensor,
        |contract| contract.retention = Retention::Last,
        |contract| contract.replay = Replay::Last,
        |contract| contract.buffer = ReceiveBuffer::Latest(2),
    ];
    for mutate in cases {
        let [mut source, _] = stream_graph();
        mutate(&mut source.report.outputs.get_mut("left").unwrap().contract);

        let error = validate_report(&source.bindings, &source.report).unwrap_err();

        assert_eq!(error.code, ReportErrorCode::Contract);
        assert_eq!(error.port.as_deref(), Some("left"));
    }
}

#[test]
fn report_checks_compiled_names_and_rpc_response_shape_in_both_directions() {
    for input in [true, false] {
        let cases: [(ReportErrorCode, Mutation<PortReport>); 3] = [
            (ReportErrorCode::Type, |port| {
                port.type_name = "other.State".into();
            }),
            (ReportErrorCode::Response, |port| port.response = None),
            (ReportErrorCode::Response, |port| {
                port.response.as_mut().unwrap().type_name = "other.Result".into();
            }),
        ];
        for (expected, mutate) in cases {
            let [source, receiver] = rpc_graph();
            let mut fixture = if input { receiver } else { source };
            let name = if input { "state" } else { "left" };
            let ports = if input {
                &mut fixture.report.inputs
            } else {
                &mut fixture.report.outputs
            };
            mutate(ports.get_mut(name).unwrap());

            let error = validate_report(&fixture.bindings, &fixture.report).unwrap_err();

            assert_eq!(error.code, expected);
            assert_eq!(
                error.side,
                Some(if input {
                    ReportPortSide::Input
                } else {
                    ReportPortSide::Output
                })
            );
            assert_eq!(error.port.as_deref(), Some(name));
        }
    }
}

#[test]
fn stream_report_rejects_a_compiled_response() {
    let [mut source, _] = stream_graph();
    source.report.outputs.get_mut("left").unwrap().response = Some(ResponseReport {
        type_name: "robot.Result".into(),
        schema: 31,
    });

    let error = validate_report(&source.bindings, &source.report).unwrap_err();

    assert_eq!(error.code, ReportErrorCode::Response);
}

#[test]
fn delegated_child_report_keeps_parent_endpoint_identity() {
    let [mut source, _] = stream_graph();
    source.bindings.namespace = "robot/sensor/owned/worker".into();
    source.bindings.executables.push("worker".into());
    let child = fixture(source.bindings);

    let result = validate_report(&child.bindings, &child.report);

    assert_eq!(result, Ok(()));
}

#[test]
fn report_selects_first_failure_by_direction_then_port_name() {
    let [mut source, receiver] = stream_graph();
    source.bindings.inputs = receiver.bindings.inputs;
    let mut fixture = fixture(source.bindings);
    fixture.report.inputs.get_mut("state").unwrap().schema = 999;
    fixture.report.inputs.get_mut("state").unwrap().type_name = "other.State".into();
    fixture.report.outputs.get_mut("left").unwrap().type_name = "other.State".into();
    fixture.report.outputs.get_mut("right").unwrap().type_name = "other.State".into();

    let error = validate_report(&fixture.bindings, &fixture.report).unwrap_err();

    assert_eq!(error.code, ReportErrorCode::Type);
    assert_eq!(error.side, Some(ReportPortSide::Input));
    assert_eq!(error.port.as_deref(), Some("state"));
}
