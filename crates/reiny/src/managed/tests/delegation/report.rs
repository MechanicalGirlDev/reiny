use super::*;
use crate::bindings::ReportPortSide::{Input, Output};
use crate::bindings::{PortEndpoint, ReportError, ReportErrorCode as Code, ResponseReport};

#[tokio::test]
async fn delegated_partial_report_is_admitted_only_with_its_exact_compiled_contract() {
    // The host retains a port absent from the child's delegated subset; its source is external.
    let directory = Directory::new();
    let bus = Arc::new(Local::new());
    let mut binding = host_bindings();
    binding.outputs.insert(
        "local".into(),
        OutputBinding {
            contract: contract("test.Probe"),
        },
    );
    let (host, mut process, _stdin) = spawn_fixture(bus.clone(), &directory, binding).await;
    let _local = host.output::<Probe>("local").expect("host-owned output");
    let child = open_child(bus.clone(), &directory).await;
    let _incoming = child.input::<Probe>("incoming").expect("child input");
    let _outgoing = child.output::<Probe>("outgoing").expect("child output");
    announce_child_ready(&bus, &child).await;
    let path = directory.0.join("driver/report.json");
    let valid: ModuleReport =
        serde_json::from_slice(&std::fs::read(&path).expect("report")).expect("parse");
    assert_eq!(
        valid.inputs.keys().map(String::as_str).collect::<Vec<_>>(),
        ["incoming"]
    );
    assert_eq!(
        valid.outputs.keys().map(String::as_str).collect::<Vec<_>>(),
        ["outgoing"]
    );
    assert_eq!(valid.namespace, process.namespace());
    assert_eq!(valid.endpoint_namespace, SENDER);

    // Each malformed report must fail at owned-child admission and retain its structured cause.
    for (code, side, port) in [
        (Code::Namespace, None, None),
        (Code::EndpointNamespace, None, None),
        (Code::Endpoint, Some(Input), Some("incoming")),
        (Code::Type, Some(Output), Some("outgoing")),
        (Code::Response, Some(Output), Some("outgoing")),
        (Code::PortSet, Some(Output), Some("outgoing")),
        (Code::Contract, Some(Output), Some("outgoing")),
    ] {
        let mut report = valid.clone();
        let output = report.outputs.get_mut("outgoing").expect("output");
        match code {
            Code::Namespace => report.namespace = SENDER.into(),
            Code::EndpointNamespace => {
                report.endpoint_namespace = process.namespace().into();
            }
            Code::Endpoint => {
                report.inputs.get_mut("incoming").expect("input").endpoint = PortEndpoint::Input {
                    sources: vec![endpoint("deployment/other", "outgoing")],
                };
            }
            Code::Type => {
                output.type_name = "test.OtherProbe".into();
            }
            Code::Response => {
                output.response = Some(ResponseReport {
                    type_name: "test.Probe".into(),
                    schema: 0,
                });
            }
            Code::PortSet => {
                report.outputs.clear();
            }
            Code::Contract => {
                output.contract.qos = crate::bindings::QosProfile::Sensor;
            }
            _ => unreachable!("only the listed admission failures are exercised"),
        }
        std::fs::write(&path, serde_json::to_vec(&report).expect("serialize"))
            .expect("replace child report");
        let failure = timeout(PATIENCE, process.wait_ready())
            .await
            .expect("bounded admission")
            .expect_err("malformed report rejected");
        let structured = failure
            .downcast_ref::<ReportError>()
            .expect("structured report error");
        assert_eq!(
            (structured.code, structured.side, structured.port.as_deref()),
            (code, side, port)
        );
        assert_eq!(structured.module.as_deref(), Some(process.namespace()));
        assert!(host.ready().is_err());
    }

    // Restoring the partial report admits the child and lets the host aggregate the complete set.
    std::fs::write(path, serde_json::to_vec(&valid).expect("serialize")).expect("restore report");
    timeout(PATIENCE, process.wait_ready())
        .await
        .expect("bounded child readiness")
        .expect("partial delegated report accepted");
    host.ready()
        .expect("host aggregates child and local endpoint");
    timeout(PATIENCE, process.kill())
        .await
        .expect("reap")
        .expect("status");
}
