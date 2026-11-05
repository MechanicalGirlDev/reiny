use super::*;
use crate::bindings::{ReportEntry, validate_graph};

#[tokio::test]
async fn opened_ports_preserve_application_semantics_in_compiled_reports() {
    let mut graph = graph(Role::Source);
    let semantics = std::collections::BTreeMap::from([
        ("units".into(), "app.si.v1".into()),
        ("ordering".into(), "app.xy.v1".into()),
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
    let authority = graph
        .instances
        .iter()
        .map(|spec| spec.bindings.clone())
        .collect::<Vec<_>>();
    let (handle, mut control) = ComponentHost::prepare(
        graph.registry,
        options(Arc::new(Local::new())),
        graph.instances,
    )
    .expect("prepare");
    let reports = control.prepared().await.expect("compiled reports");
    for report in &reports {
        for (name, port) in report.inputs.iter().chain(&report.outputs) {
            if name != "unrelated" {
                assert_eq!(port.contract.semantics, semantics);
                assert_eq!(port.schema, Probe::SCHEMA_FINGERPRINT);
            }
        }
        let encoded = serde_json::to_string(report).expect("serialize");
        let decoded: crate::bindings::ModuleReport =
            serde_json::from_str(&encoded).expect("report roundtrip");
        assert_eq!(&decoded, report);
    }
    let entries = authority
        .iter()
        .map(|bindings| ReportEntry {
            bindings,
            report: reports
                .iter()
                .find(|report| report.namespace == bindings.namespace)
                .expect("matching report"),
        })
        .collect::<Vec<_>>();
    validate_graph(&entries).expect("matching graph");
    assert!(graph.records.try_recv().is_err());
    control.commit().await.expect("commit");
    control.activate().await.expect("activate");
    let delivery = timeout(PATIENCE, graph.deliveries.recv())
        .await
        .expect("deadline")
        .expect("delivery");
    assert_eq!(delivery.value.value, 42);
    assert!(matches!(handle.stop().await, HostOutcome::Stopped));
}

#[tokio::test]
async fn separate_host_reports_admit_only_as_one_validated_graph() {
    for role in [Role::Source, Role::WrongSchema] {
        let Graph {
            registry,
            instances,
            payload,
            mut deliveries,
            mut records,
            gate: _gate,
        } = graph(role);
        let engine = Arc::new(Local::new());
        let mut hosts = Vec::new();
        let mut controls = Vec::new();
        let mut reports = Vec::new();
        let mut authority = Vec::new();
        for instance in instances {
            authority.push(instance.bindings.clone());
            let (handle, mut control) =
                ComponentHost::prepare(registry.clone(), options(engine.clone()), vec![instance])
                    .expect("prepare assigned subset");
            reports.extend(control.prepared().await.expect("subset reports"));
            hosts.push(handle);
            controls.push(control);
        }
        let entries = authority
            .iter()
            .zip(&reports)
            .map(|(bindings, report)| ReportEntry { bindings, report })
            .collect::<Vec<_>>();
        // The sink subset is not closed. Only the deployment-wide report set is.
        assert!(validate_graph(&entries[1..]).is_err());
        let validated = validate_graph(&entries);
        match role {
            Role::Source => {
                validated.expect("complete graph");
                for control in &mut controls {
                    control.commit().await.expect("commit");
                }
                assert!(
                    hosts
                        .iter()
                        .all(|host| host.status().phase == HostPhase::Committed)
                );
                assert!(records.try_recv().is_err());
                for control in &mut controls {
                    control
                        .activate()
                        .await
                        .expect("activate after every commit");
                }
                let envelope = timeout(PATIENCE, deliveries.recv())
                    .await
                    .expect("delivery deadline")
                    .expect("delivery");
                assert!(Arc::ptr_eq(&envelope.value, &payload));
                assert_eq!(envelope.source, "app/source");
                assert_eq!(envelope.source_port, "out");
                for host in &hosts {
                    assert!(matches!(host.stop().await, HostOutcome::Stopped));
                }
            }
            Role::WrongSchema => {
                let report = validated.expect_err("compiled schemas differ");
                let rejected = HostError {
                    code: HostErrorCode::Graph,
                    instance: report.module.clone(),
                    message: report.to_string(),
                    report: Some(Box::new(report)),
                };
                for control in &controls {
                    control.abort(rejected.clone()).await;
                }
                for host in &hosts {
                    assert_eq!(failure(terminal(host).await).0, rejected);
                }
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
            Role::Sink | Role::MissingOutput | Role::LoseOutput => {
                unreachable!("fixture source role")
            }
        }
    }
}
