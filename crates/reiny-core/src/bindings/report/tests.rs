//! Behavioral tests of report authority and compiled graph compatibility.

use crate::bindings::*;
use alloc::{collections::BTreeMap, vec, vec::Vec};

#[path = "graph.rs"]
mod graph;
#[path = "individual.rs"]
mod individual;
#[path = "ordering.rs"]
mod ordering;

#[test]
fn semantic_contracts_match_exactly_for_streams_rpc_and_every_fan_in_source() {
    for make in [stream_graph, rpc_graph] {
        let [source, receiver] = make();
        for key in ["units", "identity", "ordering", "time", "update"] {
            for value in [None, Some("app.v1"), Some("app.v2")] {
                let mut source = source.bindings.clone();
                let mut receiver = receiver.bindings.clone();
                for port in source.outputs.values_mut() {
                    port.contract.semantics.insert(key.into(), "app.v1".into());
                }
                if let Some(value) = value {
                    receiver
                        .inputs
                        .get_mut("state")
                        .unwrap()
                        .contract
                        .semantics
                        .insert(key.into(), value.into());
                }
                let fixtures = [fixture(source), fixture(receiver)];
                if value == Some("app.v1") {
                    check(&fixtures).unwrap();
                } else {
                    let error = check(&fixtures).unwrap_err();
                    assert_eq!(error.code, ReportErrorCode::Semantics);
                    assert_eq!(error.source.as_deref(), Some(&address("left")));
                }
            }
        }
    }
    let [mut source, mut receiver] = stream_graph();
    source
        .bindings
        .outputs
        .get_mut("left")
        .unwrap()
        .contract
        .semantics
        .insert("units".into(), "app.v1".into());
    receiver
        .bindings
        .inputs
        .get_mut("state")
        .unwrap()
        .contract
        .semantics
        .insert("units".into(), "app.v1".into());
    let fixtures = [fixture(source.bindings), fixture(receiver.bindings)];
    let error = check(&fixtures).unwrap_err();
    assert_eq!(error.code, ReportErrorCode::Semantics);
    assert_eq!(error.source.as_deref(), Some(&address("right")));
}

#[test]
fn report_cannot_erase_or_replace_declared_semantics() {
    let [mut source, _] = stream_graph();
    source
        .bindings
        .outputs
        .get_mut("left")
        .unwrap()
        .contract
        .semantics
        .insert("units".into(), "app.v1".into());
    assert_eq!(
        validate_report(&source.bindings, &source.report)
            .unwrap_err()
            .code,
        ReportErrorCode::Contract
    );
    let mut source = fixture(source.bindings);
    source
        .report
        .outputs
        .get_mut("left")
        .unwrap()
        .contract
        .semantics
        .insert("units".into(), "app.v2".into());
    assert_eq!(
        validate_report(&source.bindings, &source.report)
            .unwrap_err()
            .code,
        ReportErrorCode::Contract
    );
}

#[test]
fn semantics_require_the_same_key_set_but_never_choose_another_source() {
    let [mut source, mut receiver] = stream_graph();
    receiver
        .bindings
        .inputs
        .get_mut("state")
        .unwrap()
        .sources
        .truncate(1);
    source
        .bindings
        .outputs
        .get_mut("right")
        .unwrap()
        .contract
        .semantics
        .insert("units".into(), "app.v1".into());
    receiver
        .bindings
        .inputs
        .get_mut("state")
        .unwrap()
        .contract
        .semantics
        .insert("units".into(), "app.v1".into());
    let fixtures = [fixture(source.bindings), fixture(receiver.bindings)];
    let error = check(&fixtures).unwrap_err();
    assert_eq!(error.code, ReportErrorCode::Semantics);
    assert_eq!(error.source.as_deref(), Some(&address("left")));
}

type Mutation<T> = fn(&mut T);

struct Fixture {
    bindings: ModuleBindings,
    report: ModuleReport,
}

impl Fixture {
    fn entry(&self) -> ReportEntry<'_> {
        ReportEntry {
            bindings: &self.bindings,
            report: &self.report,
        }
    }
}

fn contract() -> EndpointContract {
    EndpointContract {
        type_name: "robot.State".into(),
        ..EndpointContract::default()
    }
}

fn address(port: &str) -> EndpointAddress {
    EndpointAddress {
        namespace: "robot/sensor".into(),
        port: port.into(),
    }
}

fn fixture(bindings: ModuleBindings) -> Fixture {
    let port = |contract: &EndpointContract, endpoint| PortReport {
        endpoint,
        type_name: contract.type_name.clone(),
        schema: 17,
        contract: contract.clone(),
        response: contract.response.as_ref().map(|name| ResponseReport {
            type_name: name.clone(),
            schema: 31,
        }),
    };
    let report = ModuleReport {
        version: MODULE_REPORT_VERSION,
        fingerprint_version: crate::SCHEMA_FINGERPRINT_VERSION,
        namespace: bindings.namespace.clone(),
        endpoint_namespace: bindings.endpoint_namespace.clone(),
        inputs: bindings
            .inputs
            .iter()
            .map(|(name, binding)| {
                (
                    name.clone(),
                    port(
                        &binding.contract,
                        PortEndpoint::Input {
                            sources: binding.sources.clone(),
                        },
                    ),
                )
            })
            .collect(),
        outputs: bindings
            .outputs
            .iter()
            .map(|(name, binding)| {
                (
                    name.clone(),
                    port(
                        &binding.contract,
                        PortEndpoint::Output {
                            address: EndpointAddress {
                                namespace: bindings.endpoint_namespace.clone(),
                                port: name.clone(),
                            },
                        },
                    ),
                )
            })
            .collect(),
    };
    Fixture { bindings, report }
}

fn stream_graph() -> [Fixture; 2] {
    let source = ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: "robot/sensor".into(),
        endpoint_namespace: "robot/sensor".into(),
        inputs: BTreeMap::new(),
        outputs: ["left", "right"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    OutputBinding {
                        contract: contract(),
                    },
                )
            })
            .collect(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    };
    let receiver = ModuleBindings {
        namespace: "robot/control".into(),
        endpoint_namespace: "robot/control".into(),
        inputs: BTreeMap::from([(
            "state".into(),
            InputBinding {
                contract: contract(),
                sources: vec![address("left"), address("right")],
            },
        )]),
        outputs: BTreeMap::new(),
        ..source.clone()
    };
    [fixture(source), fixture(receiver)]
}

fn rpc_graph() -> [Fixture; 2] {
    let [mut source, mut receiver] = stream_graph();
    for binding in source.bindings.outputs.values_mut() {
        binding.contract.kind = PortKind::Rpc;
        binding.contract.response = Some("robot.Result".into());
    }
    for binding in receiver.bindings.inputs.values_mut() {
        binding.contract.kind = PortKind::Rpc;
        binding.contract.response = Some("robot.Result".into());
        binding.sources.truncate(1);
    }
    [fixture(source.bindings), fixture(receiver.bindings)]
}

fn check(fixtures: &[Fixture]) -> Result<(), ReportError> {
    let entries: Vec<_> = fixtures.iter().map(Fixture::entry).collect();
    validate_graph(&entries)
}
