use super::*;

#[test]
fn graph_reports_first_failure_in_entry_then_input_then_source_order() {
    let [source, mut receiver] = stream_graph();
    receiver.bindings.inputs.get_mut("state").unwrap().sources =
        vec![address("z-missing"), address("a-missing")];
    let mut second_input = receiver.bindings.inputs["state"].clone();
    second_input.sources = vec![address("earlier-input")];
    receiver
        .bindings
        .inputs
        .insert("a-input".into(), second_input);
    let first = fixture(receiver.bindings.clone());
    receiver.bindings.namespace = "robot/second".into();
    receiver.bindings.endpoint_namespace = receiver.bindings.namespace.clone();
    let second = fixture(receiver.bindings);

    let error = check(&[source, second, first]).unwrap_err();

    assert_eq!(error.module.as_deref(), Some("robot/second"));
    assert_eq!(error.port.as_deref(), Some("a-input"));
    assert_eq!(error.source.as_deref(), Some(&address("earlier-input")));
}

#[test]
fn graph_retains_declared_source_order_in_diagnostics() {
    let [source, mut receiver] = stream_graph();
    receiver.bindings.inputs.get_mut("state").unwrap().sources =
        vec![address("z-missing"), address("a-missing")];
    let receiver = fixture(receiver.bindings);

    let error = check(&[source, receiver]).unwrap_err();

    assert_eq!(error.source.as_deref(), Some(&address("z-missing")));
}
