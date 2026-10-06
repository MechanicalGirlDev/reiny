use super::*;

#[test]
fn config_keys_are_idents_and_values_scalars() {
    let manifest = |config: &str| {
        format!(
            "version: 1\nschema:\n  project: {{ name: app }}\n  publications:\n    A: {{ proto: proto/a.proto, message: a.A }}\n  config:\n    {config}\n"
        )
    };
    let f = Fixture::new("config-key");
    f.write("proto/a.proto", "");
    f.write("main.yaml", &manifest("delay-ms: 0"));
    let err = describe_err(f.root());
    assert!(err.contains("[config]"), "{err}");
    assert!(err.contains("delay-ms"), "{err}");

    let f = Fixture::new("config-value");
    f.write("proto/a.proto", "");
    f.write("main.yaml", &manifest("limits: { max: 1 }"));
    let err = describe_err(f.root());
    assert!(err.contains("[config].limits"), "{err}");
    assert!(err.contains("table"), "{err}");
}

#[test]
fn describe_reports_config_and_services() {
    let f = Fixture::new("services");
    f.write("proto/calc.proto", "");
    f.write(
        "main.yaml",
        r"
{version: 1,schema: {project: {name: calc,version: 0.1.0},publications: {Add: {proto: proto/calc.proto,message: calc.Add},Sum: {proto: proto/calc.proto,message: calc.Sum}},services: {Adder: {request: Add,response: Sum}},config: {rate_hz: 10,name: calc}}}
",
    );

    let res = describe(f.root()).unwrap();
    assert!(res.has_config());
    let services = res.services();
    assert_eq!(services.len(), 1);
    let s = &services[0];
    assert_eq!(s.name, "Adder");
    assert_eq!(
        (s.request.as_str(), s.request_message.as_str()),
        ("Add", "calc.Add")
    );
    assert_eq!(
        (s.response.as_str(), s.response_message.as_str()),
        ("Sum", "calc.Sum")
    );
}
