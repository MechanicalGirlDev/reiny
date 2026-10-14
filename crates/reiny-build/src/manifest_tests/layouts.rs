use super::*;

#[test]
fn one_named_schema_part_assigns_ownership_and_compiles_its_protos() {
    let f = Fixture::new("named-schema");
    f.write(
        "proto/ping.proto",
        "syntax = \"proto3\"; package ping; message Ping { uint64 value = 1; }\n",
    );
    f.write(
        "schema.yaml",
        "version: 3\nschema:\n  shared_types:\n    Ping: { proto: proto/ping.proto, message: ping.Ping }\n  schema_crates:\n    messages:\n      crate: app-schema\n      protos: [proto/ping.proto]\n  projects:\n    ping: { types: [Ping] }\n",
    );
    let res = describe(f.root()).unwrap();
    assert_eq!(res.schema_crates()[0].name, "messages");
    assert_eq!(res.entries[0].owner.as_deref(), Some("messages"));
    #[cfg(feature = "compile")]
    {
        let res = resolve_for(f.root(), "app-schema").unwrap();
        assert!(matches!(res.mode(), Mode::Schema { part: Some(name) } if name == "messages"));
        let consumer = resolve_for(f.root(), "ping").unwrap();
        assert!(matches!(consumer.mode(), Mode::SchemaConsumer { .. }));
        let plan = compile_plan(&res).unwrap();
        assert_eq!(plan.protos, [f.path("proto/ping.proto")]);
        let out_dir = f.path("out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let fds = compile_protos(&plan, &out_dir, |_| {}).unwrap();
        let scan = scan_descriptors(&fds, &plan.own_names);
        let out = render_generated(&plan.entries, None, &scan.fingerprints, &[]).unwrap();
        assert!(out.contains("const SCHEMA_FINGERPRINT: u64"));
        assert!(out.contains("message: \"ping.Ping\""));
    }
}

#[test]
fn describe_resolves_a_per_project_layout() {
    let f = Fixture::new("per-project");
    f.write("pong/proto/pong.proto", "");
    f.write(
        "pong/schema.yaml",
        r"
{version: 3,schema: {project: {name: pong,version: 0.1.0},exported_types: {Pong: {proto: proto/pong.proto,message: pong.Pong}}}}
",
    );
    f.write("ping/proto/ping.proto", "");
    f.write(
        "ping/schema.yaml",
        r#"
{version: 3,schema: {project: {name: ping,version: 0.1.0},exported_types: {Ping: {proto: proto/ping.proto,message: ping.Ping}},dependencies: {pong: {version: "0.1",path: ../pong}}}}
"#,
    );

    let res = describe(&f.path("ping")).unwrap();
    assert!(matches!(res.mode(), Mode::PerProject), "{:?}", res.mode());
    assert!(res.manifest_path().parent().unwrap().ends_with("ping"));
    assert!(!res.has_config());
    assert!(res.services().is_empty());
    assert!(res.projects().is_empty(), "per-project has no [projects.*]");

    let mut types: Vec<(String, String, String)> = res
        .types()
        .into_iter()
        .map(|t| (t.alias, t.type_name, t.module))
        .collect();
    types.sort();
    assert_eq!(
        types,
        vec![
            (
                "Ping".to_string(),
                "ping.Ping".to_string(),
                "exported_types".to_string()
            ),
            (
                "Pong".to_string(),
                "pong.Pong".to_string(),
                "dependencies::pong".to_string()
            ),
        ]
    );
    // The transport name retains the complete protobuf package.
    let ping = res.types().into_iter().find(|t| t.alias == "Ping").unwrap();
    assert_eq!(ping.message, "ping.Ping");
    assert_eq!(ping.type_name, "ping.Ping");
    assert!(ping.proto.is_absolute());
}

#[test]
fn describe_resolves_a_workspace_layout() {
    let f = Fixture::new("workspace");
    f.write("proto/ping.proto", "");
    f.write("proto/pong.proto", "");
    f.write(
        "schema.yaml",
        r"
{version: 3,schema: {shared_types: {Ping: {proto: proto/ping.proto,message: ping.Ping},Pong: {proto: proto/pong.proto,message: pong.Pong}},projects: {talker: {types: [Ping, Pong, Ping]},listener: {types: [Pong, Ping]}}}}
",
    );

    let res = describe(f.root()).unwrap();
    assert!(matches!(res.mode(), Mode::Workspace), "{:?}", res.mode());
    assert!(res.schema_crates().is_empty());
    assert!(
        res.types().iter().all(|t| t.module == "shared_types"),
        "workspace types all go to shared_types"
    );

    let mut projects: Vec<&str> = res.projects().iter().map(|p| p.name.as_str()).collect();
    projects.sort_unstable();
    assert_eq!(projects, ["listener", "talker"]);
    let talker = res
        .projects()
        .iter()
        .find(|p| p.name == "talker")
        .expect("talker is declared");
    assert_eq!(talker.types, ["Ping", "Pong"]);
}

#[cfg(feature = "compile")]
#[test]
fn codegen_preserves_full_names_and_package_less_names() {
    // Given same-named messages in different packages and an unqualified message.
    let fixture = Fixture::new("full-type-names");
    for (file, package) in [
        ("first.proto", "package first;"),
        ("second.proto", "package second;"),
        ("bare.proto", ""),
    ] {
        fixture.write(
            file,
            &format!("syntax = \"proto3\"; {package} message Value {{ uint64 value = 1; }}\n"),
        );
    }
    fixture.write("schema.yaml", "version: 3\nschema:\n  shared_types:\n    First: {proto: first.proto, message: first.Value}\n    Second: {proto: second.proto, message: second.Value}\n    Bare: {proto: bare.proto, message: Value}\n  projects: {app: {types: [First, Second, Bare]}}\n");
    // When the real descriptor compiler feeds Rust generation.
    let resolution = resolve_for(fixture.root(), "app").unwrap();
    let plan = compile_plan(&resolution).unwrap();
    let out = fixture.path("out");
    std::fs::create_dir_all(&out).unwrap();
    let descriptor = compile_protos(&plan, &out, |_| {}).unwrap();
    let scan = scan_descriptors(&descriptor, &plan.own_names);
    let generated = render_generated(&plan.entries, None, &scan.fingerprints, &[]).unwrap();
    // Then each generated impl carries its exact protobuf name and fingerprint.
    for name in ["first.Value", "second.Value", "Value"] {
        assert!(
            generated.contains(&format!("const TYPE_NAME: &'static str = {name:?};")),
            "{generated}"
        );
        assert!(scan.fingerprints.contains_key(name));
    }
    assert_eq!(
        generated.matches("impl ::reiny::MessageType for").count(),
        3
    );
}
