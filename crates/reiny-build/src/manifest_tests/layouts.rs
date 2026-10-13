use super::*;

#[test]
fn one_named_schema_part_assigns_ownership_and_compiles_its_protos() {
    let f = Fixture::new("named-schema");
    f.write(
        "proto/ping.proto",
        "syntax = \"proto3\"; package ping; message Ping { uint64 value = 1; }\n",
    );
    f.write(
        "main.yaml",
        "version: 2\nschema:\n  internals:\n    Ping: { proto: proto/ping.proto, message: ping.Ping }\n  schema:\n    messages:\n      crate: app-schema\n      protos: [proto/ping.proto]\n  projects:\n    ping: { publications: [Ping] }\n",
    );
    let res = describe(f.root()).unwrap();
    assert_eq!(res.schema_crates()[0].part.as_deref(), Some("messages"));
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
        assert!(out.contains("const SCHEMA: u64"));
        assert!(out.contains("message: \"ping.Ping\""));
    }
}

#[test]
fn describe_resolves_a_per_project_layout() {
    let f = Fixture::new("per-project");
    f.write("pong/proto/pong.proto", "");
    f.write(
        "pong/main.yaml",
        r"
{version: 2,schema: {project: {name: pong,version: 0.1.0},publications: {Pong: {proto: proto/pong.proto,message: pong.Pong}}}}
",
    );
    f.write("ping/proto/ping.proto", "");
    f.write(
        "ping/main.yaml",
        r#"
{version: 2,schema: {project: {name: ping,version: 0.1.0},publications: {Ping: {proto: proto/ping.proto,message: ping.Ping}},dependencies: {pong: {version: "0.1",path: ../pong}}}}
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
        .map(|t| (t.alias, t.topic_segment, t.module))
        .collect();
    types.sort();
    assert_eq!(
        types,
        vec![
            (
                "Ping".to_string(),
                "Ping".to_string(),
                "publications".to_string()
            ),
            (
                "Pong".to_string(),
                "Pong".to_string(),
                "dependencies::pong".to_string()
            ),
        ]
    );
    // The topic segment is the bare type name — the proto package is stripped.
    let ping = res.types().into_iter().find(|t| t.alias == "Ping").unwrap();
    assert_eq!(ping.message, "ping.Ping");
    assert_eq!(ping.topic_segment, "Ping");
    assert!(ping.proto.is_absolute());
}

#[test]
fn describe_resolves_a_workspace_layout() {
    let f = Fixture::new("workspace");
    f.write("proto/ping.proto", "");
    f.write("proto/pong.proto", "");
    f.write(
        "main.yaml",
        r"
{version: 2,schema: {internals: {Ping: {proto: proto/ping.proto,message: ping.Ping},Pong: {proto: proto/pong.proto,message: pong.Pong}},projects: {talker: {publications: [Ping],dependencies: [Pong]},listener: {publications: [Pong],dependencies: [Ping]}}}}
",
    );

    let res = describe(f.root()).unwrap();
    assert!(matches!(res.mode(), Mode::Workspace), "{:?}", res.mode());
    assert!(res.schema_crates().is_empty());
    assert!(
        res.types().iter().all(|t| t.module == "internals"),
        "workspace types all go to internals"
    );

    let mut projects: Vec<&str> = res.projects().iter().map(|p| p.name.as_str()).collect();
    projects.sort_unstable();
    assert_eq!(projects, ["listener", "talker"]);
    let talker = res
        .projects()
        .iter()
        .find(|p| p.name == "talker")
        .expect("talker is declared");
    assert_eq!(talker.publications, ["Ping"]);
    assert_eq!(talker.dependencies, ["Pong"]);
}
