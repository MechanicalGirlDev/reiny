use super::*;

#[test]
fn v1_catalog_is_rejected_at_discovery_and_dependency_boundaries() {
    let f = Fixture::new("v1-catalog");
    f.write(
        "schema.yaml",
        "version: 1\nschema: { project: { name: app } }\n",
    );
    let err = find_manifest(f.root()).unwrap_err().to_string();
    assert!(err.contains("unsupported schema.yaml version 1"), "{err}");
    let err = parse_manifest(&f.path("schema.yaml"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("unsupported schema.yaml version 1"), "{err}");
}

#[test]
fn legacy_single_schema_crate_is_rejected() {
    let f = Fixture::new("single-schema");
    f.write(
        "schema.yaml",
        "version: 3\nschema:\n  shared_types: {}\n  schema_crates: { crate: app-schema }\n",
    );
    let err = describe_err(f.root());
    assert!(err.contains("schema.schema_crates.crate"), "{err}");
    assert!(err.contains("invalid type"), "{err}");
}

#[test]
fn catalog_requires_supported_version_and_valid_schema() {
    // Given malformed catalog manifests.
    for (name, text) in [
        ("missing-version", "schema: { project: { name: app } }\n"),
        (
            "wrong-version",
            "version: 2\nschema: { project: { name: app } }\n",
        ),
        (
            "bad-schema",
            "version: 3\nschema: { exported_types: invalid }\n",
        ),
        ("bad-yaml", "version: 3\nschema: [\n"),
        ("missing-schema", "version: 3\n"),
        ("null-schema", "version: 3\nschema: null\n"),
    ] {
        let f = Fixture::new(name);
        f.write("schema.yaml", text);

        // When discovery encounters them, it must not silently fall back.
        let result = find_manifest(f.root());

        // Then parsing fails at the manifest boundary.
        assert!(result.is_err(), "{name}");
    }
}

#[test]
fn ancestor_catalog_preserves_distinct_fully_qualified_types() {
    // Given distinct protobuf packages with the same message name.
    let f = Fixture::new("namespaced-collision");
    f.write("proto/a.proto", "");
    f.write("proto/b.proto", "");
    f.write(
        "schema.yaml",
        "version: 3\nschema:\n  shared_types:\n    First: { proto: proto/a.proto, message: first.Value }\n    Second: { proto: proto/b.proto, message: second.Value }\n",
    );
    f.write(
        "modules/leaf/main.yaml",
        "version: 3\nrun: { provider: process, bin: leaf }\n",
    );

    // When resolving the namespaced module.
    let result = describe(&f.path("modules/leaf"));

    // Then both full names survive resolution independently.
    let resolution = result.unwrap();
    let names: Vec<_> = resolution
        .types()
        .into_iter()
        .map(|ty| ty.type_name)
        .collect();
    assert_eq!(names, ["first.Value", "second.Value"]);
}

#[test]
fn invalid_nearest_catalog_does_not_fall_back_to_ancestor() {
    let f = Fixture::new("invalid-nearest");
    f.write(
        "schema.yaml",
        "version: 3\nschema: { project: { name: ancestor } }\n",
    );
    f.write("worker/schema.yaml", "version: 3\n");
    let err = describe_err(&f.path("worker"));
    assert!(err.contains("worker"), "{err}");
    assert!(err.contains("missing field `schema`"), "{err}");
}

#[test]
fn describe_reports_layout_mistakes() {
    // Neither [project] nor [shared_types]/[projects]: the message names both ways out.
    let f = Fixture::new("neither");
    f.write("schema.yaml", "version: 3\nschema: {}\n");
    let err = describe_err(f.root());
    assert!(err.contains("[project]"), "{err}");
    assert!(err.contains("[shared_types]"), "{err}");

    // A publication naming a proto that is not on disk.
    let f = Fixture::new("missing-proto");
    f.write(
        "schema.yaml",
        r"
{version: 3,schema: {project: {name: ping,version: 0.1.0},exported_types: {Ping: {proto: proto/ping.proto,message: ping.Ping}}}}
",
    );
    let err = describe_err(f.root());
    assert!(err.contains("proto file not found"), "{err}");

    // A [dependencies] key becomes `pub mod <key>` in the generated code, so a hyphen has to be
    // caught here, not as a syntax error inside reiny_generated.rs.
    let f = Fixture::new("bad-dep-key");
    f.write("dep/proto/d.proto", "");
    f.write(
        "dep/schema.yaml",
        r"
{version: 3,schema: {project: {name: dep,version: 0.1.0},exported_types: {D: {proto: proto/d.proto,message: d.D}}}}
",
    );
    f.write("app/proto/a.proto", "");
    f.write(
        "app/schema.yaml",
        r#"
{version: 3,schema: {project: {name: app,version: 0.1.0},exported_types: {A: {proto: proto/a.proto,message: a.A}},dependencies: {my-dep: {version: "0.1",path: ../dep}}}}
"#,
    );
    let err = describe_err(&f.path("app"));
    assert!(err.contains("[dependencies]"), "{err}");
    assert!(err.contains("my-dep"), "{err}");
}

#[test]
fn missing_manifest_names_the_starting_directory() {
    let f = Fixture::new("no-manifest"); // deliberately empty
    let err = describe_err(f.root());
    assert!(err.contains("schema.yaml"), "{err}");
    assert!(err.contains("no-manifest"), "{err}");
}

#[test]
fn legacy_catalog_fields_and_unknown_nested_fields_are_rejected() {
    // Given legacy role metadata and misspelled catalog definitions.
    for text in [
        "version: 3\nschema: {}\npublications: {}\n",
        "version: 3\nschema: {publications: {}}\n",
        "version: 3\nschema: {internals: {}}\n",
        "version: 3\nschema: {schema: {messages: {crate: app-schema, protos: []}}}\n",
        "version: 3\nschema: {projects: {app: {publications: []}}}\n",
        "version: 3\nschema: {projects: {app: {dependencies: []}}}\n",
        "version: 3\nschema: {projects: {app: {description: ignored}}}\n",
        "version: 3\nschema: {project: {name: app, description: ignored}}\n",
        "version: 3\nschema: {exported_types: {State: {proto: state.proto, message: State, typo: true}}}\n",
        "version: 3\nschema: {schema_crates: {messages: {crate: app-schema, protos: [], depends: []}}}\n",
        "version: 3\nschema: {dependencies: {dep: {path: dep, typo: true}}}\n",
        "version: 3\nschema: {services: {Call: {request: Req, response: Resp, typo: true}}}\n",
    ] {
        let fixture = Fixture::new("unknown-catalog-field");
        fixture.write("schema.yaml", text);
        // When parsing the catalog at its boundary.
        let error = parse_manifest(&fixture.path("schema.yaml")).unwrap_err();
        // Then no legacy or unknown field is silently ignored.
        assert!(
            format!("{error:#}").contains("unknown field"),
            "{text}: {error:#}"
        );
    }
}
