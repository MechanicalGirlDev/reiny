use super::*;

#[test]
fn main_manifest_requires_supported_version_and_valid_schema() {
    // Given malformed unified manifests.
    for (name, text) in [
        ("missing-version", "schema: { project: { name: app } }\n"),
        (
            "wrong-version",
            "version: 2\nschema: { project: { name: app } }\n",
        ),
        (
            "bad-schema",
            "version: 1\nschema: { publications: invalid }\n",
        ),
        ("bad-yaml", "version: 1\nschema: [\n"),
    ] {
        let f = Fixture::new(name);
        f.write("main.yaml", text);

        // When discovery encounters them, it must not silently fall back.
        let result = find_manifest(f.root());

        // Then parsing fails at the manifest boundary.
        assert!(result.is_err(), "{name}");
    }
}

#[test]
fn ancestor_catalog_still_rejects_topic_collisions() {
    // Given distinct descriptors sharing a short topic name in an ancestor catalog.
    let f = Fixture::new("namespaced-collision");
    f.write("proto/a.proto", "");
    f.write("proto/b.proto", "");
    f.write(
        "main.yaml",
        "version: 1\nschema:\n  internals:\n    First: { proto: proto/a.proto, message: first.Value }\n    Second: { proto: proto/b.proto, message: second.Value }\n",
    );
    f.write(
        "modules/leaf/main.yaml",
        "version: 1\nrun: { provider: process, bin: leaf }\n",
    );

    // When resolving the namespaced module.
    let result = describe(&f.path("modules/leaf"));

    // Then runtime namespaces cannot conceal a schema collision.
    assert!(result.is_err());
}

#[test]
fn describe_reports_layout_mistakes() {
    // Neither [project] nor [internals]/[projects]: the message names both ways out.
    let f = Fixture::new("neither");
    f.write("main.yaml", "version: 1\nschema: {}\n");
    let err = describe_err(f.root());
    assert!(err.contains("[project]"), "{err}");
    assert!(err.contains("[internals]"), "{err}");

    // A publication naming a proto that is not on disk.
    let f = Fixture::new("missing-proto");
    f.write(
        "main.yaml",
        r"
{version: 1,schema: {project: {name: ping,version: 0.1.0},publications: {Ping: {proto: proto/ping.proto,message: ping.Ping}}}}
",
    );
    let err = describe_err(f.root());
    assert!(err.contains("proto file not found"), "{err}");

    // A [dependencies] key becomes `pub mod <key>` in the generated code, so a hyphen has to be
    // caught here, not as a syntax error inside reiny_generated.rs.
    let f = Fixture::new("bad-dep-key");
    f.write("dep/proto/d.proto", "");
    f.write(
        "dep/main.yaml",
        r"
{version: 1,schema: {project: {name: dep,version: 0.1.0},publications: {D: {proto: proto/d.proto,message: d.D}}}}
",
    );
    f.write("app/proto/a.proto", "");
    f.write(
        "app/main.yaml",
        r#"
{version: 1,schema: {project: {name: app,version: 0.1.0},publications: {A: {proto: proto/a.proto,message: a.A}},dependencies: {my-dep: {version: "0.1",path: ../dep}}}}
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
    assert!(err.contains("main.yaml"), "{err}");
    assert!(err.contains("no-manifest"), "{err}");
}
