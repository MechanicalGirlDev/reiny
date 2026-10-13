use super::*;

#[test]
fn manifest_search_takes_the_nearest_one_upward() {
    let f = Fixture::new("upward");
    f.write("proto/shared.proto", "");
    f.write(
        "schema.yaml",
        r"
{version: 2,schema: {internals: {Shared: {proto: proto/shared.proto,message: ws.Shared}},projects: {ping: {publications: [Shared]}}}}
",
    );
    f.write("ping/proto/ping.proto", "");
    f.write(
        "ping/schema.yaml",
        r"
{version: 2,schema: {project: {name: ping,version: 0.1.0},publications: {Ping: {proto: proto/ping.proto,message: ping.Ping}}}}
",
    );
    std::fs::create_dir_all(f.path("ping/src")).unwrap();

    // From the launch directory: its own manifest, not the workspace's.
    let inner = describe(&f.path("ping")).unwrap();
    assert!(matches!(inner.mode(), Mode::PerProject));
    // From a subdirectory with no manifest: the nearest ancestor's, which is still the launch's.
    let nested = describe(&f.path("ping/src")).unwrap();
    assert_eq!(nested.manifest_path(), inner.manifest_path());
    // From the root: the workspace manifest.
    let outer = describe(f.root()).unwrap();
    assert!(matches!(outer.mode(), Mode::Workspace));
    assert_ne!(outer.manifest_path(), inner.manifest_path());
}

#[test]
fn runtime_only_modules_inherit_the_ancestor_catalog() {
    // Given a catalog above a runtime module with deliberately opaque provider data.
    let f = Fixture::new("catalog-inheritance");
    f.write("proto/shared.proto", "");
    f.write(
        "schema.yaml",
        "version: 2\nschema:\n  internals:\n    Shared: { proto: proto/shared.proto, message: ws.Shared }\n",
    );
    f.write(
        "modules/worker/main.yaml",
        "version: 2\nproviders: runtime-only\nrun: { provider: remote, bin: worker }\n",
    );
    std::fs::create_dir_all(f.path("modules/worker/src")).unwrap();

    // When resolving from the namespaced leaf.
    let res = describe(&f.path("modules/worker/src")).unwrap();

    // Then the shared catalog, not the provider or leaf directory, owns proto paths.
    assert_eq!(res.manifest_path(), f.path("schema.yaml"));
    assert_eq!(res.types()[0].proto, f.path("proto/shared.proto"));
    assert_eq!(res.types()[0].message, "ws.Shared");
}

#[test]
fn v2_catalog_resolves_types_without_interpreting_runtime_contracts() {
    // Given a v2 catalog beside a malformed runtime manifest.
    let fixture = Fixture::new("v2-catalog");
    fixture.write("proto/state.proto", "");
    fixture.write("main.yaml", "not valid yaml: [");
    fixture.write("schema.yaml",
        "version: 2\nschema:\n  project: {name: app}\n  publications:\n    State: {proto: proto/state.proto, message: probe.State}\n");
    // When codegen resolves the app's catalog.
    let resolution = describe(fixture.root()).unwrap();
    // Then catalog-relative type resolution does not require a runtime deployment.
    assert_eq!(
        resolution.types()[0].proto,
        fixture.path("proto/state.proto")
    );
    assert_eq!(resolution.types()[0].message, "probe.State");
}

#[test]
fn main_yaml_catalogs_are_not_discovered_locally_or_in_ancestors() {
    let f = Fixture::new("no-main-fallback");
    let catalog = "version: 2\nschema: { project: { name: old } }\n";
    f.write("main.yaml", catalog);
    f.write("worker/main.yaml", catalog);
    let err = describe_err(&f.path("worker"));
    assert!(err.contains("schema.yaml not found"), "{err}");
}

#[test]
fn dependencies_require_their_own_schema_yaml() {
    let f = Fixture::new("dependency-no-main-fallback");
    f.write(
        "schema.yaml",
        "version: 2\nschema: { project: { name: app }, dependencies: { old: { path: dep } } }\n",
    );
    f.write(
        "dep/main.yaml",
        "version: 2\nschema: { project: { name: old } }\n",
    );
    let err = describe_err(f.root());
    assert!(err.contains("dependency 'old' schema.yaml"), "{err}");
}
