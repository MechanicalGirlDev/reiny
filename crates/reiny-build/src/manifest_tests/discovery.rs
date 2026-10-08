use super::*;

#[test]
fn manifest_search_takes_the_nearest_one_upward() {
    let f = Fixture::new("upward");
    f.write("proto/shared.proto", "");
    f.write(
        "main.yaml",
        r"
{version: 1,schema: {internals: {Shared: {proto: proto/shared.proto,message: ws.Shared}},projects: {ping: {publications: [Shared]}}}}
",
    );
    f.write("ping/proto/ping.proto", "");
    f.write(
        "ping/main.yaml",
        r"
{version: 1,schema: {project: {name: ping,version: 0.1.0},publications: {Ping: {proto: proto/ping.proto,message: ping.Ping}}}}
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
        "main.yaml",
        "version: 1\nschema:\n  internals:\n    Shared: { proto: proto/shared.proto, message: ws.Shared }\n",
    );
    f.write(
        "modules/worker/main.yaml",
        "version: 2\nproviders: runtime-only\nrun: { provider: remote, bin: worker }\n",
    );
    std::fs::create_dir_all(f.path("modules/worker/src")).unwrap();

    // When resolving from the namespaced leaf.
    let res = describe(&f.path("modules/worker/src")).unwrap();

    // Then the shared catalog, not the provider or leaf directory, owns proto paths.
    assert_eq!(res.manifest_path(), f.path("main.yaml"));
    assert_eq!(res.types()[0].proto, f.path("proto/shared.proto"));
    assert_eq!(res.types()[0].message, "ws.Shared");
}

#[test]
fn v2_catalog_resolves_types_without_interpreting_runtime_contracts() {
    // Given a v2 catalog beside deliberately opaque runtime provider data.
    let fixture = Fixture::new("v2-catalog");
    fixture.write("proto/state.proto", "");
    fixture.write("main.yaml",
        "version: 2\nproviders: runtime-only\nschema:\n  project: {name: app}\n  publications:\n    State: {proto: proto/state.proto, message: probe.State}\n");
    // When codegen resolves the app's catalog.
    let resolution = describe(fixture.root()).unwrap();
    // Then catalog-relative type resolution does not require a runtime deployment.
    assert_eq!(
        resolution.types()[0].proto,
        fixture.path("proto/state.proto")
    );
    assert_eq!(resolution.types()[0].message, "probe.State");
}
