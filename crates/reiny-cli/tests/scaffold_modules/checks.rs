use super::Sandbox;

#[test]
fn runtime_check_does_not_build_when_root_declares_missing_cargo_manifest() {
    // Given
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.0.join("main.yaml"),
        "version: 3\ndeployment: demo\nproviders: {process: {type: process}}\nrun: {provider: process, bin: demo}\nbuild: {type: cargo, manifest: absent.toml}\n",
    ).unwrap();
    // When
    sandbox.success(&["check", "."]);
    // Then
    assert!(!sandbox.0.join(".reiny/build").exists());
}

#[test]
fn runtime_check_rejects_legacy_reusable_definition() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.0.join("main.yaml"),
        "version: 1\nrun: {provider: process, bin: absent}\n",
    )
    .unwrap();

    assert!(!sandbox.run(&["check", "main.yaml"]).status.success());
}

#[test]
fn runtime_check_rejects_definition_without_run_or_modules() {
    let sandbox = Sandbox::new();
    std::fs::write(sandbox.0.join("main.yaml"), "version: 3\n").unwrap();

    assert!(!sandbox.run(&["check", "main.yaml"]).status.success());
}

#[test]
fn runtime_check_accepts_reusable_leaf_without_provider_or_executable() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.0.join("main.yaml"),
        "version: 3\nrun: {provider: external_process, bin: absent}\nin:\n  request: {type: test.Request}\nout:\n  response: {type: test.Response}\n",
    )
    .unwrap();

    sandbox.success(&["check", "main.yaml"]);
    assert!(!sandbox.0.join("absent").exists());
    assert!(!sandbox.0.join(".reiny").exists());
}

#[test]
fn explicit_runtime_check_is_independent_of_invalid_catalog() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.0.join("main.yaml"),
        "version: 3\ndeployment: demo\nproviders: {process: {type: process}}\nrun: {provider: process, bin: absent}\n",
    )
    .unwrap();
    std::fs::write(
        sandbox.0.join("schema.yaml"),
        "version: 1\nschema: {project: {name: demo}}\n",
    )
    .unwrap();

    sandbox.success(&["check", "main.yaml"]);
    assert!(!sandbox.run(&["check", "schema.yaml"]).status.success());
    assert!(!sandbox.run(&["check", "."]).status.success());
}

#[test]
fn explicit_catalog_check_is_independent_of_invalid_runtime() {
    let sandbox = Sandbox::new();
    std::fs::write(sandbox.0.join("main.yaml"), "version: 1\n").unwrap();
    std::fs::write(
        sandbox.0.join("schema.yaml"),
        "version: 3\nschema: {project: {name: demo}, exported_types: {}}\n",
    )
    .unwrap();

    sandbox.success(&["check", "schema.yaml"]);
    assert!(!sandbox.run(&["check", "main.yaml"]).status.success());
}

#[test]
fn runtime_check_rejects_root_with_unresolved_input_wiring() {
    let sandbox = Sandbox::new();
    std::fs::create_dir(sandbox.0.join("consumer")).unwrap();
    std::fs::write(
        sandbox.0.join("consumer/main.yaml"),
        "version: 3\nrun: {provider: process, bin: absent}\nin:\n  state: {type: test.State}\n",
    )
    .unwrap();
    std::fs::write(
        sandbox.0.join("main.yaml"),
        "version: 3\ndeployment: demo\nproviders: {process: {type: process}}\nmodules:\n  consumer:\n    source: ./consumer\n    in: {state: {type: test.State, from: missing.state}}\n",
    )
    .unwrap();

    assert!(!sandbox.run(&["check", "main.yaml"]).status.success());
    assert!(!sandbox.0.join(".reiny/build").exists());
    assert!(!sandbox.0.join(".reiny/state.json").exists());
}

#[test]
fn schema_check_discovers_catalog_when_nearest_module_is_runtime_only() {
    // Given
    let sandbox = Sandbox::new();
    std::fs::create_dir_all(sandbox.0.join("leaf/src")).unwrap();
    std::fs::write(
        sandbox.0.join("schema.yaml"),
        "version: 3\nschema: {project: {name: demo}, exported_types: {}}\n",
    )
    .unwrap();
    std::fs::write(
        sandbox.0.join("leaf/main.yaml"),
        "version: 3\nrun: {provider: process, bin: demo}\n",
    )
    .unwrap();
    // When
    sandbox.success(&["check", "leaf/src"]);
    // Then
    assert!(!sandbox.0.join(".reiny/build").exists());
}
