use super::{Sandbox, yaml};

#[test]
fn publisher_declares_matching_schema_and_port_when_created() {
    // Given
    let sandbox = Sandbox::new();
    // When
    sandbox.success(&["new", "demo", "--publish", "Ping"]);
    // Then
    let dir = sandbox.0.join("demo");
    let manifest = yaml(&dir.join("main.yaml"));
    assert_eq!(manifest["version"], 3);
    assert_eq!(manifest["deployment"], "demo");
    assert_eq!(manifest["out"]["ping"]["type"], "ping.Ping");
    assert_eq!(
        yaml(&dir.join("schema.yaml"))["schema"]["exported_types"]["Ping"]["message"],
        "ping.Ping"
    );
    assert_eq!(manifest["build"]["locked"], false);
    assert!(manifest["in"].as_mapping().unwrap().is_empty());
    assert!(!dir.join("Reiny.toml").exists());
    let cargo: toml::Table =
        toml::from_str(&std::fs::read_to_string(dir.join("Cargo.toml")).unwrap()).unwrap();
    assert!(cargo["workspace"].is_table());
    assert!(cargo["dependencies"]["reiny"]["path"].is_str());
    sandbox.success(&["check", "demo"]);
}

#[test]
fn user_values_and_sources_survive_when_initialized() {
    // Given
    let sandbox = Sandbox::new();
    std::fs::create_dir(sandbox.0.join("src")).unwrap();
    let source = "fn main() { println!(\"existing\"); }\n";
    let build = "fn main() {}\n";
    let manifest = "version: 3\n";
    std::fs::write(sandbox.0.join("src/main.rs"), source).unwrap();
    std::fs::write(sandbox.0.join("build.rs"), build).unwrap();
    std::fs::write(sandbox.0.join("main.yaml"), manifest).unwrap();
    std::fs::write(
        sandbox.0.join("Cargo.toml"),
        "[package]\nname = \"custom\"\nversion = \"2.0.0\"\nedition = \"2024\"\n[dependencies]\nreiny = \"=0.6.0\"\nprost = \"=0.14.1\"\n",
    ).unwrap();
    // When
    sandbox.success(&["init", ".", "--name", "demo"]);
    // Then
    assert_eq!(
        std::fs::read_to_string(sandbox.0.join("src/main.rs")).unwrap(),
        source
    );
    assert_eq!(
        std::fs::read_to_string(sandbox.0.join("build.rs")).unwrap(),
        build
    );
    assert_eq!(
        std::fs::read_to_string(sandbox.0.join("main.yaml")).unwrap(),
        manifest
    );
    let cargo: toml::Table =
        toml::from_str(&std::fs::read_to_string(sandbox.0.join("Cargo.toml")).unwrap()).unwrap();
    assert_eq!(cargo["package"]["name"].as_str(), Some("custom"));
    assert_eq!(cargo["package"]["version"].as_str(), Some("2.0.0"));
    assert_eq!(cargo["dependencies"]["reiny"].as_str(), Some("=0.6.0"));
    assert_eq!(cargo["dependencies"]["prost"].as_str(), Some("=0.14.1"));
}

#[test]
fn runtime_is_preserved_when_local_schema_dependency_is_added() {
    // Given
    let sandbox = Sandbox::new();
    std::fs::create_dir(sandbox.0.join("dep")).unwrap();
    std::fs::write(
        sandbox.0.join("dep/schema.yaml"),
        "version: 3\nschema: {project: {name: ping, version: 1.2.3}}\n",
    )
    .unwrap();
    let manifest = sandbox.0.join("main.yaml");
    std::fs::write(
        &manifest,
        "version: 3\ndeployment: demo\nproviders: {process: {type: process}}\nrun: {provider: process, bin: demo}\nbuild: {type: cargo, features: [fast], locked: false}\n",
    ).unwrap();
    let before = yaml(&manifest);
    let catalog = sandbox.0.join("schema.yaml");
    std::fs::write(
        &catalog,
        "version: 3\nschema:\n  project: {name: demo}\n  config: {count: {type: u32}}\n",
    )
    .unwrap();
    let before_catalog = yaml(&catalog);
    // When
    sandbox.success(&["add", "dep"]);
    // Then
    let after = yaml(&manifest);
    for field in ["version", "deployment", "providers", "run", "build"] {
        assert_eq!(after[field], before[field]);
    }
    let after_catalog = yaml(&catalog);
    assert_eq!(
        after_catalog["schema"]["config"],
        before_catalog["schema"]["config"]
    );
    assert_eq!(
        after_catalog["schema"]["dependencies"]["ping"]["path"],
        "dep"
    );
    assert_eq!(
        after_catalog["schema"]["dependencies"]["ping"]["version"],
        "1.2"
    );
}

#[test]
fn manifest_bytes_are_unchanged_when_dependency_already_exists() {
    // Given
    let sandbox = Sandbox::new();
    std::fs::create_dir(sandbox.0.join("dep")).unwrap();
    std::fs::write(
        sandbox.0.join("dep/schema.yaml"),
        "version: 3\nschema: {project: {name: ping}}\n",
    )
    .unwrap();
    let manifest = sandbox.0.join("schema.yaml");
    let text = "# retain formatting\nversion: 3\nschema: {dependencies: {ping: {path: elsewhere, version: '7.0'}}}\n";
    std::fs::write(&manifest, text).unwrap();
    // When
    sandbox.success(&["add", "dep"]);
    // Then
    assert_eq!(std::fs::read_to_string(manifest).unwrap(), text);
}

#[test]
fn empty_scaffold_declares_no_ports_when_created() {
    // Given
    let sandbox = Sandbox::new();
    // When
    sandbox.success(&["new", "demo"]);
    // Then
    let manifest = yaml(&sandbox.0.join("demo/main.yaml"));
    assert_eq!(manifest["version"], 3);
    assert!(manifest["in"].as_mapping().unwrap().is_empty());
    assert!(manifest["out"].as_mapping().unwrap().is_empty());
    assert_eq!(manifest["build"]["locked"], false);
    assert_eq!(manifest["run"]["provider"], "process");
    sandbox.success(&["check", "demo"]);
}

#[test]
fn raw_cargo_arguments_are_rejected_when_deployment_needs_no_build() {
    // Given: this deployment prepares successfully if arguments are ignored.
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.0.join("main.yaml"),
        "version: 3\ndeployment: demo\n",
    )
    .unwrap();
    // When
    let output = sandbox.run(&["build", "--", "--features", "other"]);
    // Then
    assert!(!output.status.success());
    assert!(!sandbox.0.join(".reiny/build").exists());
}
