//! Drive scaffold and dependency operations through the CLI with isolated module directories.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_yaml::Value;

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct Sandbox(PathBuf);

impl Sandbox {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "reiny-scaffold-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_reiny"))
            .current_dir(&self.0)
            .args(args)
            .output()
            .unwrap()
    }

    fn success(&self, args: &[&str]) {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("removing {}: {error}", self.0.display());
        }
    }
}

fn yaml(path: &Path) -> Value {
    serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

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
fn runtime_plan_accepts_a_bare_manifest_filename_in_its_own_directory() {
    // Given a deployment root in an isolated command working directory.
    let sandbox = Sandbox::new();
    std::fs::write(sandbox.0.join("main.yaml"),
        "version: 3\ndeployment: bare-manifest\nproviders:\n  process: {type: process}\nrun: {provider: process, bin: missing-on-purpose}\n"
    ).unwrap();
    // When the CLI receives the filename without a directory component.
    let output = sandbox.run(&["plan", "main.yaml", "--json"]);
    // Then it resolves the current directory without changing the test runner or building.
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        PathBuf::from(plan["root"].as_str().unwrap()),
        sandbox.0.canonicalize().unwrap()
    );
    assert_eq!(plan["nodes"][0]["namespace"], "bare-manifest");
    let recorded = reiny_launch::DeploymentStatus {
        version: 2,
        root: sandbox.0.canonicalize().unwrap(),
        deployment: "bare-manifest".into(),
        domain: "bare-manifest".into(),
        owner_generation: "recorded-owner".into(),
        owner_pid: 0,
        owner_alive: false,
        revision: 1,
        phase: reiny_launch::DeploymentPhase::Stopped,
        modules: std::collections::BTreeMap::new(),
        resources: std::collections::BTreeMap::new(),
        error: None,
    };
    std::fs::create_dir_all(sandbox.0.join(".reiny")).unwrap();
    std::fs::write(
        sandbox.0.join(".reiny/state.json"),
        serde_json::to_vec(&recorded).unwrap(),
    )
    .unwrap();
    let status = sandbox.run(&["status", "main.yaml", "--json"]);
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    let observed: reiny_launch::DeploymentStatus = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(observed, recorded);
    let mut legacy = recorded;
    legacy.version = 1;
    std::fs::write(
        sandbox.0.join(".reiny/state.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();
    assert!(
        !sandbox
            .run(&["status", "main.yaml", "--json"])
            .status
            .success()
    );
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

#[test]
fn plan_json_retains_named_ports_and_alias_paths_without_building() {
    // Given a composite exposes a differently named executable output.
    let sandbox = Sandbox::new();
    for directory in ["exporter/producer", "consumer"] {
        std::fs::create_dir_all(sandbox.0.join(directory)).unwrap();
    }
    for (path, text) in [
        (
            "main.yaml",
            "version: 3\ndeployment: demo\nproviders: {process: {type: process}}\nmodules:\n  exporter: {source: exporter}\n  consumer: {source: consumer, in: {incoming: {from: exporter.exposed}}}\n",
        ),
        (
            "exporter/main.yaml",
            "version: 3\nmodules: {producer: {source: producer}}\nout: {exposed: {from: producer.sent}}\n",
        ),
        (
            "exporter/producer/main.yaml",
            "version: 3\nrun: {provider: process, bin: absent-producer}\nout: {sent: {type: test.State}}\n",
        ),
        (
            "consumer/main.yaml",
            "version: 3\nrun: {provider: process, bin: absent-consumer}\nin: {incoming: {type: test.State}}\n",
        ),
    ] {
        std::fs::write(sandbox.0.join(path), text).unwrap();
    }
    // When the real CLI resolves the runtime definition.
    let output = sandbox.run(&["plan", "main.yaml", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    // Then its graph retains source, destination and intermediate public aliases.
    let connections = plan["connections"].as_array().unwrap();
    assert_eq!(connections.len(), 1);
    assert_eq!(
        connections[0]["source"],
        serde_json::json!({
            "namespace": "demo/exporter/producer", "input": false, "name": "sent"
        })
    );
    assert_eq!(
        connections[0]["destination"],
        serde_json::json!({
            "namespace": "demo/consumer", "input": true, "name": "incoming"
        })
    );
    assert_eq!(
        connections[0]["via"],
        serde_json::json!([{
            "namespace": "demo/exporter", "input": false, "name": "exposed"
        }])
    );
    assert_eq!(connections[0]["contract"]["type"], "test.State");
    assert!(!sandbox.0.join(".reiny/build").exists());
}
