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
    assert_eq!(manifest["version"], 1);
    assert_eq!(manifest["deployment"], "demo");
    assert_eq!(manifest["out"]["ping"]["type"], "ping.Ping");
    assert_eq!(
        manifest["schema"]["publications"]["Ping"]["message"],
        "ping.Ping"
    );
    assert_eq!(manifest["build"]["locked"], false);
    assert!(manifest["in"].as_mapping().unwrap().is_empty());
    assert!(!dir.join("Reiny.toml").exists());
    let cargo: toml::Table =
        toml::from_str(&std::fs::read_to_string(dir.join("Cargo.toml")).unwrap()).unwrap();
    assert!(cargo["workspace"].is_table());
    assert!(cargo["dependencies"]["reiny"]["path"].is_str());
}

#[test]
fn user_values_and_sources_survive_when_initialized() {
    // Given
    let sandbox = Sandbox::new();
    std::fs::create_dir(sandbox.0.join("src")).unwrap();
    let source = "fn main() { println!(\"existing\"); }\n";
    let build = "fn main() {}\n";
    let manifest = "version: 1\nschema: {project: {name: custom}}\n";
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
        sandbox.0.join("dep/main.yaml"),
        "version: 1\nschema: {project: {name: ping, version: 1.2.3}}\n",
    )
    .unwrap();
    let manifest = sandbox.0.join("main.yaml");
    std::fs::write(
        &manifest,
        "version: 1\ndeployment: demo\nproviders: {process: {type: process}}\nrun: {provider: process, bin: demo}\nbuild: {type: cargo, features: [fast], locked: false}\nschema:\n  project: {name: demo}\n  config: {count: {type: u32}}\n",
    ).unwrap();
    let before = yaml(&manifest);
    // When
    sandbox.success(&["add", "dep"]);
    // Then
    let after = yaml(&manifest);
    for field in ["version", "deployment", "providers", "run", "build"] {
        assert_eq!(after[field], before[field]);
    }
    assert_eq!(after["schema"]["config"], before["schema"]["config"]);
    assert_eq!(after["schema"]["dependencies"]["ping"]["path"], "dep");
    assert_eq!(after["schema"]["dependencies"]["ping"]["version"], "1.2");
}

#[test]
fn manifest_bytes_are_unchanged_when_dependency_already_exists() {
    // Given
    let sandbox = Sandbox::new();
    std::fs::create_dir(sandbox.0.join("dep")).unwrap();
    std::fs::write(
        sandbox.0.join("dep/main.yaml"),
        "version: 1\nschema: {project: {name: ping}}\n",
    )
    .unwrap();
    let manifest = sandbox.0.join("main.yaml");
    let text = "# retain formatting\nversion: 1\nschema: {dependencies: {ping: {path: elsewhere, version: '7.0'}}}\n";
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
        "version: 1\ndeployment: demo\nproviders: {process: {type: process}}\nrun: {provider: process, bin: demo}\nbuild: {type: cargo, manifest: absent.toml}\n",
    ).unwrap();
    // When
    sandbox.success(&["check", "."]);
    // Then
    assert!(!sandbox.0.join(".reiny/build").exists());
}

#[test]
fn empty_scaffold_declares_no_ports_when_created() {
    // Given
    let sandbox = Sandbox::new();
    // When
    sandbox.success(&["new", "demo"]);
    // Then
    let manifest = yaml(&sandbox.0.join("demo/main.yaml"));
    assert!(manifest["in"].as_mapping().unwrap().is_empty());
    assert!(manifest["out"].as_mapping().unwrap().is_empty());
    assert_eq!(manifest["build"]["locked"], false);
    assert_eq!(manifest["run"]["provider"], "process");
}

#[test]
fn raw_cargo_arguments_are_rejected_when_deployment_needs_no_build() {
    // Given: this deployment prepares successfully if arguments are ignored.
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.0.join("main.yaml"),
        "version: 1\ndeployment: demo\n",
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
        sandbox.0.join("main.yaml"),
        "version: 1\nschema: {project: {name: demo}, publications: {}}\n",
    )
    .unwrap();
    std::fs::write(
        sandbox.0.join("leaf/main.yaml"),
        "version: 1\nrun: {provider: process, bin: demo}\n",
    )
    .unwrap();
    // When
    sandbox.success(&["check", "leaf/src"]);
    // Then
    assert!(!sandbox.0.join(".reiny/build").exists());
}
