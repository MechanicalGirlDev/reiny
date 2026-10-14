//! Data command contract failures must precede engine or file operations.

#![allow(clippy::unwrap_used)]

use std::process::Command;

#[test]
fn every_data_command_requires_a_prepared_contract() {
    for args in [
        vec!["topic", "pub", "state", "{}"],
        vec!["topic", "echo", "state", "--raw"],
        vec!["topic", "hz", "state"],
        vec!["topic", "bw", "state"],
        vec!["service", "call", "calibrate"],
        vec!["bag", "record", "--out", "must-not-exist.mcap"],
        vec!["bag", "play", "missing.mcap"],
        vec!["bridge", "udp", "invalid-address"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_reiny"))
            .args(&args)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("require --module-bindings"),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn control_plane_commands_do_not_require_endpoint_contracts() {
    for command in ["topic", "node", "service"] {
        let output = Command::new(env!("CARGO_BIN_EXE_reiny"))
            .args([command, "list", "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
    }
}

#[test]
fn owner_identity_alias_is_validated_before_engine_startup() {
    let directory = tempfile::tempdir().unwrap();
    let bindings = directory.path().join("bindings.json");
    let contract: reiny::bindings::ModuleBindings = serde_yaml::from_str(
        "version: 3\nnamespace: cli\nendpoint_namespace: cli\ninputs:\n  state:\n    contract: {type: test.State}\n    sources: [{namespace: controller, port: state}]\noutputs: {}\n",
    ).unwrap();
    std::fs::write(&bindings, serde_json::to_vec(&contract).unwrap()).unwrap();
    for identity in ["--id", "--name"] {
        let output = Command::new(env!("CARGO_BIN_EXE_reiny"))
            .args([
                "topic",
                "echo",
                "state",
                identity,
                "other",
                "--domain",
                "lab",
                "--module-bindings",
            ])
            .arg(&bindings)
            .args(["--zenoh-config", "must-not-be-read.json"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("--id"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("must-not-be-read"));
    }
}
