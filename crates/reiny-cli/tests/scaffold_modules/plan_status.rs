use super::{PathBuf, Sandbox};

#[test]
fn runtime_plan_accepts_a_bare_manifest_filename_in_its_own_directory() {
    for filename in ["main.yaml", "deployment.yaml"] {
        // Given a deployment root in an isolated command working directory.
        let sandbox = Sandbox::new();
        std::fs::write(sandbox.0.join(filename),
        "version: 3\ndeployment: bare-manifest\nproviders:\n  process: {type: process}\nrun: {provider: process, bin: missing-on-purpose}\n"
    ).unwrap();
        // When the CLI receives the filename without a directory component.
        let output = sandbox.run(&["plan", filename, "--json"]);
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
            component_hosts: std::collections::BTreeMap::new(),
            components: std::collections::BTreeMap::new(),
            resources: std::collections::BTreeMap::new(),
            error: None,
        };
        std::fs::create_dir_all(sandbox.0.join(".reiny")).unwrap();
        std::fs::write(
            sandbox.0.join(".reiny/state.json"),
            serde_json::to_vec(&recorded).unwrap(),
        )
        .unwrap();
        let status = sandbox.run(&["status", filename, "--json"]);
        assert!(
            status.status.success(),
            "{}",
            String::from_utf8_lossy(&status.stderr)
        );
        let observed: reiny_launch::DeploymentStatus =
            serde_json::from_slice(&status.stdout).unwrap();
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
                .run(&["status", filename, "--json"])
                .status
                .success()
        );
    }
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
