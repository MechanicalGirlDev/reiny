//! Hosted plans resolve real manifests without granting process execution.

use std::fs;
use std::path::Path;

use reiny_core::bindings::{EndpointAddress, MODULE_BINDINGS_VERSION};
use reiny_launch::DeploymentPlan;

mod placements {
    mod invalid;
    mod local;
    mod preparation;
}

fn write(root: &Path, name: &str, text: &str) -> anyhow::Result<()> {
    let path = root.join(name);
    fs::create_dir_all(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("fixture parent"))?,
    )?;
    fs::write(path, text)?;
    Ok(())
}

const ROOT: &str = r"version: 4
deployment: robot
providers:
  local: {type: process, bin_dir: absent-binaries, connect: ['tcp/127.0.0.1:7447']}
component_hosts:
  sensors: {provider: local, bin: sensor-host}
  control: {provider: local, bin: control-host}
placement:
  robot/branch/left: sensors
  robot/branch/right: sensors
  robot/controller: control
modules:
  branch: {source: branch}
  controller:
    source: controller
    in:
      state: {from: [branch.right, branch.left]}
";
const SENSOR: &str = r"version: 4
component: {factory: sensor, factory_revision: '1'}
out:
  state: {type: robot.State, retention: last}
";
const CONTROLLER: &str = r"version: 4
component: {factory: controller, factory_revision: '2'}
in:
  state: {type: robot.State, replay: last}
";

fn fixture() -> anyhow::Result<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    write(root.path(), "main.yaml", ROOT)?;
    write(
        root.path(),
        "branch/main.yaml",
        r"version: 3
modules:
  left: {source: sensor}
  right: {source: sensor}
out:
  left: {from: left.state}
  right: {from: right.state}
",
    )?;
    write(root.path(), "branch/sensor/main.yaml", SENSOR)?;
    write(root.path(), "controller/main.yaml", CONTROLLER)?;
    Ok(root)
}

#[test]
fn hosted_plan_preserves_exact_sources_through_legacy_composite_aliases() -> anyhow::Result<()> {
    let root = fixture()?;

    let plan = DeploymentPlan::load(&root.path().join("main.yaml"), false)?;

    assert!(plan.nodes.is_empty());
    assert_eq!(
        plan.components
            .iter()
            .map(|node| (node.namespace.as_str(), node.host.as_str()))
            .collect::<Vec<_>>(),
        [
            ("robot/branch/left", "sensors"),
            ("robot/branch/right", "sensors"),
            ("robot/controller", "control"),
        ]
    );
    let consumer = &plan.components[2];
    assert_eq!(
        consumer.bindings.inputs["state"].sources,
        [
            EndpointAddress {
                namespace: "robot/branch/right".into(),
                port: "state".into()
            },
            EndpointAddress {
                namespace: "robot/branch/left".into(),
                port: "state".into()
            },
        ]
    );
    assert_eq!(plan.connections.len(), 2);
    assert_eq!(plan.connections[0].via[0].namespace, "robot/branch");
    assert_eq!(plan.connections[0].via[0].name, "right");
    for node in &plan.components {
        assert_eq!(node.bindings.version, MODULE_BINDINGS_VERSION);
        assert_eq!(node.bindings.endpoint_namespace, node.namespace);
        assert_eq!(node.bindings.executables, Vec::<String>::new());
        assert!(node.bindings.children.is_empty());
    }
    assert_eq!(
        plan.component_hosts["control"].provider.connect,
        ["tcp/127.0.0.1:7447"]
    );
    Ok(())
}

#[test]
fn planning_does_not_run_host_builders_or_require_their_outputs() -> anyhow::Result<()> {
    let root = fixture()?;
    write(root.path(), "main.yaml", &ROOT.replace(
        "control: {provider: local, bin: control-host}",
        "control:\n    provider: local\n    bin: control-host\n    build:\n      type: command\n      argv: [reiny-test-must-never-execute]\n      outputs: {control-host: absent-output}",
    ))?;

    let plan = DeploymentPlan::load(root.path(), false)?;

    assert!(plan.component_hosts["control"].spec.build.is_some());
    assert!(!root.path().join("absent-output").exists());
    assert!(!root.path().join(".reiny/build").exists());
    Ok(())
}

#[test]
fn component_config_overrides_keep_each_callers_origin_and_asset_selection() -> anyhow::Result<()> {
    let root = fixture()?;
    write(
        root.path(),
        "branch/sensor/main.yaml",
        &SENSOR.replace(
            "factory_revision: '1'",
            "factory_revision: '1', config: default.yaml, config_assets: [default.bin]",
        ),
    )?;
    write(root.path(), "branch/sensor/default.yaml", "default")?;
    write(root.path(), "branch/sensor/default.bin", "default asset")?;
    write(root.path(), "branch/instances/left.yaml", "left")?;
    write(root.path(), "branch/instances/right.yaml", "right")?;
    write(
        root.path(),
        "branch/main.yaml",
        r"version: 3
modules:
  left: {source: sensor, config: instances/left.yaml, config_assets: []}
  right: {source: sensor, config: instances/right.yaml, config_assets: [../assets/right.bin]}
out:
  left: {from: left.state}
  right: {from: right.state}
",
    )?;
    write(root.path(), "branch/assets/right.bin", "right asset")?;

    let plan = DeploymentPlan::load(root.path(), false)?;

    assert_eq!(
        plan.components[0].component.config,
        Some(root.path().join("branch/instances/left.yaml"))
    );
    assert_eq!(
        plan.components[1].component.config,
        Some(root.path().join("branch/instances/right.yaml"))
    );
    assert_eq!(
        plan.components[0].component.config_assets,
        Vec::<std::path::PathBuf>::new()
    );
    assert_eq!(
        plan.components[1].component.config_assets,
        [Path::new("../assets/right.bin")]
    );
    Ok(())
}
