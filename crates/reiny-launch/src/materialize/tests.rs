use std::{fs, path::Path};

use anyhow::{Context, Result};

use super::{DeploymentPlan, Events, PrepareOptions, PreparedDeployment, transaction};

#[cfg(unix)]
mod builds;
mod integrity;
mod metadata;

fn write(root: &Path, name: &str, bytes: &str) -> Result<()> {
    let path = root.join(name);
    fs::create_dir_all(path.parent().context("fixture parent")?)?;
    fs::write(path, bytes)?;
    Ok(())
}

fn fixture() -> Result<(tempfile::TempDir, DeploymentPlan)> {
    let root = tempfile::tempdir()?;
    write(
        root.path(),
        "main.yaml",
        r"version: 4
deployment: robot
providers:
  local: {type: process, bin_dir: bin, zenoh_config: zenoh.json5, connect: ['tcp/127.0.0.1:7447']}
component_hosts:
  sensors: {provider: local, bin: host}
  control: {provider: local, bin: host}
placement:
  robot/sensor: sensors
  robot/controller: control
modules:
  sensor: {source: sensor, config: configs/sensor/config.yaml, config_assets: [data.bin]}
  controller:
    source: controller
    config: configs/controller/config.yaml
    config_assets: [../assets/table.bin]
    in: {state: {from: sensor.state}}
",
    )?;
    write(
        root.path(),
        "sensor/main.yaml",
        r"version: 4
component: {factory: sensor, factory_revision: 'sensor-v1'}
out: {state: {type: robot.State, retention: last}}
",
    )?;
    write(
        root.path(),
        "controller/main.yaml",
        r"version: 4
component: {factory: controller, factory_revision: 'controller-v2'}
in: {state: {type: robot.State, replay: last}}
",
    )?;
    write(root.path(), "configs/sensor/config.yaml", "sensor config")?;
    write(root.path(), "configs/sensor/data.bin", "sensor asset")?;
    write(
        root.path(),
        "configs/controller/config.yaml",
        "controller config",
    )?;
    write(root.path(), "configs/assets/table.bin", "controller asset")?;
    write(root.path(), "zenoh.json5", "{}")?;
    write(
        root.path(),
        &format!("bin/host{}", std::env::consts::EXE_SUFFIX),
        "host executable",
    )?;
    write(root.path(), "bin/runtime.dll", "runtime dependency")?;
    let plan = DeploymentPlan::load(root.path(), false)?;
    Ok((root, plan))
}

// Exercise the same freezing transaction as public preparation.
fn stage(plan: &DeploymentPlan) -> Result<PreparedDeployment> {
    Ok(transaction(
        plan,
        &PrepareOptions {
            build_cache: Some(plan.root.join(".private-build")),
            ..PrepareOptions::default()
        },
        &Events::new(None),
    )?)
}

#[test]
fn frozen_hosted_instances_survive_source_edits_and_removal() -> Result<()> {
    let (root, plan) = fixture()?;
    let prepared = stage(&plan)?;
    let output = root.path().join("prepared.json");
    prepared.write_file(&output)?;
    write(root.path(), "configs/sensor/config.yaml", "changed config")?;
    write(root.path(), "configs/assets/table.bin", "changed asset")?;
    fs::remove_dir_all(root.path().join("bin"))?;
    fs::remove_dir_all(root.path().join("configs"))?;
    fs::remove_file(root.path().join("zenoh.json5"))?;

    let loaded = PreparedDeployment::load_file(&output)?;

    assert!(loaded.nodes.is_empty());
    assert_eq!(
        loaded.component_hosts["sensors"].instances,
        ["robot/sensor"]
    );
    assert_eq!(
        loaded.component_hosts["control"].instances,
        ["robot/controller"]
    );
    for host in loaded.component_hosts.values() {
        assert_eq!(fs::read_to_string(&host.executable)?, "host executable");
        assert_eq!(
            fs::read_to_string(host.bundle_dir.join("runtime.dll"))?,
            "runtime dependency"
        );
        assert_eq!(
            fs::read_to_string(host.provider.zenoh_config.as_ref().context("transport")?)?,
            "{}"
        );
        assert_eq!(host.provider.connect, ["tcp/127.0.0.1:7447"]);
    }
    let controller = &loaded.components[0];
    let sensor = &loaded.components[1];
    assert_eq!(controller.component.factory_revision, "controller-v2");
    assert_eq!(controller.bindings, plan.components[0].bindings);
    assert_eq!(sensor.bindings, plan.components[1].bindings);
    assert_ne!(controller.config_dir, sensor.config_dir);
    assert_eq!(
        fs::read_to_string(controller.component.config.as_ref().context("config")?)?,
        "controller config"
    );
    assert_eq!(
        fs::read_to_string(
            controller
                .config_dir
                .as_ref()
                .context("origin")?
                .join("../assets/table.bin")
        )?,
        "controller asset"
    );
    assert_eq!(
        fs::read_to_string(sensor.component.config.as_ref().context("config")?)?,
        "sensor config"
    );
    assert_eq!(
        fs::read_to_string(
            sensor
                .config_dir
                .as_ref()
                .context("origin")?
                .join("data.bin")
        )?,
        "sensor asset"
    );
    Ok(())
}

#[test]
fn missing_current_sources_never_fall_back_to_old_host_artifacts() -> Result<()> {
    let (root, plan) = fixture()?;
    let prepared = stage(&plan)?;
    fs::remove_file(
        root.path()
            .join(format!("bin/host{}", std::env::consts::EXE_SUFFIX)),
    )?;

    let error = stage(&plan)
        .err()
        .context("missing current executable must fail")?;

    assert!(error.to_string().contains("prebuilt executable is missing"));
    prepared.verify()?;
    Ok(())
}

#[test]
fn legacy_v1_envelope_keeps_its_original_payload_shape() -> Result<()> {
    let (root, _) = fixture()?;
    write(
        root.path(),
        "main.yaml",
        r"version: 3
deployment: robot
providers:
  local: {type: process, bin_dir: bin}
run: {provider: local, bin: host}
",
    )?;
    let prepared = DeploymentPlan::load(root.path(), false)?.prepare()?;
    let path = root.path().join("legacy.json");
    prepared.write_file(&path)?;
    let envelope: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;

    let loaded = PreparedDeployment::load_file(&path)?;

    assert_eq!(envelope["version"], 1);
    assert!(envelope["deployment"].get("components").is_none());
    assert!(envelope["deployment"].get("component_hosts").is_none());
    assert_eq!(loaded.nodes[0].fingerprint, prepared.nodes[0].fingerprint);
    assert!(loaded.components.is_empty());
    for (field, value) in [
        ("components", serde_json::json!([])),
        ("component_hosts", serde_json::json!({})),
    ] {
        let mut widened = envelope.clone();
        widened["deployment"][field] = value;
        fs::write(&path, serde_json::to_vec(&widened)?)?;
        assert!(PreparedDeployment::load_file(&path).is_err());
    }
    Ok(())
}
