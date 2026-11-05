use super::super::{DeploymentPlan, ROOT, fixture, fs, write};
use reiny_launch::{PrepareEvent, PrepareObserver, PrepareOptions, PrepareStatus};
use std::sync::Mutex;

#[derive(Default)]
struct Recorder(Mutex<Vec<PrepareEvent>>);

impl PrepareObserver for Recorder {
    fn event(&self, event: &PrepareEvent) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
    }
}

#[test]
fn hosted_public_preparation_freezes_exact_host_assignments() -> anyhow::Result<()> {
    let root = fixture()?;
    for name in ["sensor-host", "control-host"] {
        write(
            root.path(),
            &format!("absent-binaries/{name}{}", std::env::consts::EXE_SUFFIX),
            name,
        )?;
    }
    let plan = DeploymentPlan::load(root.path(), false)?;
    let recorder = Recorder::default();

    let prepared = plan.prepare_with(&PrepareOptions {
        observer: Some(&recorder),
        ..PrepareOptions::default()
    })?;
    prepared.verify()?;
    assert!(prepared.nodes.is_empty());
    assert_eq!(
        prepared.component_hosts["sensors"].instances,
        ["robot/branch/left", "robot/branch/right"]
    );
    assert_eq!(
        prepared.component_hosts["control"].instances,
        ["robot/controller"]
    );
    assert_eq!(prepared.components.len(), 3);
    let events = recorder
        .0
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(events.first(), Some(&PrepareEvent::Started));
    assert!(matches!(
        events.last(),
        Some(PrepareEvent::Finished {
            status: PrepareStatus::Success,
            ..
        })
    ));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, PrepareEvent::Finished { .. }))
            .count(),
        1
    );
    assert_eq!(fs::read_to_string(root.path().join("main.yaml"))?, ROOT);
    Ok(())
}

#[test]
fn version_four_root_keeps_legacy_only_preparation_and_frozen_integrity() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    write(
        root.path(),
        "main.yaml",
        r"version: 4
deployment: legacy
providers:
  local: {type: process, bin_dir: bin}
modules:
  app: {source: app}
",
    )?;
    write(
        root.path(),
        "app/main.yaml",
        r"version: 3
run: {provider: local, bin: app, config: config.yaml, config_assets: [asset.bin]}
",
    )?;
    write(root.path(), "app/config.yaml", "original config")?;
    write(root.path(), "app/asset.bin", "original asset")?;
    write(
        root.path(),
        &format!("bin/app{}", std::env::consts::EXE_SUFFIX),
        "original executable",
    )?;
    let prepared = DeploymentPlan::load(root.path(), false)?.prepare()?;
    let node = &prepared.nodes[0];

    write(root.path(), "app/config.yaml", "changed config")?;
    write(root.path(), "app/asset.bin", "changed asset")?;
    fs::remove_dir_all(root.path().join("bin"))?;
    prepared.verify()?;

    let config = node
        .run
        .config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("frozen config"))?;
    let origin = node
        .config_dir
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("frozen origin"))?;
    assert_eq!(fs::read_to_string(config)?, "original config");
    assert_eq!(
        fs::read_to_string(origin.join("asset.bin"))?,
        "original asset"
    );
    assert_eq!(fs::read_to_string(&node.executable)?, "original executable");
    Ok(())
}
