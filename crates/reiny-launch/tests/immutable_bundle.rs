//! Immutable prepared binaries, configuration files and declared assets.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::Context;
use reiny_launch::DeploymentPlan;

fn fixture() -> anyhow::Result<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("bin"))?;
    fs::create_dir_all(root.path().join("config/nested"))?;
    fs::write(
        root.path()
            .join("bin")
            .join(format!("app{}", std::env::consts::EXE_SUFFIX)),
        b"app bytes",
    )?;
    fs::write(
        root.path()
            .join("bin")
            .join(format!("helper{}", std::env::consts::EXE_SUFFIX)),
        b"helper bytes",
    )?;
    fs::write(
        root.path().join("config/app.yaml"),
        b"asset: nested/input.bin\n",
    )?;
    fs::write(root.path().join("config/nested/input.bin"), b"asset bytes")?;
    fs::write(root.path().join("zenoh.json5"), b"{}\n")?;
    fs::write(
        root.path().join("main.yaml"),
        r"version: 2
deployment: frozen
providers:
  process:
    type: process
    bin_dir: bin
    zenoh_config: zenoh.json5
run:
  provider: process
  bin: app
  companions: [helper]
  config: config/app.yaml
  config_assets: [nested/input.bin]
",
    )?;
    Ok(root)
}

#[test]
fn source_mutation_and_deletion_cannot_change_prepared_bytes() -> anyhow::Result<()> {
    let root = fixture()?;
    let plan = DeploymentPlan::load(root.path(), false)?;
    let first = plan.prepare()?;
    let node = &first.nodes[0];
    let config = node
        .run
        .config
        .as_ref()
        .context("missing prepared config")?;
    let config_dir = node
        .config_dir
        .as_ref()
        .context("missing config directory")?;
    let helper = node
        .bundle_dir
        .join(format!("helper{}", std::env::consts::EXE_SUFFIX));
    assert_eq!(config.parent(), Some(config_dir.as_path()));
    assert_eq!(node.executable.parent(), Some(node.bundle_dir.as_path()));
    assert_ne!(
        config,
        plan.nodes[0]
            .run
            .config
            .as_ref()
            .context("missing source config")?
    );
    let fingerprint = node.fingerprint.clone();
    fs::write(
        root.path().join("config/nested/input.bin"),
        b"changed asset",
    )?;
    let second = plan.prepare()?;
    assert_ne!(fingerprint, second.nodes[0].fingerprint);
    assert_ne!(
        config_dir,
        second.nodes[0]
            .config_dir
            .as_ref()
            .context("missing changed config directory")?
    );
    fs::write(
        root.path()
            .join("bin")
            .join(format!("helper{}", std::env::consts::EXE_SUFFIX)),
        b"changed helper",
    )?;
    let third = plan.prepare()?;
    assert_ne!(second.nodes[0].fingerprint, third.nodes[0].fingerprint);
    fs::write(root.path().join("config/app.yaml"), b"changed config")?;
    let fourth = plan.prepare()?;
    assert_ne!(third.nodes[0].fingerprint, fourth.nodes[0].fingerprint);
    fs::write(root.path().join("zenoh.json5"), b"{mode: 'peer'}")?;
    let fifth = plan.prepare()?;
    assert_ne!(fourth.nodes[0].fingerprint, fifth.nodes[0].fingerprint);
    fs::write(
        root.path()
            .join("bin")
            .join(format!("app{}", std::env::consts::EXE_SUFFIX)),
        b"changed app",
    )?;
    let sixth = plan.prepare()?;
    assert_ne!(fifth.nodes[0].fingerprint, sixth.nodes[0].fingerprint);
    fs::remove_dir_all(root.path().join("bin"))?;
    fs::remove_dir_all(root.path().join("config"))?;
    fs::remove_file(root.path().join("zenoh.json5"))?;
    assert_eq!(fs::read(&node.executable)?, b"app bytes");
    assert_eq!(fs::read(helper)?, b"helper bytes");
    assert_eq!(fs::read(config)?, b"asset: nested/input.bin\n");
    assert_eq!(
        fs::read(config_dir.join("nested/input.bin"))?,
        b"asset bytes"
    );
    assert_eq!(
        fs::read(
            node.provider
                .zenoh_config
                .as_ref()
                .context("missing frozen Zenoh config")?
        )?,
        b"{}\n"
    );
    assert_eq!(node.fingerprint, fingerprint);
    Ok(())
}

#[test]
fn explicit_missing_helpers_and_conflicting_config_paths_are_rejected() -> anyhow::Result<()> {
    let root = fixture()?;
    let mut plan = DeploymentPlan::load(root.path(), false)?;
    plan.nodes[0].run.companions = vec!["missing".into()];
    assert!(plan.prepare().is_err());
    plan.nodes[0].run.companions = vec!["app".into()];
    assert!(plan.prepare().is_err());
    plan.nodes[0].run.companions = vec!["helper".into()];
    for paths in [
        vec!["app.yaml"],
        vec!["nested/input.bin", "nested/input.bin"],
        vec!["nested/input.bin", "nested/../nested/input.bin"],
        vec!["nested/input.bin", "nested/input.bin/child"],
        vec!["missing.bin"],
    ] {
        plan.nodes[0].run.config_assets = paths.into_iter().map(PathBuf::from).collect();
        assert!(plan.prepare().is_err());
    }
    plan.nodes[0].run.config_assets = vec![root.path().join("zenoh.json5")];
    assert!(plan.prepare().is_err());
    let depth = root.path().join("config").components().count();
    plan.nodes[0].run.config_assets = vec![PathBuf::from("../".repeat(depth)).join("zenoh.json5")];
    assert!(plan.prepare().is_err());
    plan.nodes[0].run.config = None;
    assert!(plan.prepare().is_err());
    Ok(())
}

#[test]
fn sibling_assets_preserve_config_origin_without_original_files() -> anyhow::Result<()> {
    let root = fixture()?;
    let mut plan = DeploymentPlan::load(root.path(), false)?;
    let selected = tempfile::tempdir()?;
    fs::create_dir(selected.path().join("configs"))?;
    fs::create_dir(selected.path().join("assets"))?;
    let config_bytes = br#"{"calibration":"../assets/calibration.bin"}"#;
    fs::write(selected.path().join("configs/receiver.json"), config_bytes)?;
    fs::write(
        selected.path().join("assets/calibration.bin"),
        b"calibration",
    )?;
    fs::write(
        selected.path().join("assets/undeclared.bin"),
        b"not bundled",
    )?;
    plan.nodes[0].run.config = Some(selected.path().join("configs/../configs/receiver.json"));
    plan.nodes[0].run.config_assets = vec![PathBuf::from("../assets/./calibration.bin")];
    let prepared = plan.prepare()?;
    let node = &prepared.nodes[0];
    let config = node.run.config.as_ref().context("missing frozen config")?;
    let origin = node.config_dir.as_ref().context("missing frozen origin")?;
    let bundle = node
        .config_bundle_dir
        .as_ref()
        .context("missing config bundle root")?;
    assert_eq!(
        config.strip_prefix(bundle)?,
        PathBuf::from("configs/receiver.json")
    );
    assert_eq!(origin, &bundle.join("configs"));
    assert!(!bundle.join("assets/undeclared.bin").exists());
    fs::write(
        selected.path().join("assets/calibration.bin"),
        b"changed calibration",
    )?;
    let changed = plan.prepare()?;
    assert_ne!(node.fingerprint, changed.nodes[0].fingerprint);
    fs::remove_dir_all(selected.path())?;
    assert_eq!(fs::read(config)?, config_bytes);
    assert_eq!(
        fs::read(origin.join("../assets/calibration.bin"))?,
        b"calibration"
    );
    Ok(())
}

#[test]
fn config_assets_follow_the_selected_config_origin() -> anyhow::Result<()> {
    let root = fixture()?;
    let mut plan = DeploymentPlan::load(root.path(), false)?;
    let selected = tempfile::tempdir()?;
    fs::create_dir(selected.path().join("nested"))?;
    fs::write(selected.path().join("override.yaml"), b"selected config")?;
    fs::write(selected.path().join("nested/input.bin"), b"selected asset")?;
    plan.nodes[0].run.config = Some(selected.path().join("override.yaml"));
    let prepared = plan.prepare()?;
    let node = &prepared.nodes[0];
    fs::remove_dir_all(selected.path().join("nested"))?;
    fs::remove_file(selected.path().join("override.yaml"))?;
    assert_eq!(
        fs::read(
            node.run
                .config
                .as_ref()
                .context("missing overridden config")?
        )?,
        b"selected config"
    );
    assert_eq!(
        fs::read(
            node.config_dir
                .as_ref()
                .context("missing overridden config directory")?
                .join("nested/input.bin")
        )?,
        b"selected asset"
    );
    Ok(())
}

#[test]
fn caller_assets_replace_defaults_and_freeze_from_selected_config_parent() -> anyhow::Result<()> {
    let root = fixture()?;
    fs::create_dir(root.path().join("app"))?;
    fs::rename(root.path().join("config"), root.path().join("app/config"))?;
    fs::write(root.path().join("app/config/override.bin"), b"app override")?;
    fs::write(
        root.path().join("app/main.yaml"),
        "version: 2\nrun: {provider: process, bin: app, config: config/app.yaml, config_assets: [nested/input.bin]}\n",
    )?;
    fs::create_dir_all(root.path().join("settings/nested"))?;
    fs::create_dir(root.path().join("assets"))?;
    fs::write(root.path().join("settings/instance.yaml"), b"caller config")?;
    fs::write(
        root.path().join("settings/nested/input.bin"),
        b"caller default",
    )?;
    fs::write(root.path().join("assets/override.bin"), b"caller override")?;
    fs::write(
        root.path().join("main.yaml"),
        r"version: 2
deployment: frozen
providers:
  process: {type: process, bin_dir: bin}
modules:
  a: {source: app}
  b: {source: app, config_assets: [override.bin]}
  c: {source: app, config_assets: []}
  d: {source: app, config: settings/instance.yaml}
  e: {source: app, config: settings/instance.yaml, config_assets: [../assets/override.bin]}
",
    )?;
    let plan = DeploymentPlan::load(root.path(), false)?;
    let prepared = plan.prepare()?;
    // Unselected app assets must not enter the caller's fingerprint or bundle.
    fs::write(
        root.path().join("settings/nested/input.bin"),
        b"changed default",
    )?;
    let changed_default = plan.prepare()?;
    assert_ne!(
        prepared.nodes[3].fingerprint,
        changed_default.nodes[3].fingerprint
    );
    assert_eq!(
        prepared.nodes[4].fingerprint,
        changed_default.nodes[4].fingerprint
    );
    fs::write(root.path().join("assets/override.bin"), b"changed override")?;
    let changed_override = plan.prepare()?;
    assert_ne!(
        prepared.nodes[4].fingerprint,
        changed_override.nodes[4].fingerprint
    );
    fs::remove_dir_all(root.path().join("app"))?;
    fs::remove_dir_all(root.path().join("settings"))?;
    fs::remove_dir_all(root.path().join("assets"))?;
    for (index, asset, bytes) in [
        (0, "nested/input.bin", b"asset bytes".as_slice()),
        (1, "override.bin", b"app override".as_slice()),
        (3, "nested/input.bin", b"caller default".as_slice()),
        (4, "../assets/override.bin", b"caller override".as_slice()),
    ] {
        let node = &prepared.nodes[index];
        let origin = node.config_dir.as_ref().context("missing frozen origin")?;
        assert_eq!(fs::read(origin.join(asset))?, bytes);
        if index == 1 || index == 4 {
            assert!(!origin.join("nested/input.bin").exists());
        }
        let config = node.run.config.as_ref().context("missing frozen config")?;
        assert_eq!(config.parent(), Some(origin.as_path()));
        assert_eq!(
            fs::read(config)?,
            if index < 3 {
                b"asset: nested/input.bin\n".as_slice()
            } else {
                b"caller config".as_slice()
            }
        );
    }
    let cleared = prepared.nodes[2]
        .config_dir
        .as_ref()
        .context("missing cleared origin")?;
    assert!(!cleared.join("nested/input.bin").exists());
    assert!(!cleared.join("override.bin").exists());
    Ok(())
}

#[test]
fn cargo_builds_only_declared_companions_and_launches_frozen_helper() -> anyhow::Result<()> {
    let root = fixture()?;
    fs::create_dir_all(root.path().join("src/bin"))?;
    fs::write(
        root.path().join("Cargo.toml"),
        r#"[workspace]
[package]
name = "frozen-helper-fixture"
version = "0.0.0"
edition = "2021"
"#,
    )?;
    fs::write(
        root.path().join("Cargo.lock"),
        "version = 4\n[[package]]\nname = \"frozen-helper-fixture\"\nversion = \"0.0.0\"\n",
    )?;
    fs::write(
        root.path().join("src/bin/app.rs"),
        r#"fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executable = std::env::current_exe()?;
    let dir = executable.parent().ok_or("missing executable parent")?;
    let output = std::process::Command::new(dir.join(format!("helper{}", std::env::consts::EXE_SUFFIX)))
        .output()?;
    assert!(output.status.success());
    print!("{}", String::from_utf8(output.stdout)?);
    Ok(())
}
"#,
    )?;
    fs::write(
        root.path().join("src/bin/helper.rs"),
        "fn main() { println!(\"frozen helper\"); }\n",
    )?;
    fs::write(
        root.path().join("src/bin/undeclared.rs"),
        "compile_error!(\"undeclared binary must not be built\");\nfn main() {}\n",
    )?;
    let mut plan = DeploymentPlan::load(root.path(), false)?;
    plan.nodes[0].build = Some(serde_yaml::from_str("type: cargo\nprofile: dev\n")?);
    let prepared = plan.prepare()?;
    let node = &prepared.nodes[0];
    let output = Command::new(&node.executable).output()?;
    assert!(output.status.success());
    assert_eq!(output.stdout, b"frozen helper\n");
    fs::remove_dir_all(root.path().join("src"))?;
    fs::remove_file(root.path().join("Cargo.toml"))?;
    fs::remove_file(root.path().join("Cargo.lock"))?;
    let output = Command::new(&node.executable).output()?;
    assert!(output.status.success());
    assert_eq!(output.stdout, b"frozen helper\n");
    Ok(())
}
