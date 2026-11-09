//! Explicit root selection through the CLI, without starting a deployment.

use std::fs;
use std::process::Command;

use anyhow::{Context, Result, ensure};
use reiny_launch::DeploymentPlan;

#[test]
fn selected_root_controls_plan_check_and_bundle() -> Result<()> {
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("source");
    fs::create_dir_all(root.join("installed"))?;
    fs::create_dir(root.join("settings"))?;
    let root = root.canonicalize()?;
    fs::copy(
        std::env::current_exe()?,
        root.join("installed")
            .join(format!("worker{}", std::env::consts::EXE_SUFFIX)),
    )?;
    for name in ["main", "deployment", "alternative"] {
        fs::write(
            root.join(format!("settings/{name}.yaml")),
            format!("value: {name}"),
        )?;
        fs::write(root.join(format!("settings/{name}.bin")), name)?;
        fs::write(
            root.join(format!("{name}.yaml")),
            format!(
                "version: 3\ndeployment: {name}\nproviders:\n  {name}: {{type: process, bin_dir: installed}}\nrun:\n  provider: {name}\n  bin: worker\n  config: settings/{name}.yaml\n  config_assets: [{name}.bin]\n"
            ),
        )?;
    }
    let alternative = root.join("alternative.yaml");
    let mut declaration = fs::read_to_string(&alternative)?;
    declaration.push_str("build: {type: cargo, manifest: never-build.toml}\n");
    fs::write(alternative, declaration)?;
    // Distinct documents in one directory cannot fall back to main.yaml.
    for (input, selected) in [
        (".", "main"),
        ("main.yaml", "main"),
        ("deployment.yaml", "deployment"),
        ("alternative.yaml", "alternative"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_reiny"))
            .current_dir(&root)
            .args(["plan", input, "--json"])
            .output()?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let plan: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(plan["deployment"], selected);
        assert_eq!(plan["nodes"][0]["run"]["provider"], selected);
        assert_eq!(
            plan["root_manifest"].as_str(),
            root.join(format!("{selected}.yaml")).to_str()
        );
        assert_eq!(plan["module_sources"][selected].as_str(), root.to_str());
        assert_eq!(
            plan["nodes"][0]["run"]["config"].as_str(),
            root.join(format!("settings/{selected}.yaml")).to_str()
        );
        assert_eq!(
            plan["nodes"][0]["run"]["config_assets"][0],
            format!("{selected}.bin")
        );
        let checked = Command::new(env!("CARGO_BIN_EXE_reiny"))
            .current_dir(&root)
            .args(["check", input])
            .output()?;
        ensure!(
            checked.status.success(),
            "{}",
            String::from_utf8_lossy(&checked.stderr)
        );
    }

    fs::remove_file(root.join("main.yaml"))?;
    let destination = workspace.path().join("bundle");
    let bundled = Command::new(env!("CARGO_BIN_EXE_reiny"))
        .current_dir(&root)
        .args(["bundle", "deployment.yaml", "--out"])
        .arg(&destination)
        .output()?;
    ensure!(
        bundled.status.success(),
        "{}",
        String::from_utf8_lossy(&bundled.stderr)
    );
    fs::remove_dir_all(&root)?;
    let plan = DeploymentPlan::load(&destination, false)?;
    assert_eq!(plan.deployment, "deployment");
    let prepared = plan.prepare()?;
    let config = prepared.nodes[0].run.config.as_ref().context("config")?;
    assert_eq!(fs::read_to_string(config)?, "value: deployment");
    assert_eq!(
        fs::read_to_string(
            config
                .parent()
                .context("config parent")?
                .join("deployment.bin")
        )?,
        "deployment"
    );
    Ok(())
}

#[test]
fn explicit_runtime_check_never_substitutes_a_sibling_manifest_or_catalog() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::write(
        root.path().join("main.yaml"),
        "version: 3\nrun: {provider: process, bin: absent}\n",
    )?;
    for text in [
        "version: 3\n",
        "version: 3\nschema: {project: {name: catalog}}\n",
    ] {
        fs::write(root.path().join("deployment.yaml"), text)?;
        for command in ["plan", "check"] {
            let output = Command::new(env!("CARGO_BIN_EXE_reiny"))
                .current_dir(root.path())
                .args([command, "deployment.yaml"])
                .output()?;
            assert!(!output.status.success());
        }
    }
    Ok(())
}
