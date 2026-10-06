//! Prepare declared deployment artifacts without starting application processes.

use anyhow::{Context, Result, bail};

/// Build cwd's `main.yaml`; `--release` overrides declared Cargo profiles.
/// Raw Cargo arguments are rejected: features and build settings belong in `build`.
pub(crate) fn build(release: bool, extra: &[String]) -> Result<()> {
    if !extra.is_empty() {
        bail!(
            "raw Cargo arguments are unsupported; declare features and build settings in main.yaml"
        );
    }
    let root = std::env::current_dir().context("resolving current directory")?;
    let mut plan = reiny_launch::DeploymentPlan::load(&root.join("main.yaml"), false)?;
    if release {
        for node in &mut plan.nodes {
            if let Some(build) = &mut node.build {
                "release".clone_into(&mut build.profile);
            }
        }
    }
    let prepared = plan.prepare()?;
    println!(
        "prepared deployment '{}' ({} executable modules, {} resources)",
        prepared.deployment,
        prepared.nodes.len(),
        prepared.resources.len(),
    );
    Ok(())
}
