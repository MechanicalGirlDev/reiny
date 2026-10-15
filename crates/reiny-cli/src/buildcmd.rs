//! Prepare declared deployment artifacts without starting application processes.

use crate::control::Control;
use crate::preparecmd::{self, BuildOptions, Events, Protocol};
use anyhow::{Context, Result, bail};

/// Build cwd's `main.yaml`; `--release` overrides declared Cargo profiles.
/// Raw Cargo arguments are rejected: features and build settings belong in `build`.
pub(crate) fn build(release: bool, events: Option<Events>, extra: &[String]) -> Result<()> {
    if !extra.is_empty() {
        bail!(
            "raw Cargo arguments are unsupported; declare features and build settings in main.yaml"
        );
    }
    let root = std::env::current_dir().context("resolving current directory")?;
    let control = Control::install(false)?;
    let protocol = Protocol::new(events);
    let prepared = preparecmd::prepare(
        &root.join("main.yaml"),
        None,
        &BuildOptions {
            release,
            ..BuildOptions::default()
        },
        &control,
        &protocol,
    )?;
    if !protocol.enabled() {
        println!(
            "prepared deployment '{}' ({} executable modules, {} resources)",
            prepared.deployment,
            prepared.nodes.len(),
            prepared.resources.len(),
        );
    }
    Ok(())
}
