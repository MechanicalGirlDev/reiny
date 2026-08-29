//! `reiny build` — the wrapper that makes sure the Reiny.toml-driven codegen runs before the build.
//!
//! It is really `cargo build` in the cwd (a launch project). The codegen runs as part of that build,
//! from the `reiny_build::compile()` each launch's `build.rs` calls, so a thin wrapper is all this needs.

use anyhow::{Context, Result, bail};

/// `reiny build [--release] [-- <extra cargo args>]`.
pub(crate) fn build(release: bool, extra: &[String]) -> Result<()> {
    let mut cmd = std::process::Command::new("cargo");
    cmd.arg("build");
    if release {
        cmd.arg("--release");
    }
    cmd.args(extra);

    let status = cmd
        .status()
        .context("running `cargo build` (is cargo on PATH?)")?;
    if !status.success() {
        bail!("cargo build failed with {status}");
    }
    Ok(())
}
