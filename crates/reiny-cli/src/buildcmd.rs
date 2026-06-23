//! `reiny build` — Reiny.toml 駆動の codegen を確実に走らせてからビルドするラッパ。
//!
//! 実体は cwd(grain プロジェクト)での `cargo build`。codegen は各 grain の `build.rs` が
//! 呼ぶ `reiny_build::compile()` が cargo build の一部として走るので、ここは薄いラッパでよい。

use anyhow::{Context, Result, bail};

/// `reiny build [--release] [-- <extra cargo args>]`。
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
