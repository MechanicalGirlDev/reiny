//! `reiny compress` — bundle only what it takes to run into one directory (the launcher included).
//!
//! It walks the launch config and gathers the reachable launch bins, the shared libraries they
//! actually link, the launch config and **the reiny launcher itself** into `<out>/`. With
//! `--launcher <name>`, reiny is renamed to `<name>` and the config lined up as `<name>.<ext>`
//! (the config's own extension, so its format is still told apart), so `./<name>` alone starts everything.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use reiny_launch::LaunchPlan;

use crate::runcmd::{config_dir, find_bin, search_dirs};

/// `reiny compress <launch.yaml> --out <dir> [--launcher <name>] [--include-system]`.
pub(crate) fn compress(
    config: &Path,
    out: &Path,
    launcher: Option<&str>,
    include_system: bool,
) -> Result<()> {
    let plan = LaunchPlan::from_launch_config(config)
        .with_context(|| format!("loading launch config {}", config.display()))?;
    plan.validate()?;

    let launcher_name = launcher.unwrap_or("reiny");
    std::fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;

    // 1. Bundle the launcher itself (this executable) as <out>/<launcher>(.exe).
    let exe = std::env::current_exe().context("resolving current_exe")?;
    let launcher_dst = out.join(format!("{launcher_name}{}", std::env::consts::EXE_SUFFIX));
    copy_file(&exe, &launcher_dst)?;
    println!(
        "  launcher  {} -> {}",
        exe.display(),
        launcher_dst.display()
    );

    // 2. The launch config as <out>/<launcher>.<ext> (a renamed launcher reads it from its own name;
    //    the extension travels with it so `.toml` stays TOML, and an extension-less file is the YAML it was).
    let ext = config
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("yaml");
    let cfg_dst = out.join(format!("{launcher_name}.{ext}"));
    copy_file(config, &cfg_dst)?;
    println!("  config    {} -> {}", config.display(), cfg_dst.display());

    // 3. Gather each launch bin, collecting the shared libraries they link.
    let cfg_dir = config_dir(config);
    let dirs = search_dirs(&cfg_dir, &plan, None, true);
    let mut libs: Vec<PathBuf> = Vec::new();
    for g in &plan.launches {
        let bin = find_bin(&dirs, &g.bin).ok_or_else(|| {
            anyhow::anyhow!("binary '{}' not found — run `reiny build` first", g.bin)
        })?;
        let dst = out.join(format!("{}{}", g.bin, std::env::consts::EXE_SUFFIX));
        copy_file(&bin, &dst)?;
        println!("  launch     {} -> {}", bin.display(), dst.display());
        collect_libs(&bin, include_system, &mut libs);
    }

    // 4. Only the shared libraries that are needed, into <out>/lib/.
    libs.sort();
    libs.dedup();
    if libs.is_empty() {
        println!("  libs      none (statically linked / system only)");
    } else {
        let lib_dir = out.join("lib");
        std::fs::create_dir_all(&lib_dir).context("creating lib/")?;
        for lib in &libs {
            if let Some(name) = lib.file_name() {
                let dst = lib_dir.join(name);
                copy_file(lib, &dst)?;
                println!("  lib       {} -> {}", lib.display(), dst.display());
            }
        }
    }

    println!(
        "\nbundled into {}. run it anywhere with:  cd {} && ./{}",
        out.display(),
        out.display(),
        launcher_name
    );
    Ok(())
}

/// Copy a file (its parent directory is expected to exist). `fs::copy` preserves the mode, executable bit included.
fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    std::fs::copy(src, dst)
        .with_context(|| format!("copying {} -> {}", src.display(), dst.display()))?;
    Ok(())
}

/// Collect the non-system shared libraries `ldd <bin>` resolves.
/// With `include_system`, the system ones too. Off Linux, or without ldd, it does nothing.
fn collect_libs(bin: &Path, include_system: bool, out: &mut Vec<PathBuf>) {
    let output = match std::process::Command::new("ldd").arg(bin).output() {
        Ok(o) if o.status.success() => o,
        _ => return, // no ldd, or it failed (non-Linux, say) — collecting libraries is best-effort.
    };
    let text = String::from_utf8_lossy(&output.stdout);
    for line in text.lines() {
        // The format is "libfoo.so => /path/to/libfoo.so (0x...)".
        let Some((_, rest)) = line.split_once("=>") else {
            continue;
        };
        let path = rest.split_whitespace().next().unwrap_or("");
        if path.is_empty() || path == "not" {
            continue; // "not found" and the like.
        }
        if !include_system && is_system_lib(path) {
            continue;
        }
        let p = PathBuf::from(path);
        if p.is_file() {
            out.push(p);
        }
    }
}

/// Whether it is a system library shipped with the OS (which stays out of the bundle).
fn is_system_lib(path: &str) -> bool {
    path.contains("ld-linux")
        || path.contains("linux-vdso")
        || path.starts_with("/lib/")
        || path.starts_with("/lib64/")
        || path.starts_with("/usr/lib/")
        || path.starts_with("/usr/lib64/")
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;

    /// What counts as a system library decides the bundle: keep too much and the artifact carries the
    /// host's libc around; keep too little and it does not start on the target.
    #[test]
    fn system_libraries_stay_out_of_the_bundle() {
        for system in [
            "/lib/x86_64-linux-gnu/libc.so.6",
            "/lib64/ld-linux-x86-64.so.2",
            "/usr/lib/x86_64-linux-gnu/libstdc++.so.6",
            "/usr/lib64/libm.so.6",
            "linux-vdso.so.1",
        ] {
            assert!(is_system_lib(system), "{system} should be a system library");
        }

        for ours in [
            "/home/nop/dev/robot/target/release/libmylib.so",
            "/opt/robot/lib/libdriver.so",
            "./libplugin.so",
        ] {
            assert!(!is_system_lib(ours), "{ours} has to be bundled");
        }

        // A path merely *containing* a system directory is not one: only the prefix counts.
        assert!(!is_system_lib("/home/nop/usr/lib/libmine.so"));
    }
}
