//! `reiny compress` — 動かすのに要るものだけを 1 ディレクトリに束ねる(ランチャ込みで完結)。
//!
//! launch config を辿り、到達可能な grain bin・実際にリンクしている共有ライブラリ・launch config・
//! **ランチャ reiny 本体**を `<out>/` に集める。`--launcher <name>` で reiny を `<name>` に
//! リネームし、launch config も `<name>.toml` に揃えると、`./<name>` だけで起動できる。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use reiny_launch::LaunchPlan;

use crate::runcmd::{config_dir, find_bin, search_dirs};

/// `reiny compress <launch.toml> --out <dir> [--launcher <name>] [--include-system]`。
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

    // 1. ランチャ本体(自分自身)を <out>/<launcher>(.exe) に同梱。
    let exe = std::env::current_exe().context("resolving current_exe")?;
    let launcher_dst = out.join(format!("{launcher_name}{}", std::env::consts::EXE_SUFFIX));
    copy_file(&exe, &launcher_dst)?;
    println!(
        "  launcher  {} -> {}",
        exe.display(),
        launcher_dst.display()
    );

    // 2. launch config を <out>/<launcher>.toml に(リネームしたランチャが自分の名前から読む)。
    let cfg_dst = out.join(format!("{launcher_name}.toml"));
    copy_file(config, &cfg_dst)?;
    println!("  config    {} -> {}", config.display(), cfg_dst.display());

    // 3. 各 grain bin を集めつつ、リンクしている共有ライブラリを収集する。
    let cfg_dir = config_dir(config);
    let dirs = search_dirs(&cfg_dir, &plan, None, true);
    let mut libs: Vec<PathBuf> = Vec::new();
    for g in &plan.grains {
        let bin = find_bin(&dirs, &g.bin).ok_or_else(|| {
            anyhow::anyhow!("binary '{}' not found — run `reiny build` first", g.bin)
        })?;
        let dst = out.join(format!("{}{}", g.bin, std::env::consts::EXE_SUFFIX));
        copy_file(&bin, &dst)?;
        println!("  grain     {} -> {}", bin.display(), dst.display());
        collect_libs(&bin, include_system, &mut libs);
    }

    // 4. 必要な共有ライブラリだけ <out>/lib/ へ。
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

/// ファイルをコピーする(親ディレクトリは作成済み前提)。実行権限など mode は `fs::copy` が保つ。
fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    std::fs::copy(src, dst)
        .with_context(|| format!("copying {} -> {}", src.display(), dst.display()))?;
    Ok(())
}

/// `ldd <bin>` で解決した共有ライブラリのうち、システム外のものを集める。
/// `include_system` ならシステムライブラリも含める。Linux 以外や ldd 不在では何もしない。
fn collect_libs(bin: &Path, include_system: bool, out: &mut Vec<PathBuf>) {
    let output = match std::process::Command::new("ldd").arg(bin).output() {
        Ok(o) if o.status.success() => o,
        _ => return, // ldd が無い / 失敗(非 Linux 等)— 共有ライブラリ収集は best-effort。
    };
    let text = String::from_utf8_lossy(&output.stdout);
    for line in text.lines() {
        // 形式: "libfoo.so => /path/to/libfoo.so (0x...)"。
        let Some((_, rest)) = line.split_once("=>") else {
            continue;
        };
        let path = rest.split_whitespace().next().unwrap_or("");
        if path.is_empty() || path == "not" {
            continue; // "not found" 等。
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

/// OS 同梱のシステムライブラリ(配布物には入れない)か。
fn is_system_lib(path: &str) -> bool {
    path.contains("ld-linux")
        || path.contains("linux-vdso")
        || path.starts_with("/lib/")
        || path.starts_with("/lib64/")
        || path.starts_with("/usr/lib/")
        || path.starts_with("/usr/lib64/")
}
