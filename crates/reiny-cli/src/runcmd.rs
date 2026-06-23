//! `reiny run` — launch config から grain 群を起動する。
//!
//! grain は独立プロジェクト(各自の `target/`)に分かれていることがあるので、launch config の
//! 場所を基準に複数の候補ディレクトリを組み立て、`reiny_launch::run_launch_dirs` に渡す。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use reiny_launch::{LaunchPlan, run_launch_dirs};

/// launch config の置かれたディレクトリ(絶対化。失敗時はそのまま)。
pub(crate) fn config_dir(config: &Path) -> PathBuf {
    let abs = std::fs::canonicalize(config).unwrap_or_else(|_| config.to_path_buf());
    abs.parent().map(Path::to_path_buf).unwrap_or_default()
}

/// 実行ファイルの隣(dist レイアウトでは grain bin がここに並ぶ)。
fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|e| e.parent().map(Path::to_path_buf))
}

/// bin を探す候補ディレクトリを組み立てる。優先順:
/// 1. 明示 `--bin-dir`、2. `<cfg>/<grain>/target/{debug,release}`(個別プロジェクト)、
/// 3. `<cfg>/target/{debug,release}`(共有ワークスペース)、4. 実行ファイルの隣(dist)。
pub(crate) fn search_dirs(
    cfg_dir: &Path,
    plan: &LaunchPlan,
    explicit: Option<PathBuf>,
    include_exe_dir: bool,
) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut push = |d: PathBuf| {
        if !dirs.contains(&d) {
            dirs.push(d);
        }
    };

    if let Some(d) = explicit {
        push(d);
    }
    // 各 grain の個別プロジェクト target(ディレクトリ名 = grain 名 = bin 名の前提)。
    for g in &plan.grains {
        push(cfg_dir.join(&g.name).join("target").join("debug"));
        push(cfg_dir.join(&g.name).join("target").join("release"));
    }
    // 共有ワークスペースの target。
    push(cfg_dir.join("target").join("debug"));
    push(cfg_dir.join("target").join("release"));
    if include_exe_dir && let Some(d) = exe_dir() {
        push(d);
    }
    dirs
}

/// 複数の候補ディレクトリから bin を探す(`reiny run` / `reiny compress` 共通)。
pub(crate) fn find_bin(dirs: &[PathBuf], bin: &str) -> Option<PathBuf> {
    dirs.iter()
        .map(|dir| dir.join(format!("{bin}{}", std::env::consts::EXE_SUFFIX)))
        .find(|p| p.exists())
}

/// `reiny run <launch.toml>`。`cd` した先からの相対 bin も拾えるよう launch config 基準で探す。
pub(crate) fn run(config: &Path, explicit_bin_dir: Option<PathBuf>, log_level: &str) -> Result<()> {
    let plan = LaunchPlan::from_launch_config(config)
        .with_context(|| format!("loading launch config {}", config.display()))?;
    let cfg_dir = config_dir(config);
    let dirs = search_dirs(&cfg_dir, &plan, explicit_bin_dir, true);
    run_launch_dirs(&plan, &dirs, Some(log_level))
}

/// リネームされたランチャ(`./ping-pong`)が自分の隣の `<name>.toml` を引数なしで起動する経路。
/// bin はランチャの隣(dist)にも、launch config 基準のプロジェクト target にも置けるよう探す。
pub(crate) fn run_self(config: &Path) -> Result<()> {
    run(config, None, "info")
}
