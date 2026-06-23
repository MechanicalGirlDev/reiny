//! launch plan に従い grain の子プロセスを起動・監視するランナー。
//!
//! grain は全て同一ワークスペースの target ディレクトリ(ランチャ実行ファイルの隣)から
//! 起動する。hs-launch のような別ワークスペース plugin 探索は持たない。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;

use crate::config::OnExit;
use crate::launch::{LaunchPlan, ResolvedGrain};

/// `bin` 名を、与えられたディレクトリ内の実行ファイルパスに解決する。
/// プラットフォームの実行ファイル拡張子(Windows は `.exe`)を付与する。
fn resolve_bin(bin_dir: &Path, bin: &str) -> PathBuf {
    bin_dir.join(format!("{bin}{}", std::env::consts::EXE_SUFFIX))
}

/// 複数の探索ディレクトリから `bin` を探し、最初に見つかった実行ファイルパスを返す。
/// grain は独立したプロジェクト(各自の `target/`)に置かれることがあるため、単一の
/// ディレクトリではなく候補列を順に当たる。
fn find_bin(bin_dirs: &[PathBuf], bin: &str) -> Option<PathBuf> {
    bin_dirs
        .iter()
        .map(|dir| resolve_bin(dir, bin))
        .find(|path| path.exists())
}

/// ランチャ実行ファイルと同じディレクトリ(同一 target プロファイル)を返す。
fn default_bin_dir() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe().context("failed to resolve current_exe")?;
    exe.parent()
        .map(Path::to_path_buf)
        .context("current_exe has no parent dir")
}

/// 1 grain を子プロセスとして起動する。`bin_dirs` を順に探して bin を解決する。
fn spawn_one(
    bin_dirs: &[PathBuf],
    spec: &ResolvedGrain,
    default_log_level: Option<&str>,
) -> anyhow::Result<Child> {
    let path = find_bin(bin_dirs, &spec.bin).ok_or_else(|| {
        let searched = bin_dirs
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::anyhow!(
            "binary '{}' not found in any of [{}] — build first (reiny build / cargo build)",
            spec.bin,
            searched
        )
    })?;
    let mut cmd = Command::new(&path);
    let log_level = spec.log_level.as_deref().or(default_log_level);
    if let Some(level) = log_level {
        cmd.arg("--log-level").arg(level);
    }
    cmd.arg("--name").arg(&spec.name);
    cmd.args(&spec.args);
    cmd.spawn()
        .with_context(|| format!("failed to spawn '{}'", spec.bin))
}

/// 残りの子プロセスを停止する。まず終了を促し、猶予後に kill。
/// (Windows では graceful なプロセス間シグナルが限定的なため、
///  最後は `Child::kill` = `TerminateProcess` にフォールバックする。)
fn shutdown_children(mut children: HashMap<String, Child>) {
    let deadline = Duration::from_secs(3);
    let start = std::time::Instant::now();
    while start.elapsed() < deadline
        && children
            .values_mut()
            .any(|c| matches!(c.try_wait(), Ok(None)))
    {
        std::thread::sleep(Duration::from_millis(50));
    }
    for (name, mut child) in children {
        if matches!(child.try_wait(), Ok(None)) {
            tracing::warn!("force-killing '{}'", name);
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// 依存順起動・監視ループ本体。`children` に起動済みの子を蓄積するため、
/// この関数が Err を返しても呼び出し元が `children` を元に shutdown できる。
fn run_inner(
    plan: &LaunchPlan,
    bin_dirs: &[PathBuf],
    default_log_level: Option<&str>,
    stop: &Arc<AtomicBool>,
    children: &mut HashMap<String, Child>,
) -> anyhow::Result<()> {
    let order = plan.topo_order()?;

    // 依存順に逐次起動(name → Child)。
    for &i in &order {
        let spec = &plan.grains[i];
        tracing::info!("starting '{}' (bin={})", spec.name, spec.bin);
        let child = spawn_one(bin_dirs, spec, default_log_level)?;
        children.insert(spec.name.clone(), child);
    }

    // 監視ループ。
    loop {
        if stop.load(Ordering::SeqCst) {
            tracing::info!("Ctrl+C received; stopping all grains");
            break;
        }
        // 終了した子を探す。
        let mut exited: Option<(String, std::process::ExitStatus)> = None;
        for (name, child) in children.iter_mut() {
            if let Some(status) = child.try_wait()? {
                exited = Some((name.clone(), status));
                break;
            }
        }
        let Some((name, status)) = exited else {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };

        // name は children のキー由来なので plan に必ず在るが、無ければ安全側で読み飛ばす。
        let Some(spec) = plan.grains.iter().find(|g| g.name == name) else {
            children.remove(&name);
            continue;
        };
        tracing::warn!("grain '{}' exited with {:?}", name, status);
        match spec.on_exit {
            OnExit::Ignore => {
                children.remove(&name);
                if children.is_empty() {
                    break;
                }
            }
            OnExit::Respawn => {
                tracing::info!("respawning '{}'", name);
                match spawn_one(bin_dirs, spec, default_log_level) {
                    Ok(child) => {
                        children.insert(name, child);
                    }
                    // respawn 失敗は当該 grain を諦め、他は監視継続。
                    Err(e) => {
                        tracing::error!("failed to respawn '{}': {:#}; dropping grain", name, e);
                        children.remove(&name);
                        if children.is_empty() {
                            break;
                        }
                    }
                }
            }
            OnExit::ShutdownAll => {
                tracing::info!("'{}' triggered shutdown_all", name);
                children.remove(&name);
                break;
            }
        }
    }

    Ok(())
}

/// launch plan を実行する。`bin_dir` が None なら `current_exe` の隣接ディレクトリ。
/// `default_log_level` は per-grain override の無い子に渡すログレベル(通常はランチャ自身)。
pub fn run_launch(
    plan: &LaunchPlan,
    bin_dir: Option<PathBuf>,
    default_log_level: Option<&str>,
) -> anyhow::Result<()> {
    let bin_dir = match bin_dir {
        Some(d) => d,
        None => default_bin_dir()?,
    };
    run_launch_dirs(plan, &[bin_dir], default_log_level)
}

/// `run_launch` の複数探索ディレクトリ版。各 grain bin を `bin_dirs` の順で探す。
/// grain が別々のプロジェクト(各自の `target/`)に分かれているとき(例: `reiny new` で
/// 個別生成した grain 群)に、`<launch>/<grain>/target/<profile>` などを順に当たれる。
pub fn run_launch_dirs(
    plan: &LaunchPlan,
    bin_dirs: &[PathBuf],
    default_log_level: Option<&str>,
) -> anyhow::Result<()> {
    plan.validate()?;

    // Ctrl+C フラグ。
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        let _ = ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst));
    }

    let mut children: HashMap<String, Child> = HashMap::new();
    let result = run_inner(plan, bin_dirs, default_log_level, &stop, &mut children);
    // 正常・異常いずれの場合も残存子を確実に停止する。
    shutdown_children(children);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_bin_adds_exe_suffix() {
        let dir = Path::new("/tmp/target/debug");
        let got = resolve_bin(dir, "reiny-monitor");
        let want = dir.join(format!("reiny-monitor{}", std::env::consts::EXE_SUFFIX));
        assert_eq!(got, want);
    }
}
