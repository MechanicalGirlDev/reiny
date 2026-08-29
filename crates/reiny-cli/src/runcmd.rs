//! `reiny run` — launch config から launch 群を起動する。
//!
//! launch は独立プロジェクト(各自の `target/`)に分かれていることがあるので、launch config の
//! 場所を基準に複数の候補ディレクトリを組み立て、`reiny_launch::run_launch_dirs` に渡す。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use reiny_launch::{LaunchPlan, run_launch_dirs};

/// launch config の置かれたディレクトリ(絶対化。失敗時はそのまま)。
pub(crate) fn config_dir(config: &Path) -> PathBuf {
    let abs = std::fs::canonicalize(config).unwrap_or_else(|_| config.to_path_buf());
    abs.parent().map(Path::to_path_buf).unwrap_or_default()
}

/// 実行ファイルの隣(dist レイアウトでは launch bin がここに並ぶ)。
fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|e| e.parent().map(Path::to_path_buf))
}

/// bin を探す候補ディレクトリを組み立てる。優先順:
/// 1. 明示 `--bin-dir`、2. `<cfg>/<launch>/target/{debug,release}`(個別プロジェクト)、
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
    // 各 launch の個別プロジェクト target(ディレクトリ名 = launch 名 = bin 名の前提)。
    for g in &plan.launches {
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
    log_topic_flow(&cfg_dir, &plan);
    let dirs = search_dirs(&cfg_dir, &plan, explicit_bin_dir, true);
    run_launch_dirs(&plan, &dirs, Some(log_level))
}

/// リネームされたランチャ(`./ping-pong`)が自分の隣の `<name>.toml` を引数なしで起動する経路。
/// bin はランチャの隣(dist)にも、launch config 基準のプロジェクト target にも置けるよう探す。
pub(crate) fn run_self(config: &Path) -> Result<()> {
    run(config, None, "info")
}

// ---------------------------------------------------------------------------
// トピックの流れ図(起動ログ)
// ---------------------------------------------------------------------------

/// 1 launch の pub/sub 宣言(値はトピックの型セグメント)。
struct LaunchFlow {
    name: String,
    pubs: BTreeSet<String>,
    subs: BTreeSet<String>,
}

/// 各 launch の Reiny.toml を解決し、宣言(publications / dependencies)と `[services]` の
/// request 型 → response 別名を集める。マニフェストの見つからない launch は黙って飛ばす
/// (dist 配置などでも起動を妨げないため、この図は best-effort)。
fn collect_flows(cfg_dir: &Path, plan: &LaunchPlan) -> (Vec<LaunchFlow>, BTreeMap<String, String>) {
    let mut flows = Vec::new();
    let mut services = BTreeMap::new();
    for g in &plan.launches {
        // 個別プロジェクト(<cfg>/<launch>)を優先し、無ければ launch config の場所から上方探索。
        let dir = [cfg_dir.join(&g.name), cfg_dir.join(&g.bin)]
            .into_iter()
            .find(|d| d.is_dir())
            .unwrap_or_else(|| cfg_dir.to_path_buf());
        let Ok(res) = reiny_build::describe(&dir) else {
            continue;
        };
        let types = res.types();
        let segment: BTreeMap<&str, &str> = types
            .iter()
            .map(|t| (t.alias.as_str(), t.topic_segment.as_str()))
            .collect();
        let (pubs, subs) = if matches!(res.mode(), reiny_build::Mode::PerProject) {
            // per-project: publications = 公開、dependencies::* = 購読できる型。
            let pick = |f: fn(&str) -> bool| {
                types
                    .iter()
                    .filter(|t| f(&t.module))
                    .map(|t| t.topic_segment.clone())
                    .collect::<BTreeSet<_>>()
            };
            (
                pick(|m| m == "publications"),
                pick(|m| m.starts_with("dependencies::")),
            )
        } else {
            // workspace: [projects.<bin>] の宣言(別名)をカタログでセグメントへ引く。
            let Some(p) = res
                .projects()
                .iter()
                .find(|p| p.name == g.bin || p.name == g.name)
            else {
                continue;
            };
            let to_segments = |aliases: &[String]| {
                aliases
                    .iter()
                    .filter_map(|a| segment.get(a.as_str()).map(|s| (*s).to_string()))
                    .collect::<BTreeSet<_>>()
            };
            (to_segments(&p.publications), to_segments(&p.dependencies))
        };
        for s in res.services() {
            if let Some(seg) = segment.get(s.request.as_str()) {
                services.insert((*seg).to_string(), s.response.clone());
            }
        }
        flows.push(LaunchFlow {
            name: g.name.clone(),
            pubs,
            subs,
        });
    }
    (flows, services)
}

/// 集めた宣言から `publisher --[Type]--> subscriber` の行を組む。`[services]` の request 型は
/// 向きが逆(caller --> server)なので左右を入れ替え、reply の型を添える。
fn render_flow(flows: &[LaunchFlow], services: &BTreeMap<String, String>) -> Vec<String> {
    let mut by_type: BTreeMap<&str, (Vec<&str>, Vec<&str>)> = BTreeMap::new();
    for f in flows {
        for t in &f.pubs {
            by_type.entry(t).or_default().0.push(&f.name);
        }
        for t in &f.subs {
            by_type.entry(t).or_default().1.push(&f.name);
        }
    }

    let side = |names: &[&str]| {
        if names.is_empty() {
            "(none)".to_string()
        } else {
            names.join(", ")
        }
    };
    let rows: Vec<(String, &str, String, String)> = by_type
        .iter()
        .map(|(ty, (pubs, subs))| {
            if let Some(reply) = services.get(*ty) {
                // service: 購読宣言側が caller、公開宣言側が server。
                (
                    side(subs),
                    *ty,
                    side(pubs),
                    format!("  (service, reply {reply})"),
                )
            } else {
                (side(pubs), *ty, side(subs), String::new())
            }
        })
        .collect();

    let w_left = rows.iter().map(|r| r.0.chars().count()).max().unwrap_or(0);
    let w_ty = rows.iter().map(|r| r.1.len()).max().unwrap_or(0);
    rows.iter()
        .map(|(left, ty, right, note)| {
            let dashes = "-".repeat(w_ty - ty.len() + 2);
            format!("  {left:>w_left$} --[{ty}]{dashes}> {right}{note}")
        })
        .collect()
}

/// 起動する launch 群のトピックの流れを起動ログに出す。宣言を 1 つも解決できなければ何も出さない。
fn log_topic_flow(cfg_dir: &Path, plan: &LaunchPlan) {
    let (flows, services) = collect_flows(cfg_dir, plan);
    let lines = render_flow(&flows, &services);
    if lines.is_empty() {
        return;
    }
    tracing::info!("topic flow:");
    for line in &lines {
        tracing::info!("{line}");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;

    fn flow(name: &str, pubs: &[&str], subs: &[&str]) -> LaunchFlow {
        LaunchFlow {
            name: name.to_string(),
            pubs: pubs.iter().map(ToString::to_string).collect(),
            subs: subs.iter().map(ToString::to_string).collect(),
        }
    }

    #[test]
    fn render_aligns_arrows_and_flips_services() {
        let flows = [
            flow("ping", &["Ping"], &["Pong"]),
            flow("pong", &["Pong"], &["Ping"]),
            flow("calc", &["Add"], &[]),
            flow("asker", &[], &["Add"]),
        ];
        let services = BTreeMap::from([("Add".to_string(), "Sum".to_string())]);
        let lines = render_flow(&flows, &services);
        assert_eq!(
            lines,
            vec![
                "  asker --[Add]---> calc  (service, reply Sum)",
                "   ping --[Ping]--> pong",
                "   pong --[Pong]--> ping",
            ]
        );
    }

    #[test]
    fn render_marks_missing_sides() {
        let lines = render_flow(&[flow("a", &["T"], &[])], &BTreeMap::new());
        assert_eq!(lines, vec!["  a --[T]--> (none)"]);
    }

    #[test]
    fn collect_reads_per_project_declarations() {
        // per-project 配置(repo 内の例)では publications / dependencies::* が流れ図に写ること。
        let cfg_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/ping-pong-ring");
        let plan = LaunchPlan::from_launch_config(&cfg_dir.join("ping-pong.toml")).unwrap();
        let (flows, _services) = collect_flows(&cfg_dir, &plan);
        let ping = flows.iter().find(|f| f.name == "ping").expect("ping");
        assert!(
            ping.pubs.contains("Ping") && ping.subs.contains("Pong"),
            "{:?} {:?}",
            ping.pubs,
            ping.subs
        );
    }

    #[test]
    fn collect_reads_workspace_declarations() {
        // 例のワークスペース(repo 内)で、[projects.*] の宣言が流れ図に写ること。
        let cfg_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/ping-pong-workspace");
        let plan = LaunchPlan::from_launch_config(&cfg_dir.join("ping-pong.toml")).unwrap();
        let (flows, services) = collect_flows(&cfg_dir, &plan);
        assert!(services.is_empty());
        let ping = flows.iter().find(|f| f.name == "ping").expect("ping");
        assert!(ping.pubs.contains("Ping") && ping.subs.contains("Pong"));
        let lines = render_flow(&flows, &services);
        assert!(
            lines.iter().any(|l| l.contains("ping --[Ping]--> pong")),
            "{lines:?}"
        );
    }
}
