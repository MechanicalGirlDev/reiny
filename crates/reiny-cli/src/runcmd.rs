//! `reiny run` — launch config から launch 群を起動する。
//!
//! launch は独立プロジェクト(各自の `target/`)に分かれていることがあるので、launch config の
//! 場所を基準に複数の候補ディレクトリを組み立て、`reiny_launch::run_launch_dirs` に渡す。

use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
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

/// `by` から型 `t` の辺を引く(無ければ作って `[services]` の reply を写す)。
fn edge_for<'m, 'k>(
    by: &'m mut BTreeMap<&'k str, crate::flowart::Edge>,
    t: &'k str,
    services: &BTreeMap<String, String>,
) -> &'m mut crate::flowart::Edge {
    by.entry(t).or_insert_with(|| crate::flowart::Edge {
        ty: t.to_string(),
        pubs: Vec::new(),
        subs: Vec::new(),
        reply: services.get(t).cloned(),
    })
}

/// 宣言を型ごとの辺(列 = `flows` の添字)へ畳む。`[services]` の request 型は矢印を
/// 呼ぶ側 → serve する側の向きにしたいので、公開/購読の役割を入れ替える。
fn build_edges(
    flows: &[LaunchFlow],
    services: &BTreeMap<String, String>,
) -> Vec<crate::flowart::Edge> {
    let mut by: BTreeMap<&str, crate::flowart::Edge> = BTreeMap::new();
    for (i, f) in flows.iter().enumerate() {
        for t in &f.pubs {
            edge_for(&mut by, t, services).pubs.push(i);
        }
        for t in &f.subs {
            edge_for(&mut by, t, services).subs.push(i);
        }
    }
    let mut edges: Vec<_> = by.into_values().collect();
    for e in &mut edges {
        if e.reply.is_some() {
            std::mem::swap(&mut e.pubs, &mut e.subs);
        }
    }
    // 行順: 送り手の列 → 受け手の列 → 型名。上から下へ流れが読めるようにする。
    edges.sort_by(|a, b| {
        (a.pubs.first(), a.subs.first(), &a.ty).cmp(&(b.pubs.first(), b.subs.first(), &b.ty))
    });
    edges
}

/// 起動する launch 群のトピックの流れ図を起動ログに出す。宣言を 1 つも解決できなければ
/// 何も出さない。多行の図なので tracing ではなく素の stdout へ描く(バナー扱い)。
fn log_topic_flow(cfg_dir: &Path, plan: &LaunchPlan) {
    let (flows, services) = collect_flows(cfg_dir, plan);
    let names: Vec<String> = flows.iter().map(|f| f.name.clone()).collect();
    let edges = build_edges(&flows, &services);
    let color = std::env::var_os("NO_COLOR").is_none() && std::io::stdout().is_terminal();
    let lines = crate::flowart::render(&names, &edges, color);
    if lines.is_empty() {
        return;
    }
    println!();
    for line in &lines {
        println!("{line}");
    }
    println!();
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
        let edges = build_edges(&flows, &services);
        let e = edges.iter().find(|e| e.ty == "Ping").expect("Ping edge");
        let col = |n: &str| flows.iter().position(|f| f.name == n).unwrap();
        assert_eq!(
            (e.pubs.as_slice(), e.subs.as_slice()),
            ([col("ping")].as_slice(), [col("pong")].as_slice())
        );
    }

    #[test]
    fn build_edges_flips_service_direction() {
        // serve する側が publications、呼ぶ側が dependencies に宣言するが、矢印は caller → server。
        let flows = [flow("asker", &[], &["Add"]), flow("calc", &["Add"], &[])];
        let services = BTreeMap::from([("Add".to_string(), "Sum".to_string())]);
        let edges = build_edges(&flows, &services);
        assert_eq!(edges.len(), 1);
        assert_eq!(
            (edges[0].pubs.as_slice(), edges[0].subs.as_slice()),
            ([0].as_slice(), [1].as_slice())
        );
        assert_eq!(edges[0].reply.as_deref(), Some("Sum"));
    }
}
