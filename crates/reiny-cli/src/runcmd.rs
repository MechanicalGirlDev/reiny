//! `reiny run` — start a set of launches from a launch config.
//!
//! Launches are sometimes separate projects with a `target/` each, so this assembles several candidate
//! directories from the launch config's location and hands them to `reiny_launch::run_launch_dirs`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use reiny::zenoh::{self, Wait};
use reiny::{DEFAULT_DOMAIN, DOMAIN_ENV, RuntimeOptions};
use reiny_launch::{LaunchPlan, Ready, ResolvedLaunch, run_launch_dirs};

use crate::bus::{KEY_ROOT, LAUNCH_CHUNK, alive_keys};

/// How long `reiny run` waits for one `depends_on` dependency to show up on the bus.
pub(crate) const DEFAULT_READY_TIMEOUT: f64 = 10.0;

/// The directory holding the launch config (made absolute; kept as-is on failure).
pub(crate) fn config_dir(config: &Path) -> PathBuf {
    let abs = std::fs::canonicalize(config).unwrap_or_else(|_| config.to_path_buf());
    abs.parent().map(Path::to_path_buf).unwrap_or_default()
}

/// Next to the executable (in a dist layout the launch bins sit here).
fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|e| e.parent().map(Path::to_path_buf))
}

/// Assemble the candidate directories to look for bins in, in priority order:
/// 1. an explicit `--bin-dir`, 2. `<cfg>/<launch>/target/{debug,release}` (separate projects),
/// 3. `<cfg>/target/{debug,release}` (a shared workspace), 4. next to the executable (dist).
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
    // Each launch's own project target (assuming directory name = launch name = bin name).
    for g in &plan.launches {
        push(cfg_dir.join(&g.name).join("target").join("debug"));
        push(cfg_dir.join(&g.name).join("target").join("release"));
    }
    // The shared workspace's target.
    push(cfg_dir.join("target").join("debug"));
    push(cfg_dir.join("target").join("release"));
    if include_exe_dir && let Some(d) = exe_dir() {
        push(d);
    }
    dirs
}

/// Look for a bin across several candidate directories (shared by `reiny run` and `reiny compress`).
pub(crate) fn find_bin(dirs: &[PathBuf], bin: &str) -> Option<PathBuf> {
    dirs.iter()
        .map(|dir| dir.join(format!("{bin}{}", std::env::consts::EXE_SUFFIX)))
        .find(|p| p.exists())
}

/// A launch's namespace, resolved the same way the child itself resolves it.
fn domain_of(spec: &ResolvedLaunch) -> String {
    spec.domain
        .clone()
        .unwrap_or_else(|| std::env::var(DOMAIN_ENV).unwrap_or_else(|_| DEFAULT_DOMAIN.to_string()))
}

/// The session `reiny run` uses to ask whether a dependency is up, or `None` when nothing depends on
/// anything (no point paying for a session) or when it cannot be opened.
///
/// A launcher that refuses to start because it could not open a *diagnostic* session would be worse
/// than one that starts without the wait, so a failure here is a warning, not an error.
fn ready_session(plan: &LaunchPlan) -> Option<zenoh::Session> {
    if plan.launches.iter().all(|l| l.depends_on.is_empty()) {
        return None;
    }
    let config = RuntimeOptions::new("reiny-run").zenoh_config();
    match config.and_then(|c| zenoh::open(c).wait().map_err(anyhow::Error::msg)) {
        Ok(session) => Some(session),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "cannot open a zenoh session to wait on depends_on; starting without the wait"
            );
            None
        }
    }
}

/// `reiny run <launch.toml>`. It searches relative to the launch config, so a bin relative to wherever you `cd`ed is still found.
///
/// `ready_timeout` seconds is how long each `depends_on` dependency is waited for (`0` = start
/// everything at once, which is what 0.5 did).
pub(crate) fn run(
    config: &Path,
    explicit_bin_dir: Option<PathBuf>,
    log_level: &str,
    ready_timeout: f64,
) -> Result<()> {
    let plan = LaunchPlan::from_launch_config(config)
        .with_context(|| format!("loading launch config {}", config.display()))?;
    let cfg_dir = config_dir(config);
    log_topic_flow(&cfg_dir, &plan);
    let dirs = search_dirs(&cfg_dir, &plan, explicit_bin_dir, true);

    let timeout = Duration::try_from_secs_f64(ready_timeout.max(0.0)).unwrap_or_default();
    let session = (!timeout.is_zero()).then(|| ready_session(&plan)).flatten();
    let Some(session) = session else {
        return run_launch_dirs(&plan, &dirs, Some(log_level), None);
    };
    // `@launch` goes up in `Cloudy::new`, before user code: it means "the process is on the bus",
    // which is exactly as much as a launcher can promise. Type-level readiness is the launch's own
    // job (`publishers` / `subscribers` / `servers`).
    let is_live = |spec: &ResolvedLaunch| {
        let key = format!(
            "{KEY_ROOT}/{}/{}/{LAUNCH_CHUNK}",
            domain_of(spec),
            spec.name
        );
        alive_keys(&session, &key).is_ok_and(|keys| !keys.is_empty())
    };
    run_launch_dirs(
        &plan,
        &dirs,
        Some(log_level),
        Some(Ready {
            is_live: &is_live,
            timeout,
        }),
    )
}

/// The path a renamed launcher (`./ping-pong`) takes to start its neighbouring `<name>.toml` with no
/// arguments. Bins are looked for next to the launcher (dist) as well as in the project targets around the launch config.
pub(crate) fn run_self(config: &Path) -> Result<()> {
    run(config, None, "info", DEFAULT_READY_TIMEOUT)
}

// ---------------------------------------------------------------------------
// The topic flow diagram (in the startup log)
// ---------------------------------------------------------------------------

/// One launch's pub/sub declarations (the values are topic type segments).
struct LaunchFlow {
    name: String,
    pubs: BTreeSet<String>,
    subs: BTreeSet<String>,
}

/// Resolve each launch's Reiny.toml and collect its declarations (publications / dependencies) and
/// its `[services]` request type → response alias. A launch whose manifest cannot be found is skipped
/// silently (this diagram is best-effort and must not get in the way of starting, in a dist layout say).
fn collect_flows(cfg_dir: &Path, plan: &LaunchPlan) -> (Vec<LaunchFlow>, BTreeMap<String, String>) {
    let mut flows = Vec::new();
    let mut services = BTreeMap::new();
    for g in &plan.launches {
        // Prefer the launch's own project (<cfg>/<launch>); failing that, search upward from the launch config.
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
            // per-project: publications = what it publishes, dependencies::* = what it can subscribe to.
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
            // workspace: look the `[projects.<bin>]` declarations (aliases) up in the catalogue for their segments.
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

/// Add an edge for the type `t` from `by` (creating it, and copying the `[services]` reply, if new).
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

/// Fold the declarations into one edge per type (columns = indices into `flows`). A `[services]`
/// request type has its publish / subscribe roles swapped, so the arrow runs caller → server.
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
    // Row order: sender's column → receiver's column → type name, so the flow reads top to bottom.
    edges.sort_by(|a, b| {
        (a.pubs.first(), a.subs.first(), &a.ty).cmp(&(b.pubs.first(), b.subs.first(), &b.ty))
    });
    edges
}

/// Print the topic flow diagram of the launches being started into the startup log. With no
/// declarations resolved at all, nothing is printed. It is many lines, so it goes to plain stdout rather than through tracing (it is a banner).
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
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
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
        // In a per-project layout (an in-repo example), publications / dependencies::* reach the diagram.
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
        // In the in-repo example workspace, the `[projects.*]` declarations reach the diagram.
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
        // The serving side declares it under publications and the caller under dependencies, but the arrow runs caller → server.
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
