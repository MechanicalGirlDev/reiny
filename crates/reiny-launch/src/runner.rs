//! The runner that starts and monitors the launches' child processes according to a launch plan.
//!
//! Every launch is started from the same workspace's target directory (next to the launcher's own
//! executable). There is no cross-workspace plugin search as in hs-launch.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;

use crate::config::OnExit;
use crate::launch::{LaunchPlan, ResolvedLaunch};

/// How often the monitor loop wakes up (to notice an exit, a due respawn or Ctrl+C).
const POLL: Duration = Duration::from_millis(100);
/// How often [`wait_for_deps`] re-asks whether a dependency is up.
const READY_POLL: Duration = Duration::from_millis(50);
/// The delay before the first respawn.
const BACKOFF_MIN: Duration = Duration::from_millis(200);
/// The ceiling the respawn delay doubles up to.
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A child that stayed up this long counts as healthy: the next respawn starts the ladder over.
const BACKOFF_RESET: Duration = Duration::from_mins(1);

/// How a caller answers "is this launch up?", plus how long to wait for it.
///
/// The launcher owns the *policy* (when to wait, how often to ask, how to give up); the caller owns
/// the *bus* — `reiny run` implements `is_live` as a zenoh liveliness query on
/// `reiny/<domain>/<name>/@launch`, which is why `reiny-launch` itself still links no bus at all.
/// `None` at the call site keeps 0.5's behaviour: spawn in dependency order and wait for nothing.
pub struct Ready<'a> {
    /// Whether this launch is live on the bus **right now** (asked repeatedly, so keep it cheap).
    pub is_live: &'a dyn Fn(&ResolvedLaunch) -> bool,
    /// How long to wait for one dependency before starting its dependent anyway.
    pub timeout: Duration,
}

/// Per-launch respawn bookkeeping.
struct Respawn {
    /// The delay used for the previous respawn (`None` = it has not been respawned yet).
    delay: Option<Duration>,
    /// When the child currently running (or the one that just exited) was spawned.
    started: Instant,
}

/// The delay before the next respawn. `uptime` is how long the process that just exited stayed up:
/// one that lived through [`BACKOFF_RESET`] counts as healthy, so the ladder starts over instead of
/// punishing it for an earlier bad patch.
fn next_backoff(previous: Option<Duration>, uptime: Duration) -> Duration {
    match previous {
        Some(d) if uptime < BACKOFF_RESET => (d * 2).min(BACKOFF_MAX),
        _ => BACKOFF_MIN,
    }
}

/// Resolve a `bin` name to an executable path inside the given directory, adding the platform's
/// executable extension (`.exe` on Windows).
fn resolve_bin(bin_dir: &Path, bin: &str) -> PathBuf {
    bin_dir.join(format!("{bin}{}", std::env::consts::EXE_SUFFIX))
}

/// Look for `bin` across several search directories and return the first executable path found.
/// Launches are sometimes separate projects with a `target/` each, so this walks a list of candidates
/// rather than a single directory.
fn find_bin(bin_dirs: &[PathBuf], bin: &str) -> Option<PathBuf> {
    bin_dirs
        .iter()
        .map(|dir| resolve_bin(dir, bin))
        .find(|path| path.exists())
}

/// The directory holding the launcher's own executable (the same target profile).
fn default_bin_dir() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe().context("failed to resolve current_exe")?;
    exe.parent()
        .map(Path::to_path_buf)
        .context("current_exe has no parent dir")
}

/// Start one launch as a child process, resolving its bin by walking `bin_dirs` in order.
fn spawn_one(
    bin_dirs: &[PathBuf],
    spec: &ResolvedLaunch,
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

/// Stop the remaining child processes: ask them to exit, then kill after a grace period.
/// (Windows has little in the way of graceful inter-process signalling, so the last resort is
///  `Child::kill` = `TerminateProcess`.)
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

/// Wait until every launch `spec` depends on is up, one at a time.
///
/// A dependency that does not appear within `ready.timeout` is warned about and started past: it may
/// simply be slow, and refusing to bring the system up leaves a human nothing to do but run it again.
/// `live` remembers the ones already confirmed, so a launch several others depend on is asked about once.
fn wait_for_deps(
    plan: &LaunchPlan,
    spec: &ResolvedLaunch,
    ready: &Ready<'_>,
    stop: &AtomicBool,
    live: &mut HashSet<String>,
) {
    for dep in &spec.depends_on {
        if live.contains(dep) {
            continue;
        }
        // `validate()` has already rejected an undefined dependency; skip defensively.
        let Some(dep_spec) = plan.launches.iter().find(|l| &l.name == dep) else {
            continue;
        };
        let started = Instant::now();
        loop {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            if (ready.is_live)(dep_spec) {
                live.insert(dep.clone());
                tracing::debug!("'{}' is up after {:?}", dep, started.elapsed());
                break;
            }
            if started.elapsed() >= ready.timeout {
                tracing::warn!(
                    "'{}' did not appear on the bus within {:?}; starting '{}' anyway \
                     (--ready-timeout 0 skips this wait)",
                    dep,
                    ready.timeout,
                    spec.name
                );
                break;
            }
            std::thread::sleep(READY_POLL);
        }
    }
}

/// The dependency-ordered start plus the monitor loop. It accumulates the started children in
/// `children`, so the caller can still shut them down when this returns an `Err`.
fn run_inner(
    plan: &LaunchPlan,
    bin_dirs: &[PathBuf],
    default_log_level: Option<&str>,
    ready: Option<&Ready<'_>>,
    stop: &Arc<AtomicBool>,
    children: &mut HashMap<String, Child>,
) -> anyhow::Result<()> {
    let order = plan.topo_order()?;
    // Respawn bookkeeping per launch name.
    let mut respawns: HashMap<String, Respawn> = HashMap::new();
    // Launches confirmed up, so a shared dependency is asked about only once.
    let mut live: HashSet<String> = HashSet::new();
    // Launches waiting out their backoff, with the instant they may be respawned.
    let mut pending: Vec<(String, Instant)> = Vec::new();

    // Start sequentially in dependency order (name → Child). `depends_on` waits for the dependency to
    // **come up**, not merely to be spawned (when a `ready` is given).
    for &i in &order {
        let spec = &plan.launches[i];
        if let Some(ready) = ready {
            wait_for_deps(plan, spec, ready, stop, &mut live);
        }
        if stop.load(Ordering::SeqCst) {
            return Ok(());
        }
        tracing::info!("starting '{}' (bin={})", spec.name, spec.bin);
        let child = spawn_one(bin_dirs, spec, default_log_level)?;
        respawns.insert(
            spec.name.clone(),
            Respawn {
                delay: None,
                started: Instant::now(),
            },
        );
        children.insert(spec.name.clone(), child);
    }

    // The monitor loop.
    loop {
        if stop.load(Ordering::SeqCst) {
            tracing::info!("Ctrl+C received; stopping all launches");
            break;
        }

        // Respawns that are due. Held as a deadline rather than `sleep(backoff)` — sleeping 30 seconds
        // would leave Ctrl+C unanswered for that long.
        let now = Instant::now();
        let mut due: Vec<String> = Vec::new();
        pending.retain(|(name, at)| {
            let ripe = *at <= now;
            if ripe {
                due.push(name.clone());
            }
            !ripe
        });
        for name in due {
            let Some(spec) = plan.launches.iter().find(|g| g.name == name) else {
                continue;
            };
            match spawn_one(bin_dirs, spec, default_log_level) {
                Ok(child) => {
                    tracing::info!("respawned '{}'", name);
                    if let Some(r) = respawns.get_mut(&name) {
                        r.started = Instant::now();
                    }
                    children.insert(name, child);
                }
                // A failed respawn gives up on that launch; the others are still monitored.
                Err(e) => {
                    tracing::error!("failed to respawn '{}': {:#}; dropping launch", name, e);
                    respawns.remove(&name);
                }
            }
        }

        // Find the children that exited.
        let mut exited: Option<(String, std::process::ExitStatus)> = None;
        for (name, child) in children.iter_mut() {
            if let Some(status) = child.try_wait()? {
                exited = Some((name.clone(), status));
                break;
            }
        }
        let Some((name, status)) = exited else {
            if children.is_empty() && pending.is_empty() {
                break;
            }
            std::thread::sleep(POLL);
            continue;
        };
        children.remove(&name);
        live.remove(&name);

        // The name comes from a `children` key, so it is always in the plan; skip it if not, to be safe.
        let Some(spec) = plan.launches.iter().find(|g| g.name == name) else {
            continue;
        };
        tracing::warn!("launch '{}' exited with {:?}", name, status);
        match spec.on_exit {
            OnExit::Ignore => {
                respawns.remove(&name);
            }
            OnExit::Respawn => {
                let entry = respawns.entry(name.clone()).or_insert(Respawn {
                    delay: None,
                    started: Instant::now(),
                });
                let delay = next_backoff(entry.delay, entry.started.elapsed());
                entry.delay = Some(delay);
                tracing::info!("respawning '{}' in {:?}", name, delay);
                pending.push((name, Instant::now() + delay));
            }
            OnExit::ShutdownAll => {
                tracing::info!("'{}' triggered shutdown_all", name);
                break;
            }
        }
        if children.is_empty() && pending.is_empty() {
            break;
        }
    }

    Ok(())
}

/// Run a launch plan. With `bin_dir` as `None`, the directory next to `current_exe` is used.
/// `default_log_level` is the log level given to children with no per-launch override (normally the launcher's own).
pub fn run_launch(
    plan: &LaunchPlan,
    bin_dir: Option<PathBuf>,
    default_log_level: Option<&str>,
) -> anyhow::Result<()> {
    let bin_dir = match bin_dir {
        Some(d) => d,
        None => default_bin_dir()?,
    };
    run_launch_dirs(plan, &[bin_dir], default_log_level, None)
}

/// The multi-search-directory form of `run_launch`. Each launch bin is looked for in `bin_dirs` order.
/// For launches split across separate projects with a `target/` each (those generated individually by
/// `reiny new`, say), it can walk `<launch>/<launch>/target/<profile>` and friends in turn.
///
/// `ready` decides what `depends_on` means: `Some` waits for each dependency to appear on the bus
/// before starting its dependent, `None` only orders the `spawn` calls (0.5's behaviour).
pub fn run_launch_dirs(
    plan: &LaunchPlan,
    bin_dirs: &[PathBuf],
    default_log_level: Option<&str>,
    ready: Option<Ready<'_>>,
) -> anyhow::Result<()> {
    plan.validate()?;

    // The Ctrl+C flag.
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        let _ = ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst));
    }

    // A zero timeout is "do not wait" — treat it as `None` so it cannot warn about every dependency.
    let ready = ready.filter(|r| !r.timeout.is_zero());
    let mut children: HashMap<String, Child> = HashMap::new();
    let result = run_inner(
        plan,
        bin_dirs,
        default_log_level,
        ready.as_ref(),
        &stop,
        &mut children,
    );
    // Whether it ended well or badly, make sure no child is left running.
    shutdown_children(children);
    result
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::launch::LaunchPlan;

    #[test]
    fn resolve_bin_adds_exe_suffix() {
        let dir = Path::new("/tmp/target/debug");
        let got = resolve_bin(dir, "reiny-monitor");
        let want = dir.join(format!("reiny-monitor{}", std::env::consts::EXE_SUFFIX));
        assert_eq!(got, want);
    }

    #[test]
    fn backoff_doubles_to_a_ceiling_and_resets_after_a_healthy_run() {
        let crashing = Duration::from_secs(1);
        assert_eq!(next_backoff(None, crashing), BACKOFF_MIN);
        let mut d = BACKOFF_MIN;
        for _ in 0..10 {
            d = next_backoff(Some(d), crashing);
        }
        assert_eq!(d, BACKOFF_MAX, "the ladder must stop at the ceiling");
        // A child that stayed up past the reset window starts over, however bad its history was.
        assert_eq!(next_backoff(Some(BACKOFF_MAX), BACKOFF_RESET), BACKOFF_MIN);
    }

    fn plan_of(toml: &str) -> LaunchPlan {
        let config: crate::config::LaunchConfig = toml::from_str(toml).expect("launch config");
        let plan = LaunchPlan::from_config(&config, Path::new("launch.toml"));
        plan.validate().expect("valid plan");
        plan
    }

    #[test]
    fn wait_for_deps_returns_once_the_dependency_is_live() {
        let plan = plan_of("[launch]\na = {}\nb = { depends_on = [\"a\"] }\n");
        let b = plan.launches.iter().find(|l| l.name == "b").expect("b");
        // Not live for the first two questions, then up.
        let asked = RefCell::new(0);
        let is_live = |_: &ResolvedLaunch| {
            *asked.borrow_mut() += 1;
            *asked.borrow() > 2
        };
        let ready = Ready {
            is_live: &is_live,
            timeout: Duration::from_secs(5),
        };
        let mut live = HashSet::new();
        wait_for_deps(&plan, b, &ready, &AtomicBool::new(false), &mut live);
        assert_eq!(*asked.borrow(), 3);
        assert!(live.contains("a"), "a confirmed up is remembered");

        // Remembered: a second dependent does not ask again.
        wait_for_deps(&plan, b, &ready, &AtomicBool::new(false), &mut live);
        assert_eq!(*asked.borrow(), 3);
    }

    #[test]
    fn wait_for_deps_gives_up_after_the_timeout() {
        let plan = plan_of("[launch]\na = {}\nb = { depends_on = [\"a\"] }\n");
        let b = plan.launches.iter().find(|l| l.name == "b").expect("b");
        let never = |_: &ResolvedLaunch| false;
        let ready = Ready {
            is_live: &never,
            timeout: Duration::from_millis(120),
        };
        let mut live = HashSet::new();
        let started = Instant::now();
        wait_for_deps(&plan, b, &ready, &AtomicBool::new(false), &mut live);
        assert!(started.elapsed() >= Duration::from_millis(120), "waited");
        assert!(started.elapsed() < Duration::from_secs(3), "but gave up");
        assert!(live.is_empty(), "a launch that never showed is not 'live'");
    }

    #[test]
    fn wait_for_deps_bails_out_on_ctrl_c() {
        let plan = plan_of("[launch]\na = {}\nb = { depends_on = [\"a\"] }\n");
        let b = plan.launches.iter().find(|l| l.name == "b").expect("b");
        let never = |_: &ResolvedLaunch| false;
        let ready = Ready {
            is_live: &never,
            timeout: Duration::from_mins(1),
        };
        let started = Instant::now();
        wait_for_deps(
            &plan,
            b,
            &ready,
            &AtomicBool::new(true),
            &mut HashSet::new(),
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "returned at once"
        );
    }

    /// A throwaway directory tree. `find_bin` looks at the filesystem, so these tests need one.
    struct Fixture(PathBuf);

    impl Fixture {
        fn new(name: &str) -> Self {
            use std::sync::atomic::AtomicU32;
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "reiny-launch-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        /// Create `<dir>/<bin><EXE_SUFFIX>` and return the directory.
        fn with_bin(&self, dir: &str, bin: &str) -> PathBuf {
            let d = self.0.join(dir);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(resolve_bin(&d, bin), b"").unwrap();
            d
        }

        fn dir(&self, dir: &str) -> PathBuf {
            let d = self.0.join(dir);
            std::fs::create_dir_all(&d).unwrap();
            d
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The search order is the caller's order, first hit wins. That is what lets `reiny run` prefer a
    /// launch's own `target/` over the launcher's neighbours when a name exists in both.
    #[test]
    fn find_bin_takes_the_first_directory_that_has_it() {
        let f = Fixture::new("search");
        let empty = f.dir("empty");
        let first = f.with_bin("first", "talker");
        let second = f.with_bin("second", "talker");

        let dirs = vec![empty.clone(), first.clone(), second.clone()];
        assert_eq!(
            find_bin(&dirs, "talker"),
            Some(resolve_bin(&first, "talker"))
        );

        // Reversing the candidates picks the other one — the order really is the caller's.
        let dirs = vec![second.clone(), first];
        assert_eq!(
            find_bin(&dirs, "talker"),
            Some(resolve_bin(&second, "talker"))
        );

        // A name nothing has is `None`, which is what turns into the "build first" error.
        assert_eq!(find_bin(&dirs, "listener"), None);
        // So is an empty search list.
        assert_eq!(find_bin(&[], "talker"), None);
    }

    /// The backoff ladder's ceiling is reached by doubling, and it stays there. (The first step and
    /// the healthy-uptime reset are covered above; this pins the ceiling itself, which is what keeps
    /// a launch that can never start from respawning forever at an ever-growing delay.)
    #[test]
    fn backoff_saturates_at_the_ceiling() {
        let crashing = Duration::from_millis(1);
        let mut delay = next_backoff(None, crashing);
        assert_eq!(delay, BACKOFF_MIN);
        for _ in 0..20 {
            delay = next_backoff(Some(delay), crashing);
            assert!(delay <= BACKOFF_MAX, "{delay:?} passed the ceiling");
        }
        assert_eq!(delay, BACKOFF_MAX);
        assert_eq!(next_backoff(Some(BACKOFF_MAX), crashing), BACKOFF_MAX);
    }
}
