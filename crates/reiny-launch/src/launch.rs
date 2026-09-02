//! Deriving a launch plan (which launches to start, and in what order) from a launch config's
//! `launch:` section (YAML; `.toml` reads TOML). `reiny run <launch>.yaml` is the only entry point.
//!
//! Unlike `HumanoidSystem`'s hs-launch there is **no known-kind / plugin distinction**. Every key is
//! an equal launch: key = instance name = default bin name. The bins are started from the same
//! workspace's target directory (there is no plugin search across workspaces).
//!
//! The default conventions (overridable through the inline table in `[launch]`):
//! - `bin` = the key name, `depends_on` = [], `on_exit` = ignore.
//! - Giving `config = "..."` adds `--config <abs>` to the startup arguments.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::{LaunchConfig, LaunchSpec, OnExit};

/// One resolved launch's startup specification (after conventions and overrides).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLaunch {
    /// The instance name (passed to the child as `--name`; unique within the plan). The launch config's key.
    pub name: String,
    /// The executable to start (unset = the key name).
    pub bin: String,
    /// The startup arguments (`--config <abs path>` and the like).
    pub args: Vec<String>,
    /// The dependencies that fix the start order (started, and waited for, before this one).
    pub depends_on: Vec<String>,
    /// What happens when the process exits.
    pub on_exit: OnExit,
    /// An override for the log level passed to this child (unset = the launcher's default).
    pub log_level: Option<String>,
    /// The namespace this launch runs in (its own `domain`, else the launch-wide default). `None`
    /// leaves it to the child's own resolution (`REINY_DOMAIN`, else `"default"`). It is already in
    /// `args` as `--domain <ns>`; the field exists so a readiness check can address the launch on the
    /// bus without parsing the argument list back.
    pub domain: Option<String>,
}

/// The launch plan derived from a launch config.
#[derive(Debug, Clone, Default)]
pub struct LaunchPlan {
    /// The launches with their order unresolved (after conventions and overrides). [`Self::topo_order`] decides the order.
    pub launches: Vec<ResolvedLaunch>,
}

/// A launch plan validation error.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LaunchError {
    /// Two or more launches share a name (key = instance name is unique within a plan).
    #[error("duplicate launch name '{0}'")]
    DuplicateName(String),
    /// A `depends_on` names a launch the plan does not have.
    #[error("launch '{launch}' depends_on undefined launch '{dep}'")]
    UndefinedDependency {
        /// The launch that declared the dependency.
        launch: String,
        /// The (undefined) launch it referred to.
        dep: String,
    },
    /// `depends_on` has a cycle.
    #[error("dependency cycle detected involving '{0}'")]
    Cycle(String),
}

impl LaunchPlan {
    /// Derive a launch plan from a launch config file (YAML; a `.toml` extension reads TOML).
    pub fn from_launch_config(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read launch config {}", path.display()))?;
        let config = parse_launch_config(path, &text)
            .with_context(|| format!("failed to parse launch config {}", path.display()))?;
        Ok(Self::from_config(&config, path))
    }

    /// Build a plan from an already-parsed `LaunchConfig`. Launch config paths resolve against the
    /// launch config's own directory, so they do not depend on the child's working directory.
    pub(crate) fn from_config(config: &LaunchConfig, launch_config_path: &Path) -> Self {
        // Make the launch config path absolute (it is expected to exist; on failure keep what was given).
        let abs = std::fs::canonicalize(launch_config_path)
            .unwrap_or_else(|_| launch_config_path.to_path_buf());
        let cfg_dir = abs.parent().map(Path::to_path_buf).unwrap_or_default();

        // A BTreeMap keeps the key order deterministic → so is the start order when nothing depends on anything.
        let launches = config
            .launch
            .iter()
            .filter(|(_, spec)| spec.enabled())
            .map(|(name, spec)| resolve(name, spec, &cfg_dir, config.domain.as_deref()))
            .collect();

        Self { launches }
    }

    /// Validate unique names, dependency references and cycles.
    pub fn validate(&self) -> Result<(), LaunchError> {
        let mut seen = std::collections::HashSet::new();
        for g in &self.launches {
            if !seen.insert(g.name.as_str()) {
                return Err(LaunchError::DuplicateName(g.name.clone()));
            }
        }
        for g in &self.launches {
            for dep in &g.depends_on {
                if !seen.contains(dep.as_str()) {
                    return Err(LaunchError::UndefinedDependency {
                        launch: g.name.clone(),
                        dep: dep.clone(),
                    });
                }
            }
        }
        self.topo_order().map(|_| ())
    }

    /// The start order (as indices) satisfying `depends_on`. `Cycle` when there is one.
    pub fn topo_order(&self) -> Result<Vec<usize>, LaunchError> {
        use std::collections::HashMap;

        fn visit(
            i: usize,
            launches: &[ResolvedLaunch],
            index: &std::collections::HashMap<&str, usize>,
            state: &mut [u8],
            order: &mut Vec<usize>,
        ) -> Result<(), LaunchError> {
            match state[i] {
                2 => return Ok(()),
                1 => return Err(LaunchError::Cycle(launches[i].name.clone())),
                _ => {}
            }
            state[i] = 1;
            for dep in &launches[i].depends_on {
                if let Some(&j) = index.get(dep.as_str()) {
                    visit(j, launches, index, state, order)?;
                }
            }
            state[i] = 2;
            order.push(i);
            Ok(())
        }

        let index: HashMap<&str, usize> = self
            .launches
            .iter()
            .enumerate()
            .map(|(i, g)| (g.name.as_str(), i))
            .collect();

        // 0 = unvisited, 1 = visiting, 2 = done
        let mut state = vec![0u8; self.launches.len()];
        let mut order = Vec::with_capacity(self.launches.len());

        for i in 0..self.launches.len() {
            visit(i, &self.launches, &index, &mut state, &mut order)?;
        }
        Ok(order)
    }
}

/// Build a launch from the default conventions plus the overrides. With a config, `--config <abs>` is added.
/// `domain` comes from the launch's own setting, then the launch-wide default; whichever is found is
/// added as `--domain <ns>` (with neither, it is left to the launch's own default = `REINY_DOMAIN` or `"default"`).
fn resolve(
    name: &str,
    spec: &LaunchSpec,
    cfg_dir: &Path,
    default_domain: Option<&str>,
) -> ResolvedLaunch {
    let mut args = Vec::new();
    if let Some(cfg) = spec.config() {
        args.push("--config".to_string());
        args.push(
            resolve_relative(cfg_dir, cfg)
                .to_string_lossy()
                .into_owned(),
        );
    }
    let domain = spec.domain().or(default_domain).map(str::to_string);
    if let Some(domain) = &domain {
        args.push("--domain".to_string());
        args.push(domain.clone());
    }
    if let Some(zcfg) = spec.zenoh_config() {
        args.push("--zenoh-config".to_string());
        args.push(
            resolve_relative(cfg_dir, zcfg)
                .to_string_lossy()
                .into_owned(),
        );
    }
    args.extend(spec.args().iter().cloned());
    ResolvedLaunch {
        name: name.to_string(),
        bin: spec.bin().unwrap_or(name).to_string(),
        args,
        depends_on: spec.depends_on().to_vec(),
        on_exit: spec.on_exit(),
        log_level: spec.log_level().map(str::to_string),
        domain,
    }
}

/// The format follows the extension: `.toml` is TOML, anything else is YAML (which reads JSON too).
fn parse_launch_config(path: &Path, text: &str) -> anyhow::Result<LaunchConfig> {
    let is_toml = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("toml"));
    Ok(if is_toml {
        toml::from_str(text)?
    } else {
        serde_yaml::from_str(text)?
    })
}

/// Resolve a relative path against `base` (an absolute path is returned unchanged).
fn resolve_relative(base: &Path, p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;

    /// YAML unless the extension says `.toml`; both spellings give the same plan, and the string
    /// shorthand survives the trip through `serde_yaml`'s untagged enum.
    #[test]
    fn launch_config_is_yaml_unless_toml() {
        let yaml = parse_launch_config(
            Path::new("ping-pong.yaml"),
            "launch:\n  pong: { bin: pong, on_exit: respawn }\n  ping: { bin: ping, depends_on: [pong] }\n",
        )
        .unwrap();
        let toml = parse_launch_config(
            Path::new("ping-pong.toml"),
            "[launch]\npong = { bin = \"pong\", on_exit = \"respawn\" }\nping = { bin = \"ping\", depends_on = [\"pong\"] }\n",
        )
        .unwrap();
        for c in [&yaml, &toml] {
            assert_eq!(c.launch["pong"].on_exit(), OnExit::Respawn);
            assert_eq!(c.launch["ping"].depends_on(), ["pong"]);
        }
        let bare = parse_launch_config(
            Path::new("ping-pong"),
            "launch: { gui: configs/gui.yaml }\n",
        )
        .unwrap();
        assert_eq!(
            bare.launch["gui"].config(),
            Some(Path::new("configs/gui.yaml"))
        );
    }

    /// A plan for tests. The launch config path may name something that does not exist (canonicalize
    /// fails and falls back to the given path — good enough for checking the structure).
    fn plan(s: &str) -> LaunchPlan {
        let config: LaunchConfig = toml::from_str(s).expect("parse launch config");
        LaunchPlan::from_config(&config, Path::new("reiny.toml"))
    }

    fn get<'a>(p: &'a LaunchPlan, name: &str) -> Option<&'a ResolvedLaunch> {
        p.launches.iter().find(|g| g.name == name)
    }

    #[test]
    fn bin_defaults_to_key_name() {
        let p = plan(
            r#"
            [launch]
            gui = "configs/gui.toml"
        "#,
        );
        let gui = get(&p, "gui").expect("gui present");
        assert_eq!(gui.bin, "gui", "bin defaults to the launch key");
        assert_eq!(gui.on_exit, OnExit::Ignore);
        assert!(gui.depends_on.is_empty());
        assert!(gui.args.iter().any(|a| a == "--config"));
        assert!(gui.args.iter().any(|a| a.ends_with("configs/gui.toml")));
    }

    #[test]
    fn domain_comes_from_entry_then_launch_default() {
        let p = plan(
            r#"
            domain = "lab"

            [launch]
            gui = "configs/gui.toml"
            solo = { bin = "solo", domain = "other" }
        "#,
        );
        let arg_after = |g: &ResolvedLaunch, flag: &str| {
            g.args
                .iter()
                .position(|a| a == flag)
                .and_then(|i| g.args.get(i + 1))
                .cloned()
        };
        let gui = get(&p, "gui").expect("gui present");
        assert_eq!(arg_after(gui, "--domain"), Some("lab".to_string()));
        let solo = get(&p, "solo").expect("solo present");
        assert_eq!(arg_after(solo, "--domain"), Some("other".to_string()));
        // The same value is kept as a field, so a readiness check can address the launch on the bus
        // without parsing `args` back.
        assert_eq!(gui.domain.as_deref(), Some("lab"));
        assert_eq!(solo.domain.as_deref(), Some("other"));
    }

    #[test]
    fn no_domain_anywhere_means_no_flag() {
        let p = plan(
            r#"
            [launch]
            gui = "configs/gui.toml"
        "#,
        );
        let gui = get(&p, "gui").expect("gui present");
        assert!(!gui.args.iter().any(|a| a == "--domain"));
    }

    #[test]
    fn detailed_overrides_apply() {
        let p = plan(
            r#"
            [launch]
            monitor = { config = "configs/m.toml", bin = "reiny-monitor", depends_on = ["gui"], on_exit = "respawn", args = ["--fast"] }
            gui = "configs/gui.toml"
        "#,
        );
        let m = get(&p, "monitor").expect("monitor present");
        assert_eq!(m.bin, "reiny-monitor");
        assert_eq!(m.depends_on, vec!["gui".to_string()]);
        assert_eq!(m.on_exit, OnExit::Respawn);
        // The extra args line up after `--config <abs>`.
        assert_eq!(m.args.last().map(String::as_str), Some("--fast"));
    }

    #[test]
    fn launch_without_config_has_no_config_arg() {
        let p = plan(
            r#"
            [launch]
            monitor = { bin = "reiny-monitor" }
        "#,
        );
        let m = get(&p, "monitor").expect("monitor present");
        assert!(m.args.iter().all(|a| a != "--config"));
    }

    #[test]
    fn disabled_launch_is_skipped() {
        let p = plan(
            r#"
            [launch]
            gui = { config = "configs/gui.toml", enabled = false }
        "#,
        );
        assert!(get(&p, "gui").is_none());
    }

    #[test]
    fn empty_config_yields_empty_plan() {
        let p = plan("");
        assert!(p.launches.is_empty());
        p.validate().unwrap();
    }

    #[test]
    fn topo_orders_dependencies_first() {
        let p = plan(
            r#"
            [launch]
            gui = "configs/gui.toml"
            monitor = { bin = "reiny-monitor", depends_on = ["gui"] }
        "#,
        );
        p.validate().unwrap();
        let order = p.topo_order().unwrap();
        let pos = |name: &str| {
            order
                .iter()
                .position(|&i| p.launches[i].name == name)
                .unwrap()
        };
        assert!(pos("gui") < pos("monitor"));
    }

    // The structural checks (validate / topo) build `ResolvedLaunch` directly.
    fn rg(name: &str, deps: &[&str]) -> ResolvedLaunch {
        ResolvedLaunch {
            name: name.to_string(),
            bin: name.to_string(),
            args: vec![],
            depends_on: deps.iter().map(|s| (*s).to_string()).collect(),
            on_exit: OnExit::Ignore,
            log_level: None,
            domain: None,
        }
    }

    #[test]
    fn rejects_undefined_dependency() {
        let p = LaunchPlan {
            launches: vec![rg("a", &["ghost"])],
        };
        assert_eq!(
            p.validate(),
            Err(LaunchError::UndefinedDependency {
                launch: "a".into(),
                dep: "ghost".into(),
            })
        );
    }

    #[test]
    fn detects_cycle() {
        let p = LaunchPlan {
            launches: vec![rg("a", &["b"]), rg("b", &["a"])],
        };
        assert!(matches!(p.validate(), Err(LaunchError::Cycle(_))));
    }

    #[test]
    fn rejects_duplicate_name() {
        let p = LaunchPlan {
            launches: vec![rg("a", &[]), rg("a", &[])],
        };
        assert_eq!(p.validate(), Err(LaunchError::DuplicateName("a".into())));
    }

    /// A diamond: every dependency has to come before its dependent, and every launch appears exactly
    /// once — a depth-first order that revisits a shared dependency would start it twice.
    #[test]
    fn topo_handles_a_diamond_and_visits_each_launch_once() {
        let p = LaunchPlan {
            launches: vec![
                rg("top", &["left", "right"]),
                rg("left", &["base"]),
                rg("right", &["base"]),
                rg("base", &[]),
            ],
        };
        p.validate().unwrap();
        let order = p.topo_order().unwrap();
        assert_eq!(order.len(), p.launches.len(), "each launch exactly once");
        let mut seen: Vec<&str> = order.iter().map(|&i| p.launches[i].name.as_str()).collect();
        let pos = |name: &str| seen.iter().position(|n| *n == name).unwrap();
        assert!(pos("base") < pos("left"));
        assert!(pos("base") < pos("right"));
        assert!(pos("left") < pos("top"));
        assert!(pos("right") < pos("top"));
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 4);
    }

    /// With nothing depending on anything, the order is the config's key order — the `BTreeMap` is
    /// there so that the same launch config starts in the same order every time.
    #[test]
    fn independent_launches_keep_the_key_order() {
        let p = plan(
            r#"
            [launch]
            zulu = "z.toml"
            alpha = "a.toml"
            mike = "m.toml"
        "#,
        );
        p.validate().unwrap();
        let order: Vec<&str> = p
            .topo_order()
            .unwrap()
            .iter()
            .map(|&i| p.launches[i].name.as_str())
            .collect();
        assert_eq!(order, ["alpha", "mike", "zulu"]);
    }

    /// A launch depending on itself is a cycle, not a no-op — starting it would wait for itself.
    #[test]
    fn a_self_dependency_is_a_cycle() {
        let p = LaunchPlan {
            launches: vec![rg("a", &["a"])],
        };
        assert!(matches!(p.validate(), Err(LaunchError::Cycle(_))));
    }
}
