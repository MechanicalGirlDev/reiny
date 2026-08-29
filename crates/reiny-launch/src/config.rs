//! A launch config's `[launch]` table — the reiny launcher's only entry point.
//!
//! It is written like Cargo's `[dependencies]`, where each value is either
//!
//! - **a string** = shorthand for the launch's own config file path
//!   (`gui = "configs/gui.toml"` ≡ `gui = { config = "configs/gui.toml" }`), or
//! - **an inline table** = the detailed form, with per-launch overrides
//!   (`monitor = { bin = "...", on_exit = "respawn" }`).
//!
//! Unlike `HumanoidSystem`'s `[component]`, there are **no known kinds (control/gui/policy/physics)
//! and no plugin distinction**. Every key is an equal "launch": key = instance name = default bin
//! name. The launcher starts each launch as a child process from the same workspace.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// What happens when a launch's process exits (the launcher's interpretation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnExit {
    /// Record it and carry on (the default). Launches are equals, so there is no privileged
    /// `control` whose exit stops everything. The launcher ends once every launch has exited.
    #[default]
    Ignore,
    /// Restart that same launch.
    Respawn,
    /// Stop everything as soon as one exits.
    ShutdownAll,
}

/// A launch config's root: the `[launch]` table plus the defaults that apply across it.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LaunchConfig {
    /// The logical namespace of this whole launch (`--domain`). It keeps launches on the same LAN or
    /// machine from mixing — hardware versus log replay, two robots, and parallel CI jobs are all
    /// saved by it for the same reason. A launch's own `domain` wins over this.
    pub domain: Option<String>,
    /// The launches to start. Key = instance name = default bin name. A `BTreeMap` makes the key
    /// order deterministic, which keeps the start order stable when nothing declares a dependency.
    #[serde(default)]
    pub launch: BTreeMap<String, LaunchSpec>,
}

/// One launch's declaration. As with a Cargo dependency, it is either a string (shorthand for the
/// config path) or an inline table (with per-launch overrides).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum LaunchSpec {
    /// Shorthand: the config file path alone (= `{ config = "..." }`).
    Config(PathBuf),
    /// The detailed form, carrying per-launch overrides.
    Detailed(LaunchEntry),
}

/// The detailed form's fields (all optional).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct LaunchEntry {
    /// The path to the launch's own config file, relative to the launch config's directory.
    /// Setting it adds `--config <abs>` to the startup arguments.
    pub config: Option<PathBuf>,
    /// An override for the bin name (unset = the key name).
    pub bin: Option<String>,
    /// Extra startup arguments (appended after `--config <...>`).
    pub args: Vec<String>,
    /// Dependencies (started before this one).
    pub depends_on: Vec<String>,
    /// What happens when the process exits (unset = `Ignore`).
    pub on_exit: Option<OnExit>,
    /// An override for the log level.
    pub log_level: Option<String>,
    /// An override for the logical namespace (unset = the launch config's `domain`).
    /// Launches in different domains do not talk, so normally the whole launch shares one.
    pub domain: Option<String>,
    /// The path to a zenoh session configuration file (JSON5), relative to the launch config's
    /// directory. Setting it adds `--zenoh-config <abs>` to the startup arguments.
    pub zenoh_config: Option<PathBuf>,
    /// `false` leaves this launch out of the plan (default `true`).
    pub enabled: Option<bool>,
}

impl LaunchSpec {
    /// The path to the launch's own config file (relative to the launch config's directory).
    #[must_use]
    pub fn config(&self) -> Option<&Path> {
        match self {
            Self::Config(p) => Some(p),
            Self::Detailed(e) => e.config.as_deref(),
        }
    }

    /// An override for the bin name (unset = the key name).
    #[must_use]
    pub fn bin(&self) -> Option<&str> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.bin.as_deref(),
        }
    }

    /// Extra startup arguments.
    #[must_use]
    pub fn args(&self) -> &[String] {
        match self {
            Self::Config(_) => &[],
            Self::Detailed(e) => &e.args,
        }
    }

    /// `depends_on`.
    #[must_use]
    pub fn depends_on(&self) -> &[String] {
        match self {
            Self::Config(_) => &[],
            Self::Detailed(e) => &e.depends_on,
        }
    }

    /// `on_exit` (unset = `Ignore`).
    #[must_use]
    pub fn on_exit(&self) -> OnExit {
        match self {
            Self::Config(_) => OnExit::default(),
            Self::Detailed(e) => e.on_exit.unwrap_or_default(),
        }
    }

    /// An override for `log_level`.
    #[must_use]
    pub fn log_level(&self) -> Option<&str> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.log_level.as_deref(),
        }
    }

    /// An override for `domain`.
    #[must_use]
    pub fn domain(&self) -> Option<&str> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.domain.as_deref(),
        }
    }

    /// The path to a zenoh session configuration file (relative to the launch config's directory).
    #[must_use]
    pub fn zenoh_config(&self) -> Option<&Path> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.zenoh_config.as_deref(),
        }
    }

    /// Whether it is part of the plan (default `true`). `enabled = false` leaves it out.
    #[must_use]
    pub fn enabled(&self) -> bool {
        match self {
            Self::Config(_) => true,
            Self::Detailed(e) => e.enabled.unwrap_or(true),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;

    fn config(s: &str) -> LaunchConfig {
        toml::from_str::<LaunchConfig>(s).expect("parse launch config")
    }

    #[test]
    fn shorthand_string_is_config_path() {
        let c = config(
            r#"
            [launch]
            gui = "configs/gui.toml"
        "#,
        );
        let g = &c.launch["gui"];
        assert_eq!(g.config(), Some(Path::new("configs/gui.toml")));
        assert_eq!(g.bin(), None);
        assert_eq!(g.on_exit(), OnExit::Ignore);
        assert!(g.enabled());
        assert!(g.depends_on().is_empty());
    }

    #[test]
    fn detailed_form_overrides() {
        let c = config(
            r#"
            [launch]
            monitor = { bin = "reiny-monitor", on_exit = "respawn", depends_on = ["gui"], args = ["--fast"] }
        "#,
        );
        let g = &c.launch["monitor"];
        assert_eq!(g.bin(), Some("reiny-monitor"));
        assert_eq!(g.on_exit(), OnExit::Respawn);
        assert_eq!(g.depends_on(), ["gui".to_string()]);
        assert_eq!(g.args(), ["--fast".to_string()]);
    }

    #[test]
    fn domain_and_zenoh_config_parse() {
        let c = config(
            r#"
            domain = "lab"

            [launch]
            gui = { config = "configs/gui.toml", zenoh_config = "z.json5" }
            solo = { bin = "solo", domain = "other" }
        "#,
        );
        assert_eq!(c.domain.as_deref(), Some("lab"));
        assert_eq!(c.launch["gui"].zenoh_config(), Some(Path::new("z.json5")));
        assert_eq!(
            c.launch["gui"].domain(),
            None,
            "an unset entry defers to the launch-wide default"
        );
        assert_eq!(c.launch["solo"].domain(), Some("other"));
    }

    #[test]
    fn disabled_flag_parses() {
        let c = config(
            r#"
            [launch]
            gui = { config = "configs/gui.toml", enabled = false }
        "#,
        );
        assert!(!c.launch["gui"].enabled());
    }
}
