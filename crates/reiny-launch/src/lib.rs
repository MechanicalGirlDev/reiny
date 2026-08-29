//! The reiny launcher. It derives a launch plan from a launch config's `[launch]` section, starts
//! each launch bin as a child process in dependency order, and monitors them under their `on_exit` policy.
//!
//! Unlike `HumanoidSystem`'s hs-launch there are **no known kinds (control/gui/policy/physics) and no
//! plugin distinction**. Every key is an equal "launch": key = instance name = default bin name.

mod config;
mod launch;
mod runner;

pub use config::{LaunchConfig, LaunchEntry, LaunchSpec, OnExit};
pub use launch::{LaunchError, LaunchPlan, ResolvedLaunch};
pub use runner::{Ready, run_launch, run_launch_dirs};
