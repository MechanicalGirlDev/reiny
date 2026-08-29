//! reiny ランチャ。launch config の `[launch]` 節から launch plan を導出し、各 launch bin を
//! 子プロセスとして依存順に起動して `on_exit` ポリシーで監視する。
//!
//! `HumanoidSystem` の hs-launch と違い、**既知種別(control/gui/policy/physics)もプラグインと
//! いう区別も無い**。すべてのキーが対等な「launch」で、キー名 = インスタンス名 = 既定 bin 名。

mod config;
mod launch;
mod runner;

pub use config::{LaunchConfig, LaunchEntry, LaunchSpec, OnExit};
pub use launch::{LaunchError, LaunchPlan, ResolvedLaunch};
pub use runner::{run_launch, run_launch_dirs};
