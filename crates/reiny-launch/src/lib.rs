//! reiny ランチャ。launch config の `[grain]` 節から launch plan を導出し、各 grain bin を
//! 子プロセスとして依存順に起動して `on_exit` ポリシーで監視する。
//!
//! HumanoidSystem の hs-launch と違い、**既知種別(control/gui/policy/physics)もプラグインと
//! いう区別も無い**。すべてのキーが対等な「grain」で、キー名 = インスタンス名 = 既定 bin 名。

mod config;
mod launch;
mod runner;

pub use config::{GrainEntry, GrainSpec, LaunchConfig, OnExit};
pub use launch::{LaunchError, LaunchPlan, ResolvedGrain};
pub use runner::run_launch;
