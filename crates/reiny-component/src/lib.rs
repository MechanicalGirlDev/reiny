//! HOS コンポーネント基盤。
//!
//! `Component` トレイトでロジックをホスト(プロセス/スレッド)から切り離し、
//! `run_component` (プロセスホスト) と `Container` (スレッドホスト) が駆動する。

mod component;
mod container;
mod shutdown;

#[cfg(feature = "gui")]
pub mod gui;
#[cfg(feature = "gui")]
pub mod plugin;

pub use component::{CommonArgs, Component, run_component};
pub use container::Container;
pub use shutdown::Shutdown;
