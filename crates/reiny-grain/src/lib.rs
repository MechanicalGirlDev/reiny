//! HOS grain SDK。
//!
//! 任意のプロセスが、自分専用の名前空間 `hos/<id>/...` を持つ「grain インスタンス」として
//! hs-gui / 他プロセスへ顔を出すための SDK。協調シャットダウンは [`Shutdown`] で行う。
//! grain の本体は [`grain::Grain`] / [`gui::GuiPanel`]（`gui` フィーチャ）。

mod shutdown;

#[cfg(feature = "gui")]
pub mod grain;
#[cfg(feature = "gui")]
pub mod gui;

pub use shutdown::Shutdown;
