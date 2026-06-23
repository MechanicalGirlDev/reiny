//! ワークスペース共有 Reiny.toml 駆動のコード生成。
//!
//! `reiny_build::compile()` は上方ディレクトリを辿ってワークスペースの Reiny.toml を見つけ、
//!  - [internals] の proto をコンパイルして共有カタログ `reiny::internals::*` を生成
//!  - 現在のパッケージ名(ping)で [projects.ping] を引き、publications/dependencies に
//!    挙がった型だけを publish/subscribe 可能にし、「型 → トピック」を埋め込む
//! ことで、main.rs では型を指定するだけで publish/subscribe 先が決まる。
fn main() {
    reiny_build::compile().expect("reiny codegen from workspace Reiny.toml");
}
