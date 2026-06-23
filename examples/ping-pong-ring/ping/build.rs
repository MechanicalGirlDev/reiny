//! Reiny.toml 駆動のコード生成。
//!
//! `reiny_build::compile()` が Reiny.toml を読み、
//!  - [publications] の proto をコンパイルして `reiny::publications::*` を生成
//!  - [dependencies] の各プロジェクトの公開型を解決して `reiny::dependencies::<project>::*` を生成
//!  - 「型 → トピック」マッピング(publish: Ping → reiny/<id>/Ping、subscribe: Pong → reiny/*/Pong)を埋め込む
//!
//! ことで、main.rs では型を指定するだけで publish/subscribe 先が決まる。
fn main() {
    reiny_build::compile().expect("reiny codegen from Reiny.toml");
}
