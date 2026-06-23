//! Reiny.toml 駆動のコード生成。
//! [publications]/[dependencies] の型に加え、[config] から型付き設定 `reiny::config::Config`
//! を生成する(既定値はここ、上書きは起動時の設定ファイル)。
fn main() {
    reiny_build::compile().expect("reiny codegen from Reiny.toml");
}
