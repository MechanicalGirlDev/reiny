//! Reiny.toml 駆動のコード生成。
//! このプロジェクトは公開型なし([publications] 空)。[dependencies] の公開型から
//! `reiny::dependencies::<project>::*` を生成し、「型 → トピック」を埋め込む。
fn main() {
    reiny_build::compile().expect("reiny codegen from Reiny.toml");
}
