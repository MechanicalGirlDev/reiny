//! ワークスペース共有 Reiny.toml 駆動のコード生成(calc と同じ)。
fn main() {
    reiny_build::compile().expect("reiny codegen from workspace Reiny.toml");
}
