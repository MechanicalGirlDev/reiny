//! ワークスペース共有 Reiny.toml 駆動のコード生成。`[services]` から
//! `impl reiny::Service for Add { type Response = Sum; }` も生成される。
fn main() {
    reiny_build::compile().expect("reiny codegen from workspace Reiny.toml");
}
