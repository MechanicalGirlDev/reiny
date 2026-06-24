//! 共有スキーマクレートの codegen。
//!
//! `reiny_build::compile()` は上方の Reiny.toml を見つけ、このパッケージ名
//! (`pingpong-schema`)が `[schema].crate` と一致するので **スキーマ本体モード**で動く:
//! [internals] の proto を prost コンパイルし、型 + impl Topic + `internals` モジュールを
//! `$OUT_DIR/reiny_generated.rs` に書く。lib.rs が `reiny::schema!()` で取り込む。
fn main() {
    reiny_build::compile().expect("reiny schema codegen from workspace Reiny.toml");
}
