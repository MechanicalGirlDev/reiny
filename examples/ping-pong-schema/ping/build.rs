//! 消費 grain の codegen。
//!
//! `reiny_build::compile()` は上方の Reiny.toml を見つけ、このパッケージ名(`ping`)が
//! `[schema].crate` ではなく `[projects.ping]` 側なので **消費モード**で動く: proto は
//! 再コンパイルせず、`pingpong-schema` の `internals` を `crate::internals` として見せる
//! 薄い生成物だけを書く。型に紐づく impl Topic / Message はスキーマクレート側に 1 つだけある。
fn main() {
    reiny_build::compile().expect("reiny consumer codegen from workspace Reiny.toml");
}
