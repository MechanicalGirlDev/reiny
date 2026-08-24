//! リーフのスキーマ区画の codegen。
//!
//! パッケージ名が `[schema.geometry].crate` と一致するので、reiny-build は
//! **区画ビルド**として動く: この区画が所有する proto だけを prost コンパイルし、
//! 自分が定義した FQN 一覧と include パスを cargo の links メタで下流へ渡す。
fn main() {
    reiny_build::compile().expect("reiny schema codegen ([schema.geometry])");
}
