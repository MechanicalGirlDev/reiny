//! Ping/Pong を持つスキーマ区画の codegen。
//!
//! `[schema.msg].depends = ["geometry"]` から、reiny-build が
//! `DEP_PINGPONG_GEOMETRY_PROTO_INCLUDE` / `_PROTO_TYPES` を読み、
//! include パスと `extern_path`(`.pingpong.Point` → `::pingpong_geometry::__pb::…`)
//! を自動で組む。手書きの build.rs は要らない。
fn main() {
    reiny_build::compile().expect("reiny schema codegen ([schema.msg])");
}
