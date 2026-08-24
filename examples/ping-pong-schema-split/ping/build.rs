//! 消費 grain の codegen。proto は再コンパイルせず、スキーマクレート **群** の
//! `internals` を `crate::internals` としてまとめて見せる薄い生成物だけを書く。
fn main() {
    reiny_build::compile().expect("reiny consumer codegen (ping)");
}
