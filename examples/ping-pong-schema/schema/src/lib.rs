//! 共有スキーマクレート。`[internals]` の型をここで 1 度だけ生成し、`internals::*` として公開する。
//!
//! `reiny::schema!()` が build.rs の生成物(`$OUT_DIR/reiny_generated.rs`)を取り込み、
//! `pingpong_schema::internals::{Ping, Pong}` と、各型の `impl reiny::Topic` / prost `Message`
//! を提供する。消費 grain(ping/pong)はこのクレートを Cargo 依存にするだけで、自前の proto
//! 再コンパイルが要らない。

reiny::schema!();
