//! 共有スキーマクレート。`[shared_types]` の型をここで 1 度だけ生成し、`shared_types::*` として公開する。
//!
//! `reiny::schema!()` が build.rs の生成物(`$OUT_DIR/reiny_generated.rs`)を取り込み、
//! `pingpong_schema::shared_types::{Ping, Pong}` と、各型の `impl reiny::MessageType` / prost `Message`
//! を提供する。消費 launch(ping/pong)はこのクレートを Cargo 依存にするだけで、自前の proto
//! 再コンパイルが要らない。

reiny::schema!();
