//! reiny の適合テストを iceoryx2 エンジンに流す。2 つの engine(= 2 node)を同一プロセスに
//! 置くので、共有メモリ越しの本物の経路が走る。`prefix` を process ごとに変えて隔離する。

#![allow(clippy::expect_used)] // テストは panic で失敗を表現してよい

use std::sync::Arc;

use iceoryx2::prelude::*;
use reiny::engine::conformance::{cloudy, exercise};
use reiny_iceoryx2::Iceoryx2;

#[tokio::test(flavor = "multi_thread")]
async fn two_grains_in_one_process() {
    // 失敗したときに reiny(warn)と iceoryx2(debug)の両方の理由が見えるように。
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .try_init();
    set_log_level(LogLevel::Debug);
    let mut config = Config::default();
    let prefix = format!("reiny_conf_{}_", std::process::id());
    config.global.prefix = FileName::new(prefix.as_bytes()).expect("prefix");
    let a = Arc::new(Iceoryx2::with_config(config.clone()).expect("engine a"));
    let b = Arc::new(Iceoryx2::with_config(config).expect("engine b"));
    exercise(cloudy(a, "a").await, cloudy(b, "b").await).await;
}
