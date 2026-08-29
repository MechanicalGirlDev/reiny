//! Run reiny's conformance test against the iceoryx2 engine. Two engines (= two nodes) live in one
//! process, so the real path through shared memory runs. `prefix` varies per process to isolate them.

#![allow(clippy::expect_used)] // tests may fail by panicking

use std::sync::Arc;

use iceoryx2::prelude::*;
use reiny::engine::conformance::{cloudy, exercise};
use reiny_iceoryx2::Iceoryx2;

#[tokio::test(flavor = "multi_thread")]
async fn two_launches_in_one_process() {
    // So that a failure shows both reiny's reason (warn) and iceoryx2's (debug).
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
