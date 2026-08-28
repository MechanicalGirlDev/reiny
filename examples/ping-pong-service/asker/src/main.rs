//! asker — 1 秒ごとに `Add` を call して `Sum` を受け取る。
//!
//! `call::<Add>(…)` は `reiny/<domain>/*/Add` へ撃ち、最初の応答を採る。宛先やタイムアウトを
//! 固定するなら `cloudy.caller::<Add>().to("calc").timeout(…).build()`。server が居ない間は
//! `CallError::NoReply` が即座に返る(ハングしない)ので、ループはそのまま続ける。

use std::time::Duration;

use reiny::prelude::*;

use crate::internals::Add;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let adder = cloudy
        .caller::<Add>()
        .timeout(Duration::from_secs(2))
        .build();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut n = 0;

    loop {
        tokio::select! {
            () = cloudy.shutdown() => break,
            _ = tick.tick() => {
                n += 1;
                match adder.call(Add { a: n, b: n * 10 }).await {
                    Ok(sum) => tracing::info!(a = n, b = n * 10, sum = sum.sum, "← Sum"),
                    // NoReply(server 不在)/ Remote(断られた)/ Timeout を出し分けられる。
                    Err(e) => tracing::warn!(error = %e, "Add failed"),
                }
            }
        }
    }

    Ok(())
}
