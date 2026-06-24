//! ping — 最初の一球を打ち、Pong が返るたびに次の Ping を打ち返す。
//!
//! 共有スキーマ版: 型は `[schema].crate`(pingpong-schema)が 1 度だけ生成したものを
//! `crate::internals::*` として再エクスポートで受け取る(../ping-pong-workspace と書き味は
//! 同じだが、この grain では proto を再コンパイルしない)。

use reiny::prelude::*;

// 共有スキーマクレート由来の型。reiny-build の消費モードが pingpong-schema を再エクスポート
// するので、書き味は workspace 版と同じ `crate::internals`。
use crate::internals::{Ping, Pong};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.publish::<Ping>()?;
    let mut pongs = cloudy.subscribe::<Pong>()?;

    // 開幕の一球。
    let mut seq = 0;
    pings
        .send(Ping {
            seq,
            message: "ping".into(),
            sent_unix: cloudy.now_unix(),
        })
        .await?;
    tracing::info!(seq, "ping →");

    // Pong が返るたびに seq を進めて打ち返す。shutdown(Ctrl+C)で抜ける。
    while let Some(pong) = pongs.recv().await {
        tracing::info!(seq = pong.seq, "← pong");
        seq += 1;
        pings
            .send(Ping {
                seq,
                message: "ping".into(),
                sent_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq, "ping →");
    }

    Ok(())
}
