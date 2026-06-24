//! pong — Ping を受け取るたびに、同じ seq の Pong を返す。
//!
//! 共有スキーマ版: 型は `[schema].crate`(pingpong-schema)が 1 度だけ生成したものを
//! `crate::internals::*` として再エクスポートで受け取る(この grain では proto を再コンパイル
//! しない)。

use reiny::prelude::*;

use crate::internals::{Ping, Pong};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pongs = cloudy.publish::<Pong>()?;
    let mut pings = cloudy.subscribe::<Ping>()?;

    // Ping を受けるたびに、同じ seq で打ち返す。shutdown(Ctrl+C)で抜ける。
    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "← ping");
        pongs
            .send(Pong {
                seq: ping.seq,
                message: "pong".into(),
                replied_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
