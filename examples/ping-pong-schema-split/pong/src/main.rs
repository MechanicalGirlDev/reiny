//! pong — Ping を受け取るたびに、同じ seq の Pong を返す。
//!
//! 分割スキーマ版: 受け取った `Ping.at`(geometry 区画の `Point`)をそのまま `Pong.at` へ
//! 移し替える。別クレート生成の型が 1 つに解決されていなければ、この代入が通らない。

use reiny::prelude::*;

use crate::internals::{Ping, Pong};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pongs = cloudy.publish::<Pong>()?;
    let mut pings = cloudy.subscribe::<Ping>()?;

    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "← ping");
        pongs
            .send(Pong {
                seq: ping.seq,
                message: "pong".into(),
                replied_unix: cloudy.now_unix(),
                // 型が 1 つに解決されているので、そのまま持ち回せる。
                at: ping.at,
            })
            .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
