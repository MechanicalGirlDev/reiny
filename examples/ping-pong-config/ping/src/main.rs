//! ping — 最初の一球を打ち、Pong が返るたびに次の Ping を打ち返す(設定例の相手役)。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

use crate::dependencies::pong::Pong;
use crate::publications::Ping;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.publish::<Ping>()?;
    let mut pongs = cloudy.subscribe::<Pong>()?;

    let mut seq = 0;
    pings
        .send(Ping {
            seq,
            sent_unix: cloudy.now_unix(),
        })
        .await?;
    tracing::info!(seq, "ping →");

    while let Some(pong) = pongs.recv().await {
        // pong.message は pong 側の設定(reply)で決まる。
        tracing::info!(seq = pong.seq, reply = %pong.message, "← pong");
        seq += 1;
        pings
            .send(Ping {
                seq,
                sent_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq, "ping →");
    }

    Ok(())
}
