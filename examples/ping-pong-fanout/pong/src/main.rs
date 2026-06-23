//! pong — Ping を受けるたびに、自分のインスタンス id を載せた Pong を返す。
//!
//! 同じ bin を複数起動すると reiny が連番 id(pong-1, pong-2, ...)を自動採番する。
//! それぞれが reiny/*/Ping を購読し、それぞれが Pong を返すので、ping には N 個届く。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

use crate::dependencies::ping::Ping;
use crate::publications::Pong;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pongs = cloudy.publish::<Pong>()?;
    let mut pings = cloudy.subscribe::<Ping>()?;

    // cloudy.id() は自動採番された自分のインスタンス id(例 "pong-2")。
    tracing::info!(id = %cloudy.id(), "pong instance ready");

    while let Some(ping) = pings.recv().await {
        pongs
            .send(Pong {
                seq: ping.seq,
                from: cloudy.id().to_string(),
                replied_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
