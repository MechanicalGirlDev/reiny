//! ping — パイプラインの source。Ping を一定間隔で流すだけ(購読しない純粋な生産者)。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use std::time::Duration;

use reiny::prelude::*;

use crate::publications::Ping;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.publish::<Ping>()?;

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut seq = 0;
    loop {
        tick.tick().await;
        pings
            .send(Ping {
                seq,
                sent_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq, "ping → relay");
        seq += 1;
    }
}
