//! ping — Ping を一定間隔で broadcast し、全 pong インスタンスからの返球を受ける。
//!
//! 見どころ: pong を複数インスタンス起動しても、ping 側は型 `Pong` を購読するだけ。
//! reiny がワイルドカードで全インスタンス(pong-1, pong-2, ...)の Pong をまとめて届ける。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use std::time::Duration;

use reiny::prelude::*;

use reiny::publications::Ping;
use reiny::dependencies::pong::Pong;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.publish::<Ping>()?;
    let mut pongs = cloudy.subscribe::<Pong>()?;

    // 送信(1 秒ごとに 1 球 broadcast)と受信(全 pong からの返球)を同時に回す。
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut seq = 0;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                pings.send(Ping { seq, sent_unix: cloudy.now_unix() }).await?;
                tracing::info!(seq, "ping → (broadcast)");
                seq += 1;
            }
            // 1 球の Ping に対し、起動中の pong インスタンス数だけ Pong が返る。
            Some(pong) = pongs.recv() => {
                tracing::info!(seq = pong.seq, from = %pong.from, "← pong");
            }
        }
    }
}
