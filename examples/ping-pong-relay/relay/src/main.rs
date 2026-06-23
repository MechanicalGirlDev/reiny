//! relay — 中継ノード。上流 `Ping` を受けて `Relayed` に変換し、下流へ流す。
//!
//! 見どころ: 1 つのノードが「購読した型」と「公開する型」を持ち、その間で変換する。
//! これを並べるとデータフローのパイプライン(ping → relay → pong)になる。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

use reiny::publications::Relayed;
use reiny::dependencies::ping::Ping;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let out = cloudy.publish::<Relayed>()?;
    let mut incoming = cloudy.subscribe::<Ping>()?;

    while let Some(ping) = incoming.recv().await {
        tracing::info!(seq = ping.seq, "↳ relaying");
        out.send(Relayed {
            seq: ping.seq,
            via: cloudy.id().to_string(),
            hops: 1,
            origin_unix: ping.sent_unix,
        })
        .await?;
    }

    Ok(())
}
