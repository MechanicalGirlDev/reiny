//! pong — Ping を受けるたびに Pong を返す。返答文と遅延は設定から読む。
//!
//! 見どころ: 振る舞い(reply / delay_ms)をコードに埋め込まず、Reiny.toml の [config]
//! で宣言した型付き設定として `cloudy.config()` から読む。既定値は [config]、上書きは
//! 起動時の設定ファイル(launch config の `config = "pong.config.toml"`)。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use std::time::Duration;

use reiny::prelude::*;

use crate::dependencies::ping::Ping;
use crate::publications::Pong;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    // reiny::config::Config は [config] から生成された型付き設定。
    let cfg = cloudy.config();
    tracing::info!(reply = %cfg.reply, delay_ms = cfg.delay_ms, "pong configured");

    let pongs = cloudy.publish::<Pong>()?;
    let mut pings = cloudy.subscribe::<Ping>()?;

    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "← ping");
        // 設定された遅延を入れてから返す。
        tokio::time::sleep(Duration::from_millis(cfg.delay_ms)).await;
        pongs
            .send(Pong {
                seq: ping.seq,
                message: cfg.reply.clone(),
                replied_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
