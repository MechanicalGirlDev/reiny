//! pong — Ping を受け取るたびに、同じ seq の Pong を返す。
//!
//! 目標とする開発体験のスケッチ:
//!   - 自分の公開型 `Pong` は Reiny.toml の [publications] により reiny/pong-1/Pong へ publish される。
//!   - 依存先の型 `Ping` は [dependencies] の ping により reiny/*/Ping から subscribe する。
//!   - そのため cloudy.publish/subscribe には *型* を渡すだけでよく、トピック名は出てこない。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

// build.rs(reiny-build)が Reiny.toml から生成する型。
use crate::publications::Pong; // 自分が公開する型
use crate::dependencies::ping::Ping; // 依存先 ping が公開する型

/// `#[reiny::main]` は Reiny.toml を読み込み、name のプロセス(cloudy)を起動して
/// `Cloudy` を渡す。型 → トピックの解決・セッション確立・graceful shutdown を肩代わりする。
#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    // 公開型 Pong の publisher。Pong は [publications] にあるので reiny/pong-1/Pong へ送られる。
    let pongs = cloudy.publish::<Pong>()?;
    // 依存先の型 Ping の subscriber。Ping は ping の公開型なので reiny/*/Ping を購読する。
    let mut pings = cloudy.subscribe::<Ping>()?;

    // Ping を受けるたびに、同じ seq で打ち返す。shutdown(Ctrl+C)で抜ける。
    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "← ping");
        pongs.send(Pong {
            seq: ping.seq,
            message: "pong".into(),
            replied_unix: cloudy.now_unix(),
        })
        .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
