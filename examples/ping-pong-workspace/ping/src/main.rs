//! ping — 最初の一球を打ち、Pong が返るたびに次の Ping を打ち返す。
//!
//! ワークスペース共有版: 型は workspace の Reiny.toml [types] が定義する共有カタログから
//! `reiny::types::*` として来る。このプロジェクトが何を公開/購読してよいかは
//! [projects.ping] の publications/dependencies で決まる(reiny-build が検証する)。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

// 共有カタログの型(per-project 版と違い、依存先プロジェクト名で名前空間化されない)。
use reiny::types::{Ping, Pong};

/// `#[reiny::main]` は workspace の Reiny.toml を読み、このバイナリ名に対応する
/// [projects.ping] からcloudy(reiny/ping)を起動して `Cloudy` を渡す。
#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    // Ping は [projects.ping].publications にあるので reiny/ping へ送れる。
    let pings = cloudy.publish::<Ping>()?;
    // Pong は [projects.ping].dependencies にあるので reiny/pong を購読できる。
    let mut pongs = cloudy.subscribe::<Pong>()?;

    // 開幕の一球。
    let mut seq = 0;
    pings.send(Ping {
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
        pings.send(Ping {
            seq,
            message: "ping".into(),
            sent_unix: cloudy.now_unix(),
        })
        .await?;
        tracing::info!(seq, "ping →");
    }

    Ok(())
}
