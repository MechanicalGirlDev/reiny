//! pong — Ping を受け取るたびに、同じ seq の Pong を返す。
//!
//! ワークスペース共有版: 型は workspace の Reiny.toml [types] が定義する共有カタログから
//! `reiny::types::*` として来る。このプロジェクトが何を公開/購読してよいかは
//! [projects.pong] の publications/dependencies で決まる(reiny-build が検証する)。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

// 共有カタログの型(per-project 版と違い、依存先プロジェクト名で名前空間化されない)。
use reiny::types::{Ping, Pong};

/// `#[reiny::main]` は workspace の Reiny.toml を読み、このバイナリ名に対応する
/// [projects.pong] からノード(reiny/pong)を起動して `Node` を渡す。
#[reiny::main]
async fn main(node: Node) -> reiny::Result<()> {
    // Pong は [projects.pong].publications にあるので reiny/pong へ送れる。
    let pongs = node.publish::<Pong>()?;
    // Ping は [projects.pong].dependencies にあるので reiny/ping を購読できる。
    let mut pings = node.subscribe::<Ping>()?;

    // Ping を受けるたびに、同じ seq で打ち返す。shutdown(Ctrl+C)で抜ける。
    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "← ping");
        pongs.send(Pong {
            seq: ping.seq,
            message: "pong".into(),
            replied_unix: node.now_unix(),
        })
        .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
