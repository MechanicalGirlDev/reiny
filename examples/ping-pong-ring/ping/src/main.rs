//! ping — 最初の一球を打ち、Pong が返るたびに次の Ping を打ち返す。
//!
//! 目標とする開発体験のスケッチ:
//!   - 自分の公開型 `Ping` は Reiny.toml の [publications] により reiny/ping トピックに紐づく。
//!   - 依存先の型 `Pong` は [dependencies] の pong により reiny/pong トピックに紐づく。
//!   - そのため node.publish/subscribe には *型* を渡すだけでよく、トピック名は出てこない。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

// build.rs(reiny-build)が Reiny.toml から生成する型。
use reiny::publications::Ping; // 自分が公開する型
use reiny::dependencies::pong::Pong; // 依存先 pong が公開する型

/// `#[reiny::main]` は Reiny.toml を読み込み、name に基づくノード(reiny/ping)を起動して
/// `Node` を渡す。トピックの解決・セッション確立・graceful shutdown を肩代わりする。
#[reiny::main]
async fn main(node: Node) -> reiny::Result<()> {
    // 公開型 Ping の publisher。Ping は [publications] にあるので reiny/ping へ送られる。
    let pings = node.publish::<Ping>()?;
    // 依存先の型 Pong の subscriber。Pong は pong の公開型なので reiny/pong を購読する。
    let mut pongs = node.subscribe::<Pong>()?;

    // 開幕の一球。
    let mut seq = 0;
    pings.send(Ping {
        seq,
        message: "ping".into(),
        sent_unix: node.now_unix(),
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
            sent_unix: node.now_unix(),
        })
        .await?;
        tracing::info!(seq, "ping →");
    }

    Ok(())
}
