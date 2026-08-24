//! ping — 最初の一球を打ち、Pong が返るたびに次の Ping を打ち返す。
//!
//! 分割スキーマ版: `Ping`/`Pong` は pingpong-msg、`Point` は pingpong-geometry が生成した型で、
//! `crate::internals` はその **和**。`Point` が 1 つの型であることは、msg の `Ping.at` に
//! geometry の `Point` を直接入れられることで確かめられる(二重生成されていたら型が合わない)。

use reiny::prelude::*;

use crate::internals::{Ping, Point, Pong};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.publish::<Ping>()?;
    let mut pongs = cloudy.subscribe::<Pong>()?;

    let mut seq = 0;
    let serve = |seq: u64, cloudy: &Cloudy| Ping {
        seq,
        message: "ping".into(),
        sent_unix: cloudy.now_unix(),
        // geometry 区画の型をそのまま入れる。extern_path が効いていないとここが
        // 「pingpong_msg 側の Point」との型不一致でコンパイルできない。
        at: Some(Point {
            x: f64::from(u32::try_from(seq % 10).unwrap_or(0)),
            y: 0.0,
        }),
    };

    pings.send(serve(seq, &cloudy)).await?;
    tracing::info!(seq, "ping →");

    while let Some(pong) = pongs.recv().await {
        let at = pong.at.unwrap_or_default();
        tracing::info!(seq = pong.seq, x = at.x, y = at.y, "← pong");
        seq += 1;
        pings.send(serve(seq, &cloudy)).await?;
        tracing::info!(seq, "ping →");
    }

    Ok(())
}
