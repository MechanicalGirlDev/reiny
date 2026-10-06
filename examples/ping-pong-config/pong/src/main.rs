//! pong demonstrates explicit typed ports in a namespaced deployment.

use std::time::Duration;

use reiny::prelude::*;

use crate::dependencies::ping::Ping;
use crate::publications::Pong;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let cfg = cloudy.config();
    tracing::info!(reply = %cfg.reply, delay_ms = cfg.delay_ms, "pong configured");

    let pongs = cloudy.output::<Pong>("pong")?;
    let mut pings = cloudy.input::<Ping>("ping")?;
    cloudy.ready()?;

    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "← ping");
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
