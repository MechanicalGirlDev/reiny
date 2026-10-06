//! ping demonstrates explicit typed ports in a namespaced deployment.

use std::time::Duration;

use reiny::prelude::*;

use crate::dependencies::pong::Pong;
use crate::publications::Ping;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.output::<Ping>("ping")?;
    let mut pongs_1 = cloudy.input::<Pong>("pong_1")?;
    let mut pongs_2 = cloudy.input::<Pong>("pong_2")?;
    let mut pongs_3 = cloudy.input::<Pong>("pong_3")?;
    cloudy.ready()?;

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut seq = 0;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                pings.send(Ping { seq, sent_unix: cloudy.now_unix() }).await?;
                tracing::info!(seq, "ping → (broadcast)");
                seq += 1;
            }
            Some(pong) = pongs_1.recv() => {
                tracing::info!(seq = pong.seq, from = %pong.from, "← pong");
            }
            Some(pong) = pongs_2.recv() => {
                tracing::info!(seq = pong.seq, from = %pong.from, "← pong");
            }
            Some(pong) = pongs_3.recv() => {
                tracing::info!(seq = pong.seq, from = %pong.from, "← pong");
            }
        }
    }
}
