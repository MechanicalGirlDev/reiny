//! pong demonstrates explicit typed ports in a namespaced deployment.

use reiny::prelude::*;

use crate::internals::{Ping, Pong};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pongs = cloudy.output::<Pong>("pong")?;
    let mut pings = cloudy.input::<Ping>("ping")?;
    cloudy.ready()?;

    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "← ping");
        pongs
            .send(Pong {
                seq: ping.seq,
                message: "pong".into(),
                replied_unix: cloudy.now_unix(),
                at: ping.at,
            })
            .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
