//! pong demonstrates explicit typed ports in a namespaced deployment.

use reiny::prelude::*;

use crate::dependencies::ping::Ping;
use crate::publications::Pong;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pongs = cloudy.output::<Pong>("pong")?;
    let mut pings = cloudy.input::<Ping>("ping")?;
    cloudy.ready()?;

    tracing::info!(id = %cloudy.id(), "pong instance ready");

    while let Some(ping) = pings.recv().await {
        pongs
            .send(Pong {
                seq: ping.seq,
                from: cloudy.id().to_string(),
                replied_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq = ping.seq, "pong →");
    }

    Ok(())
}
