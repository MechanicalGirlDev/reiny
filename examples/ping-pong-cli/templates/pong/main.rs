//! Reply to pings through the declared output port.

use reiny::prelude::*;

use crate::dependencies::ping::Ping;
use crate::exported_types::Pong;

#[reiny::main]
async fn main(cloudy: ModuleContext) -> reiny::Result<()> {
    let pongs = cloudy.output::<Pong>("pong")?;
    let mut pings = cloudy.input::<Ping>("ping")?;
    cloudy.ready()?;

    while let Some(ping) = pings.recv().await {
        tracing::info!(seq = ping.seq, "received ping");
        pongs
            .send(Pong {
                seq: ping.seq,
                sent_unix: cloudy.now_unix(),
            })
            .await?;
    }
    Ok(())
}
