//! relay demonstrates explicit typed ports in a namespaced deployment.

use reiny::prelude::*;

use crate::dependencies::ping::Ping;
use crate::publications::Relayed;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let out = cloudy.output::<Relayed>("relayed")?;
    let mut incoming = cloudy.input::<Ping>("ping")?;
    cloudy.ready()?;

    while let Some(ping) = incoming.recv().await {
        tracing::info!(seq = ping.seq, "↳ relaying");
        out.send(Relayed {
            seq: ping.seq,
            via: cloudy.id().to_string(),
            hops: 1,
            origin_unix: ping.sent_unix,
        })
        .await?;
    }

    Ok(())
}
