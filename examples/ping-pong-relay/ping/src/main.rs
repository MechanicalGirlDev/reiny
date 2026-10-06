//! ping demonstrates explicit typed ports in a namespaced deployment.

use std::time::Duration;

use reiny::prelude::*;

use crate::publications::Ping;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.output::<Ping>("ping")?;
    cloudy.ready()?;

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut seq = 0;
    loop {
        tick.tick().await;
        pings
            .send(Ping {
                seq,
                sent_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq, "ping → relay");
        seq += 1;
    }
}
