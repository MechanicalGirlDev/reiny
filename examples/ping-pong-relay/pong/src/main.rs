//! pong demonstrates explicit typed ports in a namespaced deployment.

use reiny::prelude::*;

use crate::dependencies::relay::Relayed;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let mut incoming = cloudy.input::<Relayed>("relayed")?;
    cloudy.ready()?;

    while let Some(m) = incoming.recv().await {
        let elapsed = cloudy.now_unix() - m.origin_unix;
        tracing::info!(
            seq = m.seq,
            via = %m.via,
            hops = m.hops,
            elapsed_s = elapsed,
            "● sink received"
        );
    }

    Ok(())
}
