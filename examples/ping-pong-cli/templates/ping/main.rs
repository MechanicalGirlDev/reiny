//! Start a ping-pong cycle after declaring all named ports.

use reiny::prelude::*;

use crate::dependencies::pong::Pong;
use crate::exported_types::Ping;

#[reiny::main]
async fn main(cloudy: ModuleContext) -> reiny::Result<()> {
    let pings = cloudy.output::<Ping>("ping")?;
    let mut pongs = cloudy.input::<Pong>("pong")?;
    // Register the bound source readiness watch before announcing local readiness.
    let mut readiness = cloudy.watch_input_ready("pong")?;
    cloudy.ready()?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(event) = readiness.recv().await {
            match event {
                reiny::engine::Presence::Joined(_) => return true,
                reiny::engine::Presence::Left(_) => {}
            }
        }
        false
    })
    .await?;
    if !ready {
        return Ok(());
    }

    let mut seq = 0;
    pings
        .send(Ping {
            seq,
            sent_unix: cloudy.now_unix(),
        })
        .await?;
    while let Some(pong) = pongs.recv().await {
        tracing::info!(seq = pong.seq, "received pong");
        seq += 1;
        pings
            .send(Ping {
                seq,
                sent_unix: cloudy.now_unix(),
            })
            .await?;
    }
    Ok(())
}
