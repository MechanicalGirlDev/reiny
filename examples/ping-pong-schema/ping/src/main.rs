//! ping demonstrates explicit typed ports in a namespaced deployment.

use reiny::prelude::*;

use crate::internals::{Ping, Pong};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let pings = cloudy.output::<Ping>("ping")?;
    let mut pongs = cloudy.input::<Pong>("pong")?;
    // Arm an exact readiness watch before advertising our own readiness.
    let (parent, _) = cloudy.id().rsplit_once('/').ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ping must run inside a namespaced composition",
        )
    })?;
    let pong = format!("{parent}/pong");
    let mut readiness = cloudy.watch_keys(&reiny::engine::Key::topic(
        cloudy.domain(),
        Some(&pong),
        "@ready",
    ))?;
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
            message: "ping".into(),
            sent_unix: cloudy.now_unix(),
        })
        .await?;
    tracing::info!(seq, "ping →");

    while let Some(pong) = pongs.recv().await {
        tracing::info!(seq = pong.seq, "← pong");
        seq += 1;
        pings
            .send(Ping {
                seq,
                message: "ping".into(),
                sent_unix: cloudy.now_unix(),
            })
            .await?;
        tracing::info!(seq, "ping →");
    }

    Ok(())
}
