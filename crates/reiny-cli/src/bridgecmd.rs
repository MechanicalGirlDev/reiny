//! `reiny bridge serial|udp|iceoryx2` — stand up one raw bridge between zenoh and another engine.
//!
//! It builds zenoh's `Cloudy` and a second engine's (a link's / iceoryx2's) with the same id and
//! domain and hands them to `reiny::bridge::forward`. This is why an MCU's or an iceoryx2 launch shows
//! up in `reiny node list` / `topic hz` / `bag record` as-is. The CLI itself stays on zenoh.
//!
//! The one subcommand that needs tokio (`Cloudy::open` and `Host` are async).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use reiny::Cloudy;
use reiny::engine::Engine;
use reiny_link::transport::Udp;
use reiny_link::{Host, HostLink, LinkEngine};

use crate::bus::BusArgs;

#[derive(Args)]
pub(crate) struct BridgeArgs {
    #[command(flatten)]
    bus: BusArgs,
    /// The bridge's launch id (raised as `@launch` on both sides).
    #[arg(long, default_value = "bridge")]
    id: String,
    #[command(subcommand)]
    engine: BridgeEngine,
}

#[derive(Subcommand)]
enum BridgeEngine {
    /// An MCU (`reiny-link`) at the other end of a serial port.
    Serial {
        /// `/dev/ttyACM0` / `COM3`.
        path: String,
        /// The baud rate (ignored on USB CDC).
        #[arg(long, default_value_t = 115_200)]
        baud: u32,
    },
    /// A peer (`reiny-link`) at the other end of UDP.
    Udp {
        /// The address to bind (e.g. `0.0.0.0:7000`).
        bind: String,
        /// The peer's address. Unset, it is learned from the first datagram that arrives.
        #[arg(long)]
        peer: Option<SocketAddr>,
    },
    /// iceoryx2 on the same host (shared memory).
    #[cfg(feature = "iceoryx2")]
    Iceoryx2,
}

pub(crate) fn run(args: BridgeArgs) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio")?;
    rt.block_on(async move {
        let opts = args.bus.runtime_options(&args.id);
        let domain = opts.domain.clone();
        let zenoh_side = Cloudy::open(opts).await.context("opening zenoh side")?;
        let engine: Arc<dyn Engine> = match args.engine {
            BridgeEngine::Serial { path, baud } => {
                let port = reiny_link::transport::serial::open(&path, baud)
                    .with_context(|| format!("opening serial port {path}"))?;
                Arc::new(link_engine(&args.id, &domain, port)?)
            }
            BridgeEngine::Udp { bind, peer } => {
                let mut udp = Udp::bind(&bind)
                    .await
                    .with_context(|| format!("binding {bind}"))?;
                if let Some(peer) = peer {
                    udp = udp.with_peer(peer);
                }
                Arc::new(link_engine(&args.id, &domain, udp)?)
            }
            #[cfg(feature = "iceoryx2")]
            BridgeEngine::Iceoryx2 => {
                Arc::new(reiny_iceoryx2::Iceoryx2::new().context("opening iceoryx2")?)
            }
        };
        let other_side = zenoh_side
            .with_engine(engine)
            .await
            .context("opening the other side")?;
        let _bridge = reiny::bridge::forward(&zenoh_side, &other_side)?;
        tracing::info!(id = %args.id, %domain, "bridge up; Ctrl+C to stop");
        tokio::signal::ctrl_c()
            .await
            .context("waiting for Ctrl+C")?;
        Ok(())
    })
}

fn link_engine<T: reiny_link::transport::Transport + 'static>(
    id: &str,
    domain: &str,
    transport: T,
) -> Result<LinkEngine> {
    let link = HostLink::host(id)
        .map_err(|e| anyhow::anyhow!("link: {e}"))?
        .as_bridge();
    Ok(LinkEngine::spawn(Host::spawn(link, transport), domain))
}
