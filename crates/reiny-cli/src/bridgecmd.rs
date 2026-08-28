//! `reiny bridge serial|udp|iceoryx2` —— zenoh と別エンジンの間に raw bridge を 1 本立てる。
//!
//! zenoh の `Cloudy` と、もう 1 つのエンジン(リンク / iceoryx2)の `Cloudy` を同じ id / domain
//! で組み、`reiny::bridge::forward` に渡す。MCU や iceoryx2 の grain が `reiny node list` /
//! `topic hz` / `bag record` にそのまま出るのはこれのおかげ。CLI 自体は zenoh のまま。
//!
//! 唯一 tokio が要るサブコマンド(`Cloudy::open` と `Host` が async)。

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
    /// bridge の grain id(両側の `@grain` に立つ)。
    #[arg(long, default_value = "bridge")]
    id: String,
    #[command(subcommand)]
    engine: BridgeEngine,
}

#[derive(Subcommand)]
enum BridgeEngine {
    /// シリアルポートの向こうの MCU(`reiny-link`)。
    Serial {
        /// `/dev/ttyACM0` / `COM3`。
        path: String,
        /// baud(USB CDC では無視される)。
        #[arg(long, default_value_t = 115_200)]
        baud: u32,
    },
    /// UDP の向こうの相手(`reiny-link`)。
    Udp {
        /// bind するアドレス(例 `0.0.0.0:7000`)。
        bind: String,
        /// 相手のアドレス。省略すると最初に届いた datagram の送り元。
        #[arg(long)]
        peer: Option<SocketAddr>,
    },
    /// 同一ホストの iceoryx2(共有メモリ)。
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
