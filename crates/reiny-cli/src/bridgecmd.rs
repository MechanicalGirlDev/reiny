//! `reiny bridge serial|udp|iceoryx2` — stand up one raw bridge between zenoh and another engine.
//!
//! The prepared contract pairs named inputs and outputs. Native descriptors and
//! negotiated Link metadata determine fingerprints before the SDK opens forwarding ports and
//! announces readiness. Forwarded data uses the declared bridge endpoint namespace.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use clap::{Args, Subcommand};
use reiny::ModuleContext;
use reiny::bindings::{EndpointAddress, EndpointContract, ModuleBindings, PortKind};
use reiny::engine::Engine;
use reiny_link::transport::Udp;
use reiny_link::{Host, LinkEngine, PeerType};

use crate::bus::{BusArgs, schema_fingerprint, topic_type};

#[derive(Args)]
pub(crate) struct BridgeArgs {
    #[command(flatten)]
    bus: BusArgs,
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
    let bindings = args.bus.bindings()?;
    validate_bridge_ports(&bindings)?;
    let id = bindings.namespace.clone();
    let rt = tokio::runtime::Runtime::new().context("starting tokio")?;
    rt.block_on(async move {
        let mut opts = args.bus.runtime_options(&id);
        opts.module_bindings = Some(bindings.clone());
        let domain = opts.domain.clone();
        let zenoh_side = ModuleContext::open(opts)
            .await
            .context("opening zenoh side")?;
        let (engine, link): (Arc<dyn Engine>, Option<Arc<LinkEngine>>) = match args.engine {
            BridgeEngine::Serial { path, baud } => {
                let port = reiny_link::transport::serial::open(&path, baud)
                    .with_context(|| format!("opening serial port {path}"))?;
                let link = Arc::new(link_engine(&id, &domain, port)?);
                (link.clone(), Some(link))
            }
            BridgeEngine::Udp { bind, peer } => {
                let mut udp = Udp::bind(&bind)
                    .await
                    .with_context(|| format!("binding {bind}"))?;
                if let Some(peer) = peer {
                    udp = udp.with_peer(peer);
                }
                let link = Arc::new(link_engine(&id, &domain, udp)?);
                (link.clone(), Some(link))
            }
            #[cfg(feature = "iceoryx2")]
            BridgeEngine::Iceoryx2 => (
                Arc::new(reiny_iceoryx2::Iceoryx2::new().context("opening iceoryx2")?),
                None,
            ),
        };
        let other_side = zenoh_side
            .with_engine(engine)
            .await
            .context("opening the other side")?;
        let peer = if let Some(link) = &link {
            let mut presence = other_side.watch_keys(&reiny::engine::Key::launch(&domain, None))?;
            Some(
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if let Some(peer) = link.host().peer() {
                            return Ok::<_, anyhow::Error>(peer);
                        }
                        presence
                            .recv()
                            .await
                            .context("link presence stream closed before peer Hello")?;
                    }
                })
                .await
                .context("waiting for link peer Hello")??,
            )
        } else {
            None
        };
        let schemas = bridge_schemas(&zenoh_side, &other_side, &bindings, peer.as_ref()).await?;
        let _bridge = reiny::bridge::forward(&zenoh_side, &other_side, &schemas)?;
        zenoh_side.ready()?;
        other_side.ready()?;
        tracing::info!(%id, %domain, "bridge up; Ctrl+C to stop");
        tokio::select! {
            signal = tokio::signal::ctrl_c() => signal.context("waiting for Ctrl+C")?,
            () = zenoh_side.shutdown() => {}
        }
        Ok(())
    })
}

fn paired(a: &EndpointContract, b: &EndpointContract) -> bool {
    a.type_name == b.type_name && a.kind == b.kind && a.response == b.response
}

fn validate_bridge_ports(bindings: &ModuleBindings) -> Result<()> {
    ensure!(
        !bindings.inputs.is_empty() && !bindings.outputs.is_empty(),
        "bridge requires paired declared inputs and outputs"
    );
    for (name, input) in &bindings.inputs {
        ensure!(
            bindings
                .outputs
                .get(name)
                .is_some_and(|output| paired(&input.contract, &output.contract)),
            "bridge input has no matching declared output"
        );
    }
    for (name, output) in &bindings.outputs {
        ensure!(
            bindings
                .inputs
                .get(name)
                .is_some_and(|input| paired(&input.contract, &output.contract)),
            "bridge output has no matching declared input"
        );
    }
    Ok(())
}

fn merge_schema(schemas: &mut BTreeMap<String, u64>, name: &str, schema: u64) -> Result<()> {
    if let Some(previous) = schemas.get(name) {
        ensure!(
            *previous == schema,
            "native peers disagree on schema fingerprint for {name}"
        );
    } else {
        schemas.insert(name.into(), schema);
    }
    Ok(())
}

async fn bridge_schemas(
    zenoh: &ModuleContext,
    other: &ModuleContext,
    bindings: &ModuleBindings,
    peer: Option<&(String, Vec<PeerType>)>,
) -> Result<BTreeMap<String, u64>> {
    let mut schemas = BTreeMap::new();
    for input in bindings.inputs.values() {
        let contract = &input.contract;
        let mut descriptors = zenoh.schemas(topic_type(contract)).await?;
        if peer.is_none() {
            descriptors.extend(other.schemas(topic_type(contract)).await?);
        }
        for source in &input.sources {
            let from_link = peer.filter(|(id, _)| id == &source.namespace);
            if let Some((_, types)) = from_link {
                merge_peer_schema(&mut schemas, contract, source, types)?;
            } else {
                let matches: Vec<_> = descriptors
                    .iter()
                    .filter(|descriptor| {
                        descriptor.source == source.namespace
                            && descriptor.source_port == source.port
                            && descriptor.message == contract.type_name
                    })
                    .collect();
                ensure!(
                    !matches.is_empty(),
                    "no descriptor from declared source {}:{} for {}",
                    source.namespace,
                    source.port,
                    contract.type_name
                );
                for descriptor in matches {
                    merge_schema(
                        &mut schemas,
                        &contract.type_name,
                        schema_fingerprint(&descriptor.file_set, &descriptor.message)?,
                    )?;
                }
            }
        }
        if let Some(response) = &contract.response {
            let response_type = response.as_str();
            descriptors.extend(zenoh.schemas(response_type).await?);
            if peer.is_none() {
                descriptors.extend(other.schemas(response_type).await?);
            }
            for descriptor in descriptors.iter().filter(|descriptor| {
                descriptor.message == *response
                    && input.sources.iter().any(|source| {
                        source.namespace == descriptor.source
                            && source.port == descriptor.source_port
                    })
            }) {
                merge_schema(
                    &mut schemas,
                    response,
                    schema_fingerprint(&descriptor.file_set, response)?,
                )?;
            }
            ensure!(
                schemas.contains_key(response),
                "no schema metadata for declared RPC response {response}"
            );
        }
        // A descriptor for a Link-backed type must agree with its negotiated Hello fingerprint.
        for descriptor in descriptors.iter().filter(|descriptor| {
            descriptor.message == contract.type_name
                && input.sources.iter().any(|source| {
                    source.namespace == descriptor.source && source.port == descriptor.source_port
                })
        }) {
            merge_schema(
                &mut schemas,
                &contract.type_name,
                schema_fingerprint(&descriptor.file_set, &descriptor.message)?,
            )?;
        }
    }
    Ok(schemas)
}

fn merge_peer_schema(
    schemas: &mut BTreeMap<String, u64>,
    contract: &EndpointContract,
    source: &EndpointAddress,
    types: &[PeerType],
) -> Result<()> {
    let role = if contract.kind == PortKind::Rpc {
        reiny_link::wire::flags::SERVE
    } else {
        reiny_link::wire::flags::PUB
    };
    let ty = types
        .iter()
        .find(|ty| ty.port == source.port && ty.flags & role != 0)
        .context("link peer Hello lacks the declared source output")?;
    ensure!(
        ty.name == contract.type_name
            && ty.hash == reiny_link::wire::endpoint_hash(&source.namespace, &source.port),
        "link peer endpoint address or type differs from its declared source"
    );
    merge_schema(schemas, &contract.type_name, ty.schema)?;
    if let Some(response) = &contract.response {
        let (name, schema) = ty
            .response
            .as_ref()
            .context("link peer Hello lacks the declared RPC response contract")?;
        ensure!(
            name == response,
            "link peer response type differs from its declared contract"
        );
        merge_schema(schemas, response, *schema)?;
    }
    Ok(())
}

fn link_engine<T: reiny_link::transport::Transport + 'static>(
    id: &str,
    domain: &str,
    transport: T,
) -> Result<LinkEngine> {
    let host = Host::bridge(id, transport).map_err(|e| anyhow::anyhow!("link: {e}"))?;
    Ok(LinkEngine::spawn(host, domain))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn bindings() -> ModuleBindings {
        serde_yaml::from_str(
            "version: 3\nnamespace: bridge\nendpoint_namespace: bridge\ninputs:\n  state:\n    contract: {type: e2e.Probe}\n    sources: [{namespace: mcu, port: sensor}]\noutputs:\n  state:\n    contract: {type: e2e.Probe}\n",
        ).unwrap()
    }

    #[test]
    fn bridge_requires_matching_named_directions() {
        let mut bindings = bindings();
        validate_bridge_ports(&bindings).unwrap();
        bindings
            .outputs
            .get_mut("state")
            .unwrap()
            .contract
            .type_name = "e2e.Other".into();
        assert!(validate_bridge_ports(&bindings).is_err());
        bindings
            .outputs
            .get_mut("state")
            .unwrap()
            .contract
            .type_name = "e2e.Probe".into();
        let output = bindings.outputs.remove("state").unwrap();
        bindings.outputs.insert("other".into(), output);
        assert!(validate_bridge_ports(&bindings).is_err());
        bindings.outputs.clear();
        assert!(validate_bridge_ports(&bindings).is_err());
    }

    #[test]
    fn conflicting_native_fingerprints_are_refused() {
        let mut schemas = BTreeMap::new();
        merge_schema(&mut schemas, "e2e.Probe", 42).unwrap();
        merge_schema(&mut schemas, "e2e.Probe", 42).unwrap();
        assert!(merge_schema(&mut schemas, "e2e.Probe", 43).is_err());
        assert_eq!(schemas["e2e.Probe"], 42);
    }

    #[tokio::test]
    async fn link_metadata_must_match_source_type_and_direction() {
        let bindings = bindings();
        let mut options = reiny::RuntimeOptions::new("bridge");
        options.module_bindings = Some(bindings.clone());
        options.engine = Some(Arc::new(reiny::engine::Local::new()));
        let zenoh = ModuleContext::open(options).await.unwrap();
        let other = zenoh
            .with_engine(Arc::new(reiny::engine::Local::new()))
            .await
            .unwrap();
        let mut peer = (
            "mcu".into(),
            vec![PeerType {
                hash: reiny_link::wire::endpoint_hash("mcu", "sensor"),
                name: "e2e.Probe".into(),
                port: "sensor".into(),
                response: None,
                sources: Vec::new(),
                flags: reiny_link::wire::flags::PUB,
                schema: 42,
            }],
        );
        let mut incoming = peer.1[0].clone();
        incoming.flags = reiny_link::wire::flags::SUB;
        peer.1.insert(0, incoming);
        let schemas = bridge_schemas(&zenoh, &other, &bindings, Some(&peer))
            .await
            .unwrap();
        assert_eq!(schemas["e2e.Probe"], 42);
        peer.1.remove(0);
        peer.1[0].hash ^= 1;
        assert!(
            bridge_schemas(&zenoh, &other, &bindings, Some(&peer))
                .await
                .is_err()
        );
        peer.1[0].hash = reiny_link::wire::endpoint_hash("mcu", "sensor");
        peer.1[0].port = "unwired".into();
        assert!(
            bridge_schemas(&zenoh, &other, &bindings, Some(&peer))
                .await
                .is_err()
        );
        peer.1[0].port = "sensor".into();
        let mut rpc = bindings.clone();
        let contract = &mut rpc.inputs.get_mut("state").unwrap().contract;
        contract.kind = PortKind::Rpc;
        contract.response = Some("e2e.Response".into());
        peer.1[0].flags = reiny_link::wire::flags::SERVE;
        assert!(
            bridge_schemas(&zenoh, &other, &rpc, Some(&peer))
                .await
                .is_err()
        );
        peer.1[0].response = Some(("e2e.Response".into(), 7));
        let schemas = bridge_schemas(&zenoh, &other, &rpc, Some(&peer))
            .await
            .unwrap();
        assert_eq!(schemas["e2e.Probe"], 42);
        assert_eq!(schemas["e2e.Response"], 7);
        peer.1[0].response = Some(("e2e.Other".into(), 7));
        assert!(
            bridge_schemas(&zenoh, &other, &rpc, Some(&peer))
                .await
                .is_err()
        );
        peer.1[0].response = Some(("e2e.Response".into(), 7));
        let mut metadata_only = bindings.clone();
        metadata_only
            .inputs
            .get_mut("state")
            .unwrap()
            .contract
            .type_name = "e2e.Response".into();
        assert!(
            bridge_schemas(&zenoh, &other, &metadata_only, Some(&peer))
                .await
                .is_err()
        );
        peer.1[0].flags = reiny_link::wire::flags::SUB;
        assert!(
            bridge_schemas(&zenoh, &other, &bindings, Some(&peer))
                .await
                .is_err()
        );
        peer.1.clear();
        assert!(
            bridge_schemas(&zenoh, &other, &bindings, Some(&peer))
                .await
                .is_err()
        );
        peer.0 = "unwired".into();
        assert!(
            bridge_schemas(&zenoh, &other, &bindings, Some(&peer))
                .await
                .is_err()
        );
    }
}
