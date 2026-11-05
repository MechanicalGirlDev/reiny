//! Convert verified frozen inputs without widening transport or endpoint authority.

use anyhow::{Context, Result, ensure};
use reiny_core::host_protocol::{
    ComponentAssignment, ConfigOrigin, FactoryRegistration, TransportSettings,
};
use serde::Deserialize;

use crate::prepared::{PreparedComponent, PreparedComponentHost};

use super::codec;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransportConfig {
    connect: Endpoints,
    listen: Endpoints,
    scouting: Scouting,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoints {
    endpoints: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scouting {
    multicast: Multicast,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Multicast {
    enabled: bool,
}

pub(super) fn transport(
    host: &PreparedComponentHost,
    prepared: &crate::PreparedDeployment,
) -> Result<TransportSettings> {
    let placement = prepared
        .components
        .iter()
        .map(|component| (component.namespace.clone(), component.host.clone()))
        .collect();
    if host.spec.transport == crate::ComponentTransport::Local {
        return Ok(TransportSettings::Local {
            domain: prepared.domain.clone(),
            placement,
        });
    }
    let (mut connect, listen, multicast_scouting) = match &host.provider.zenoh_config {
        Some(path) => {
            let bytes = std::fs::read(path)?;
            let config: TransportConfig = codec::decode(&bytes).context(
                "hosted zenoh_config requires explicit connect/endpoints, listen/endpoints and scouting/multicast/enabled only; other settings cannot be preserved by host protocol v1",
            )?;
            (
                config.connect.endpoints,
                config.listen.endpoints,
                config.scouting.multicast.enabled,
            )
        }
        None => (Vec::new(), Vec::new(), false),
    };
    if !host.provider.connect.is_empty() {
        connect.clone_from(&host.provider.connect);
    }
    ensure!(
        connect.len() <= 1024 && listen.len() <= 1024,
        "too many hosted transport endpoints"
    );
    Ok(TransportSettings::RoutedZenoh {
        domain: prepared.domain.clone(),
        connect,
        listen,
        multicast_scouting,
        placement,
    })
}

pub(super) fn instance(component: &PreparedComponent) -> Result<ComponentAssignment> {
    let config_origin = component
        .config_dir
        .as_ref()
        .map(|directory| {
            Ok::<_, anyhow::Error>(ConfigOrigin {
                directory: directory
                    .to_str()
                    .context("non-UTF8 hosted config origin")?
                    .to_owned(),
                config_file: component
                    .component
                    .config
                    .as_ref()
                    .map(|file| {
                        file.to_str()
                            .context("non-UTF8 hosted config path")
                            .map(str::to_owned)
                    })
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(ComponentAssignment {
        implementation: FactoryRegistration {
            factory: component.component.factory.clone(),
            revision: component.component.factory_revision.clone(),
        },
        bindings: component.bindings.clone(),
        config_origin,
    })
}
