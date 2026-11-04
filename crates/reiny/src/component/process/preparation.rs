use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::AsyncReadExt;

use super::protocol::{ComponentAssignment, FactoryRegistration, FailureCode, Prepare};
use super::{ProcessError, ProcessLimits};
use crate::component::{ComponentInstanceSpec, HostOptions};

pub(super) async fn prepare(
    request: Prepare,
    required: &[FactoryRegistration],
    limits: ProcessLimits,
) -> Result<(HostOptions, Vec<ComponentInstanceSpec>), ProcessError> {
    if request.assignments.is_empty() || request.assignments.len() > limits.instances {
        return Err(ProcessError::new(FailureCode::Assignment));
    }
    let factories: BTreeSet<_> = request
        .assignments
        .iter()
        .map(|assignment| &assignment.implementation)
        .collect();
    if factories != required.iter().collect() {
        return Err(ProcessError::new(FailureCode::Factory));
    }
    let mut namespaces = BTreeSet::new();
    for assignment in &request.assignments {
        if !namespaces.insert(&assignment.bindings.namespace) {
            return Err(ProcessError::new(FailureCode::Assignment));
        }
    }
    let mut instances = Vec::with_capacity(request.assignments.len());
    for assignment in request.assignments {
        instances.push(instance(assignment).await?);
    }
    let (domain, engine) = engine(request.transport).await?;
    Ok((
        HostOptions {
            domain,
            engine,
            startup_timeout: limits.startup,
            stop_timeout: limits.shutdown,
        },
        instances,
    ))
}

async fn instance(assignment: ComponentAssignment) -> Result<ComponentInstanceSpec, ProcessError> {
    let mut config = None;
    let mut config_origin = None;
    if let Some(origin) = assignment.config_origin {
        let directory = PathBuf::from(origin.directory);
        if !directory.is_absolute() {
            return Err(ProcessError::new(FailureCode::Assignment));
        }
        let directory = tokio::fs::canonicalize(directory)
            .await
            .map_err(|_| ProcessError::new(FailureCode::Assignment))?;
        if !tokio::fs::metadata(&directory)
            .await
            .map_err(|_| ProcessError::new(FailureCode::Assignment))?
            .is_dir()
        {
            return Err(ProcessError::new(FailureCode::Assignment));
        }
        if let Some(file) = origin.config_file {
            let path = Path::new(&file);
            if !path.is_absolute() {
                return Err(ProcessError::new(FailureCode::Assignment));
            }
            let path = tokio::fs::canonicalize(path)
                .await
                .map_err(|_| ProcessError::new(FailureCode::Assignment))?;
            if !path.starts_with(&directory) {
                return Err(ProcessError::new(FailureCode::Assignment));
            }
            if !tokio::fs::metadata(&path)
                .await
                .map_err(|_| ProcessError::new(FailureCode::Assignment))?
                .is_file()
            {
                return Err(ProcessError::new(FailureCode::Assignment));
            }
            let file = tokio::fs::File::open(&path)
                .await
                .map_err(|_| ProcessError::new(FailureCode::Assignment))?;
            let mut bytes = Vec::new();
            let limit = u64::from(super::protocol::MAX_FRAME_BYTES);
            file.take(limit + 1)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| ProcessError::new(FailureCode::Assignment))?;
            if u64::try_from(bytes.len()).map_or(true, |length| length > limit) {
                return Err(ProcessError::new(FailureCode::Assignment));
            }
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| ProcessError::new(FailureCode::Assignment))?;
            config = Some(
                match path.extension().and_then(|extension| extension.to_str()) {
                    Some("toml") => text
                        .parse::<toml::Table>()
                        .map_err(|_| ProcessError::new(FailureCode::Assignment))?,
                    Some("json") => super::codec::decode(text.as_bytes())?,
                    _ => serde_yaml::from_str(text)
                        .map_err(|_| ProcessError::new(FailureCode::Assignment))?,
                },
            );
        }
        config_origin = Some(directory);
    }
    Ok(ComponentInstanceSpec {
        factory_id: assignment.implementation.factory,
        bindings: assignment.bindings,
        config,
        config_origin,
    })
}

async fn engine(
    transport: super::protocol::TransportSettings,
) -> Result<(String, Arc<dyn crate::engine::Engine>), ProcessError> {
    #[cfg(feature = "zenoh")]
    {
        let super::protocol::TransportSettings::Zenoh {
            domain,
            connect,
            listen,
            multicast_scouting,
        } = transport;
        crate::validate_segment("domain", &domain)
            .map_err(|_| ProcessError::new(FailureCode::Assignment))?;
        let mut config = zenoh::Config::default();
        for (key, value) in [
            ("connect/endpoints", serde_json::to_string(&connect)),
            ("listen/endpoints", serde_json::to_string(&listen)),
            (
                "scouting/multicast/enabled",
                serde_json::to_string(&multicast_scouting),
            ),
        ] {
            config
                .insert_json5(key, &value.map_err(ProcessError::json)?)
                .map_err(|_| ProcessError::new(FailureCode::ContextOpen))?;
        }
        let engine = crate::engine::Zenoh::open(config)
            .await
            .map_err(|_| ProcessError::new(FailureCode::ContextOpen))?;
        Ok((domain, Arc::new(engine)))
    }
    #[cfg(not(feature = "zenoh"))]
    {
        let super::protocol::TransportSettings::Zenoh { .. } = transport;
        std::future::ready(Err(ProcessError::new(FailureCode::ContextOpen))).await
    }
}
