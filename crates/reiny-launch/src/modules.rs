//! Strict module manifests and engine-neutral endpoint resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use reiny_core::bindings::{
    EndpointAddress, EndpointContract, InputBinding, MODULE_BINDINGS_VERSION,
    MODULE_MANIFEST_VERSION, ModuleBindings, OutputBinding, PortDelegation, PortKind, QosProfile,
    ReceiveBuffer, Replay, Retention,
};
use serde::{Deserialize, Serialize};

use crate::acquire::{GitSource, SourceResolver};
use crate::artifacts::BuildSpec;

mod bindings;
mod connect;
mod hosted;
mod load;
mod model;
mod placement;
mod plan;
mod types;
mod validation;

pub(crate) use hosted::{
    ComponentHost, ComponentHostSpec, ComponentModule, ComponentSpec, ComponentTransport,
};
pub use model::*;
pub use plan::check_manifest;
pub use types::*;
use validation::{validate_definition, validate_manifest};

#[derive(Clone)]
struct ResolvedRoute {
    source: PortAddress,
    via: Vec<PortAddress>,
    contract: EndpointContract,
}

impl std::fmt::Display for PortAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.namespace,
            if self.input { "in" } else { "out" },
            self.name
        )
    }
}

struct Instance {
    dir: PathBuf,
    manifest: ModuleManifest,
    providers: BTreeMap<String, ProviderSpec>,
}

struct Loader {
    resolver: SourceResolver,
    instances: BTreeMap<String, Instance>,
    source_stack: Vec<PathBuf>,
}

fn manifest_dir(path: &Path) -> Result<PathBuf, ModuleError> {
    let path = if path.is_file() {
        if path.file_name().is_none_or(|name| name != "main.yaml") {
            return Err(invalid(
                path.display().to_string(),
                "expected main.yaml or its directory",
            ));
        }
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    } else {
        path
    };
    std::fs::canonicalize(path).map_err(|source| ModuleError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_manifest(dir: &Path) -> Result<ModuleManifest, ModuleError> {
    let path = dir.join("main.yaml");
    let text = std::fs::read_to_string(&path).map_err(|source| ModuleError::Io {
        path: path.clone(),
        source,
    })?;
    serde_yaml::from_str(&text).map_err(|source| ModuleError::Yaml { path, source })
}

fn invalid(module: impl Into<String>, message: impl Into<String>) -> ModuleError {
    ModuleError::Invalid {
        module: module.into(),
        message: message.into(),
    }
}

fn validate_name(name: &str, kind: &str) -> Result<(), ModuleError> {
    if name.is_empty()
        || name == "in"
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(invalid(
            name,
            format!("invalid {kind} name; use letters, digits, '_' or '-'"),
        ));
    }
    Ok(())
}

fn validate_port_names(ports: &BTreeMap<String, PortSpec>) -> Result<(), ModuleError> {
    for name in ports.keys() {
        validate_name(name, "port")?;
    }
    Ok(())
}

fn absolute(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn check_type(port: &str, expected: &PortSpec, actual: &PortConnection) -> Result<(), ModuleError> {
    if actual.type_name.is_empty() || expected.type_name == actual.type_name {
        Ok(())
    } else {
        Err(ModuleError::TypeMismatch {
            port: port.to_string(),
            expected: expected.type_name.clone(),
            actual: actual.type_name.clone(),
        })
    }
}

fn resolve_endpoint(
    address: &PortAddress,
    edges: &BTreeMap<PortAddress, Vec<PortAddress>>,
    terminals: &BTreeMap<PortAddress, EndpointContract>,
    active: &mut BTreeSet<PortAddress>,
    resolved: &mut BTreeMap<PortAddress, Vec<ResolvedRoute>>,
) -> Result<Vec<ResolvedRoute>, ModuleError> {
    if let Some(routes) = resolved.get(address) {
        return Ok(routes.clone());
    }
    let mut routes = Vec::new();
    if let Some(contract) = terminals.get(address) {
        routes.push(ResolvedRoute {
            source: address.clone(),
            via: Vec::new(),
            contract: contract.clone(),
        });
    } else {
        if !active.insert(address.clone()) {
            return Err(ModuleError::Cycle(address.to_string()));
        }
        let sources = edges
            .get(address)
            .ok_or_else(|| invalid(address.to_string(), "port has no executable publisher"))?;
        for source in sources {
            for mut route in resolve_endpoint(source, edges, terminals, active, resolved)? {
                if source != &route.source {
                    route.via.insert(0, source.clone());
                }
                routes.push(route);
            }
        }
        active.remove(address);
    }
    resolved.insert(address.clone(), routes.clone());
    Ok(routes)
}

fn validate_connection(namespace: &str, port: &PortSpec) -> Result<(), ModuleError> {
    if !port.semantics.is_empty()
        || port.kind != PortKind::Stream
        || port.response.is_some()
        || port.qos != QosProfile::Default
        || port.retention != Retention::Volatile
        || port.replay != Replay::Live
        || port.buffer != ReceiveBuffer::Fifo
    {
        return Err(invalid(
            namespace,
            "callers connect app-owned endpoints; they cannot redefine endpoint policy",
        ));
    }
    Ok(())
}

pub(crate) fn validate_contract(
    namespace: &str,
    contract: &EndpointContract,
    input: bool,
) -> Result<(), ModuleError> {
    contract
        .validate(input)
        .map_err(|error| invalid(namespace, error.to_string()))
}

fn check_contract(
    port: &str,
    expected: &EndpointContract,
    actual: &EndpointContract,
) -> Result<(), ModuleError> {
    if !expected.type_name.is_empty()
        && (expected.type_name != actual.type_name
            || expected.kind != actual.kind
            || expected.response != actual.response)
    {
        return Err(ModuleError::TypeMismatch {
            port: port.to_string(),
            expected: format!(
                "{:?} {} {:?}",
                expected.kind, expected.type_name, expected.response
            ),
            actual: format!(
                "{:?} {} {:?}",
                actual.kind, actual.type_name, actual.response
            ),
        });
    }
    Ok(())
}
