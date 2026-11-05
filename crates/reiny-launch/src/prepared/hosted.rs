use super::{BTreeMap, ModuleBindings, PathBuf, Result, digest};
use crate::{ComponentHostSpec, ComponentSpec, ProviderSpec};
use serde::{Deserialize, Serialize};

/// One frozen native host. Preparation does not authorize process execution.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedComponentHost {
    /// Application working directory of the declaring root.
    pub module_dir: PathBuf,
    /// Frozen host executable, never a fallback to the provider's original binary.
    pub executable: PathBuf,
    /// Content-addressed executable and runtime-library bundle.
    pub bundle_dir: PathBuf,
    /// Resolved executable, build provenance and lifecycle policy.
    pub spec: ComponentHostSpec,
    /// Process provider with frozen transport configuration.
    pub provider: ProviderSpec,
    /// Exact assigned instance namespaces, sorted and unique.
    pub instances: Vec<String>,
    /// Content, transport, build and assignment fingerprint.
    pub fingerprint: String,
}

/// One frozen registered component with its own configuration origin.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedComponent {
    /// Canonical instance namespace.
    pub namespace: String,
    /// Original module working directory.
    pub module_dir: PathBuf,
    /// Factory identity/revision and frozen configuration selection.
    pub component: ComponentSpec,
    /// Exact root host assignment.
    pub host: String,
    /// Frozen per-instance configuration origin.
    pub config_dir: Option<PathBuf>,
    /// Frozen common-ancestor tree of configuration and declared assets.
    pub config_bundle_dir: Option<PathBuf>,
    /// Owner-resolved endpoint authority, never inferred from a factory.
    pub bindings: ModuleBindings,
    /// Content, factory, placement, bindings and domain fingerprint.
    pub fingerprint: String,
}

pub(crate) fn host_fingerprint(
    name: &str,
    host: &PreparedComponentHost,
    domain: &str,
) -> Result<String> {
    Ok(digest(&serde_json::to_vec(&(
        name,
        &host.module_dir,
        &host.executable,
        &host.bundle_dir,
        &host.spec,
        &host.provider,
        &host.instances,
        domain,
    ))?))
}

pub(crate) fn component_fingerprint(
    component: &PreparedComponent,
    host_fingerprint: &str,
    domain: &str,
) -> Result<String> {
    Ok(digest(&serde_json::to_vec(&(
        &component.namespace,
        &component.module_dir,
        &component.component,
        &component.host,
        &component.config_dir,
        &component.config_bundle_dir,
        &component.bindings,
        host_fingerprint,
        domain,
    ))?))
}

pub(super) fn deserialize_hosts<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, PreparedComponentHost>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Hosts;
    impl<'de> serde::de::Visitor<'de> for Hosts {
        type Value = BTreeMap<String, PreparedComponentHost>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("unique prepared host names")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut hosts = BTreeMap::new();
            while let Some((name, host)) = map.next_entry::<String, PreparedComponentHost>()? {
                if hosts.insert(name, host).is_some() {
                    return Err(serde::de::Error::custom("duplicate prepared host"));
                }
            }
            Ok(hosts)
        }
    }
    deserializer.deserialize_map(Hosts)
}
