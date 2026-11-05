//! Explicit component declarations and their resolved placement records.

use super::{
    BTreeMap, BuildSpec, Deserialize, FailurePolicy, ModuleBindings, ModuleError, PathBuf,
    ProviderSpec, RestartPolicy, Serialize, invalid, validate_name,
};

/// One registered application component; this declaration never loads code.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentSpec {
    /// Exact registration identifier expected from the selected host.
    pub factory: String,
    /// Application-defined registration revision expected from the host.
    pub factory_revision: String,
    /// Selected instance configuration, relative to the definition by default.
    #[serde(default)]
    pub config: Option<PathBuf>,
    /// Declared assets relative to the selected configuration's parent.
    #[serde(default)]
    pub config_assets: Vec<PathBuf>,
}

impl ComponentSpec {
    pub(super) fn validate(&self, namespace: &str) -> Result<(), ModuleError> {
        validate_name(&self.factory, "component factory")?;
        if self.factory_revision.trim().is_empty() {
            return Err(invalid(
                namespace,
                "component factory_revision must not be empty",
            ));
        }
        if self.config.is_none() && !self.config_assets.is_empty() {
            return Err(invalid(
                namespace,
                "config_assets requires the selected instance config",
            ));
        }
        Ok(())
    }
}

/// A root-owned native process hosting one or more registered components.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentHostSpec {
    /// Root process provider name; child provider remapping does not affect it.
    pub provider: String,
    /// Logical executable name in that provider's bundle.
    pub bin: String,
    /// Explicit data visibility. Local is isolated from every other host and bus.
    #[serde(default, skip_serializing_if = "ComponentTransport::is_default")]
    pub transport: ComponentTransport,
    /// Optional explicit builder, resolved relative to the root.
    #[serde(default)]
    pub build: Option<BuildSpec>,
    /// Only manual whole-graph recovery is supported for hosted components.
    #[serde(default)]
    pub restart: RestartPolicy,
    /// Behavior after a running host fails.
    #[serde(default)]
    pub on_failure: FailurePolicy,
}

/// Data transport for one process host, independent of its control channel.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComponentTransport {
    /// Placement-aware reads with publications mirrored to Local and Zenoh.
    #[default]
    RoutedZenoh,
    /// An explicitly isolated Local bus, never visible on the network.
    Local,
}

impl ComponentTransport {
    fn is_default(&self) -> bool {
        *self == Self::RoutedZenoh
    }
}

/// A host with resolved root provider and build paths, without executing it.
#[derive(Clone, Debug, Serialize)]
pub struct ComponentHost {
    /// Directory of the declaring deployment root.
    pub module_dir: PathBuf,
    /// Declared executable and lifecycle settings.
    pub spec: ComponentHostSpec,
    /// Resolved process provider and transport settings.
    pub provider: ProviderSpec,
}

/// An explicitly assigned component with unchanged exact endpoint authority.
#[derive(Clone, Debug, Serialize)]
pub struct ComponentModule {
    /// Full canonical instance namespace.
    pub namespace: String,
    /// Directory of the reusable component definition.
    pub module_dir: PathBuf,
    /// Factory identity and resolved per-instance configuration sources.
    pub component: ComponentSpec,
    /// Name of the single assigned root host.
    pub host: String,
    /// App-owned endpoint contracts, with exact source namespaces and ports.
    pub bindings: ModuleBindings,
}

pub(super) fn unique_map<'de, D, V>(
    deserializer: D,
) -> Result<Option<BTreeMap<String, V>>, D::Error>
where
    D: serde::Deserializer<'de>,
    V: Deserialize<'de>,
{
    struct UniqueMap<V>(std::marker::PhantomData<V>);
    impl<'de, V: Deserialize<'de>> serde::de::Visitor<'de> for UniqueMap<V> {
        type Value = BTreeMap<String, V>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a mapping with unique names")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut values = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, V>()? {
                if values.insert(key.clone(), value).is_some() {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate assignment '{key}'"
                    )));
                }
            }
            Ok(values)
        }
    }
    deserializer
        .deserialize_map(UniqueMap(std::marker::PhantomData))
        .map(Some)
}
