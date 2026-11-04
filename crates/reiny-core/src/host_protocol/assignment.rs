use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use crate::bindings::{MODULE_BINDINGS_VERSION, MODULE_REPORT_VERSION, ModuleBindings};

/// Linked wire contracts; callers require exact equality during authentication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compatibility {
    /// Exact linked Reiny release, not a semver compatibility range.
    pub reiny_release: String,
    /// Resolved binding format.
    pub bindings_version: u32,
    /// Compiled report format.
    pub report_version: u32,
    /// Compiled schema fingerprint algorithm.
    pub fingerprint_version: u16,
    /// Named endpoint key epoch used by the host transport.
    pub named_key_epoch: u16,
}

impl Compatibility {
    /// Current core contracts with the caller's actual transport key epoch.
    #[must_use]
    pub fn current(named_key_epoch: u16) -> Self {
        Self {
            reiny_release: env!("CARGO_PKG_VERSION").into(),
            bindings_version: MODULE_BINDINGS_VERSION,
            report_version: MODULE_REPORT_VERSION,
            fingerprint_version: crate::SCHEMA_FINGERPRINT_VERSION,
            named_key_epoch,
        }
    }
}

/// An allowlisted factory and the exact declared implementation revision.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactoryRegistration {
    /// Application-owned registry identifier.
    pub factory: String,
    /// Exact revision expected by the prepared owner.
    pub revision: String,
}

/// Frozen per-instance paths, never a process-global configuration directory.
///
/// Paths use the target host's filesystem spelling. The caller verifies they
/// reference the prepared bundle; decoding does not authorize filesystem access.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOrigin {
    /// Absolute frozen directory used to resolve this instance's config assets.
    pub directory: String,
    /// Absolute frozen config file, absent if this instance has no config.
    pub config_file: Option<String>,
}

/// One exact assignment. Its namespace comes only from `bindings.namespace`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentAssignment {
    /// Required registered implementation and revision.
    pub implementation: FactoryRegistration,
    /// Owner-resolved endpoint authority, preserved without reconstruction.
    pub bindings: ModuleBindings,
    /// Per-instance frozen config and asset origin, when present.
    pub config_origin: Option<ConfigOrigin>,
}

/// Milliseconds of a remaining budget, not an absolute cross-process clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Milliseconds(pub u64);

/// Explicit application transport selection, independent of control framing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum TransportSettings {
    /// First process-host slice uses Zenoh for every application edge.
    Zenoh {
        /// Exact application communication domain.
        domain: String,
        /// Explicit connection locators.
        connect: Vec<String>,
        /// Explicit listening locators.
        listen: Vec<String>,
        /// Whether multicast discovery is enabled.
        multicast_scouting: bool,
    },
}

/// Construct and prepare assigned components, retaining local admission tickets.
///
/// The host must not run hooks, validate its subset as a closed graph, or infer
/// approval. Emit one compiled report per assigned namespace in lexical order,
/// then `PreparedComplete`. The owner retains the authoritative bindings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    /// Exact assignment set; the caller rejects duplicates or missing instances.
    pub assignments: Vec<ComponentAssignment>,
    /// Application data transport, not the owner control channel.
    pub transport: TransportSettings,
    /// Remaining whole-admission budget at send time.
    pub remaining_budget: Milliseconds,
}
