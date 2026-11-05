//! Explicit Linux/OpenSSH execution. No discovery, upload, restart, or supervisor.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

mod execution;
mod ownership;
mod verification;
pub(super) use execution::Gate;
pub use ownership::{RemoteOwnership, RemoteTreeCleanup, reconcile_remote, remote_ownership};
pub(super) use ownership::{persist, refuse_unresolved};

/// Explicit preinstalled remote files and pinned SSH server identity.
///
/// Linux owners and Linux targets with OpenSSH, `/bin/sh`, GNU find,
/// and sha256sum are supported. Remote files must remain immutable through
/// execution under the same trusted-user convention as local prepared bundles.
/// No files are uploaded and no alternate executable is selected on failure.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SshTarget {
    /// Absolute local OpenSSH client executable.
    pub ssh: PathBuf,
    /// Explicit DNS name or IP address, never an SSH configuration alias.
    pub address: String,
    /// Explicit remote account.
    pub user: String,
    /// SSH server port.
    pub port: u16,
    /// One pinned public server key: algorithm and base64 material only.
    pub host_key: String,
    /// Absolute local private authentication key; agent/default identities disabled.
    pub identity_file: PathBuf,
    /// Absolute remote directory containing the exact frozen executable bundle.
    pub bundle: PathBuf,
    /// Absolute remote executable, required to match the prepared relative name.
    pub executable: PathBuf,
    /// Explicit remote working directory.
    pub working_directory: PathBuf,
    /// Frozen owner config-bundle roots mapped to preinstalled remote roots.
    pub configs: BTreeMap<PathBuf, PathBuf>,
    /// Reserved remote loopback control forwarding port; conflicts fail closed.
    pub control_port: u16,
}
