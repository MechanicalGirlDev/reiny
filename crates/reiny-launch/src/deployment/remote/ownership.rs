//! Durable observations, never PID authority or a remote cleanup certificate.

use std::path::Path;

use anyhow::{Context, Result, ensure};
use reiny_core::host_protocol::{HostOutcome, SessionIdentity};
use serde::{Deserialize, Serialize};

use super::super::{atomic_json, digest, lock_file, private_directory};

/// Remote descendant cleanup cannot be established by an SSH command exit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RemoteTreeCleanup {
    /// No remote process-tree containment/reaping proof exists.
    Unknown,
}

/// One durable remote effect reservation, written before SSH is spawned.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemoteOwnership {
    /// Exact graph and host incarnation, including prepared digests.
    pub identity: SessionIdentity,
    /// Digest of explicit SSH identity, paths and verified remote file manifest.
    pub target_digest: String,
    /// Only the locally owned SSH client tree was reaped.
    pub local_client_reaped: bool,
    /// Authenticated component task outcome, not process containment.
    pub cooperative_outcome: Option<HostOutcome>,
    /// SSH-reported native command exit; absent for transport error or forced kill.
    pub remote_command_exit: Option<i32>,
    /// Independent remote descendant cleanup evidence.
    pub tree_cleanup: RemoteTreeCleanup,
    /// Exact-generation operator release note, never cleanup proof.
    pub reconciliation: Option<String>,
}

fn key(identity: &SessionIdentity) -> Result<String> {
    Ok(digest(&serde_json::to_vec(identity)?))
}

/// Read retained records, including operator-reconciled Unknown generations.
pub fn remote_ownership(root: &Path) -> Result<Vec<RemoteOwnership>> {
    let directory = root.join(".reiny/remote-ownership");
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry?;
        // Interrupted temporary writes are not published reservations.
        if entry.file_name().to_string_lossy().starts_with(".tmp") {
            continue;
        }
        ensure!(
            entry.file_type()?.is_file(),
            "invalid remote ownership entry"
        );
        let record: RemoteOwnership = serde_json::from_slice(&std::fs::read(entry.path())?)?;
        ensure!(
            entry.file_name()
                == std::ffi::OsString::from(format!("{}.json", key(&record.identity)?)),
            "remote ownership identity mismatch"
        );
        records.push(record);
    }
    Ok(records)
}

pub(in crate::deployment) fn refuse_unresolved(root: &Path) -> Result<()> {
    ensure!(
        remote_ownership(root)?
            .iter()
            .all(|record| record.reconciliation.is_some()),
        "remote cleanup Unknown: exact-generation operator reconciliation required"
    );
    Ok(())
}

pub(in crate::deployment) fn persist(
    root: &Path,
    record: &RemoteOwnership,
    new: bool,
) -> Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        "durable remote ownership currently requires Linux"
    );
    let cache = root.join(".reiny");
    let directory = cache.join("remote-ownership");
    private_directory(&directory)?;
    // Make creation of the reservation directory durable before remote effects.
    std::fs::File::open(&cache)?.sync_all()?;
    std::fs::File::open(root)?.sync_all()?;
    let path = directory.join(format!("{}.json", key(&record.identity)?));
    if new {
        ensure!(!path.exists(), "remote generation already reserved");
    } else {
        let old: RemoteOwnership = serde_json::from_slice(&std::fs::read(&path)?)?;
        ensure!(
            old.identity == record.identity && old.target_digest == record.target_digest,
            "remote ownership replacement mismatch"
        );
    }
    atomic_json(&path, record)?;
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

/// Release the replacement prohibition for one exact generation.
///
/// Requires the local owner to have stopped. The retained record remains Unknown;
/// this is an operator decision to proceed, not evidence that remote work stopped.
pub fn reconcile_remote(
    root: &Path,
    identity: &SessionIdentity,
    target_digest: &str,
    note: &str,
) -> Result<()> {
    ensure!(
        !note.trim().is_empty(),
        "operator reconciliation requires a note"
    );
    let root = root.canonicalize()?;
    let _lock = lock_file(&root.join(".reiny/owner.lock"))?;
    let mut record = remote_ownership(&root)?
        .into_iter()
        .find(|record| record.identity == *identity)
        .context("unknown remote generation")?;
    ensure!(
        record.target_digest == target_digest,
        "remote target identity mismatch"
    );
    ensure!(
        record.reconciliation.is_none(),
        "generation already reconciled"
    );
    record.reconciliation = Some(note.to_owned());
    persist(&root, &record, false)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use reiny_core::host_protocol::{
        GraphGeneration, HostGeneration, HostId, OwnerGeneration, PreparedDigest,
    };

    fn record() -> RemoteOwnership {
        RemoteOwnership {
            identity: SessionIdentity {
                owner_generation: OwnerGeneration(1),
                graph_generation: GraphGeneration(2),
                host_id: HostId("remote".into()),
                host_generation: HostGeneration(3),
                graph_digest: PreparedDigest("graph".into()),
                bundle_digest: PreparedDigest("bundle".into()),
            },
            target_digest: "target".into(),
            local_client_reaped: false,
            cooperative_outcome: None,
            remote_command_exit: None,
            tree_cleanup: RemoteTreeCleanup::Unknown,
            reconciliation: None,
        }
    }

    #[test]
    fn exact_reconciliation_retains_unknown_and_excludes_live_owner() -> Result<()> {
        let root = tempfile::tempdir()?;
        private_directory(&root.path().join(".reiny"))?;
        let record = record();
        persist(root.path(), &record, true)?;
        assert_eq!(remote_ownership(root.path())?, vec![record.clone()]);
        assert!(refuse_unresolved(root.path()).is_err());
        assert!(persist(root.path(), &record, true).is_err());
        let mut wrong = record.identity.clone();
        wrong.host_generation.0 += 1;
        assert!(reconcile_remote(root.path(), &wrong, "target", "inspected").is_err());
        assert!(reconcile_remote(root.path(), &record.identity, "wrong", "inspected").is_err());
        assert!(reconcile_remote(root.path(), &record.identity, "target", " ").is_err());
        let lock = lock_file(&root.path().join(".reiny/owner.lock"))?;
        assert!(reconcile_remote(root.path(), &record.identity, "target", "inspected").is_err());
        drop(lock);
        reconcile_remote(
            root.path(),
            &record.identity,
            "target",
            "operator accepts unresolved remote work",
        )?;
        refuse_unresolved(root.path())?;
        let retained = remote_ownership(root.path())?.remove(0);
        assert_eq!(retained.tree_cleanup, RemoteTreeCleanup::Unknown);
        assert!(!retained.local_client_reaped);
        assert!(retained.cooperative_outcome.is_none());
        assert!(retained.remote_command_exit.is_none());
        Ok(())
    }

    #[test]
    fn corrupt_retained_record_fails_closed() -> Result<()> {
        let root = tempfile::tempdir()?;
        private_directory(&root.path().join(".reiny"))?;
        let record = record();
        persist(root.path(), &record, true)?;
        std::fs::write(
            root.path()
                .join(".reiny/remote-ownership")
                .join(format!("{}.json", key(&record.identity)?)),
            b"{",
        )?;
        assert!(refuse_unresolved(root.path()).is_err());
        assert!(reconcile_remote(root.path(), &record.identity, "target", "inspected").is_err());
        Ok(())
    }
}
