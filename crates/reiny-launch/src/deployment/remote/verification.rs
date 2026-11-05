//! Exact target, path and frozen-tree verification before bootstrap.

use super::super::digest;
use super::SshTarget;
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::{Path, PathBuf},
};

pub(super) fn quote(value: &str) -> Result<String> {
    ensure!(
        !value.contains(['\0', '\n', '\r']),
        "unsupported SSH string"
    );
    Ok(format!("'{}'", value.replace('\'', "'\\''")))
}

fn path(value: &Path) -> Result<String> {
    ensure!(value.is_absolute(), "SSH paths must be absolute");
    quote(value.to_str().context("SSH path must be UTF-8")?)
}

pub(super) fn known_hosts_option(value: &Path) -> Result<String> {
    let value = value.to_str().context("known-hosts path must be UTF-8")?;
    // OpenSSH expands tokens/environment variables even inside quoted config
    // values. Reject those spellings rather than accidentally reading another
    // trust file. Quotes preserve spaces as part of this one filename.
    ensure!(
        !value.contains(['%', '$', '"', '\\', '\0', '\n', '\r']),
        "known-hosts path contains unsupported SSH configuration characters"
    );
    Ok(format!("-oUserKnownHostsFile=\"{value}\""))
}

fn files(directory: &Path, at: &Path, result: &mut BTreeMap<PathBuf, String>) -> Result<()> {
    for entry in std::fs::read_dir(at)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        ensure!(!kind.is_symlink(), "remote bundles cannot contain symlinks");
        if kind.is_dir() {
            files(directory, &entry.path(), result)?;
        } else {
            ensure!(kind.is_file(), "remote bundles must contain regular files");
            result.insert(
                entry.path().strip_prefix(directory)?.to_owned(),
                digest(&std::fs::read(entry.path())?),
            );
        }
    }
    Ok(())
}

impl SshTarget {
    pub(super) fn validate(&self, host: &crate::PreparedComponentHost) -> Result<String> {
        ensure!(
            cfg!(target_os = "linux"),
            "SSH process hosts currently require Linux"
        );
        ensure!(
            self.port != 0 && self.control_port != 0,
            "SSH ports must be nonzero"
        );
        ensure!(
            !self.address.is_empty()
                && !self.address.starts_with('-')
                && self
                    .address
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".:-".contains(&b))
                && !self.user.is_empty()
                && !self.user.starts_with('-')
                && self
                    .user
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)),
            "invalid explicit SSH address or account"
        );
        let key: Vec<_> = self.host_key.split_whitespace().collect();
        ensure!(
            key.len() == 2
                && matches!(key[0], "ssh-ed25519" | "ecdsa-sha2-nistp256" | "ssh-rsa")
                && key[1]
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"+/=".contains(&b)),
            "expected one pinned SSH public host key"
        );
        path(&self.ssh)?;
        path(&self.identity_file)?;
        ensure!(
            self.executable
                == self
                    .bundle
                    .join(host.executable.strip_prefix(&host.bundle_dir)?),
            "remote executable must be the frozen bundle executable"
        );
        Ok(key.join(" "))
    }

    pub(super) fn mappings(
        &self,
        host: &crate::PreparedComponentHost,
        prepared: &crate::PreparedDeployment,
        assignments: &mut [reiny_core::host_protocol::ComponentAssignment],
    ) -> Result<BTreeMap<PathBuf, PathBuf>> {
        let mut mappings = BTreeMap::from([(host.bundle_dir.clone(), self.bundle.clone())]);
        let mut used_configs = std::collections::BTreeSet::new();
        for assignment in assignments {
            let component = prepared
                .components
                .iter()
                .find(|component| component.namespace == assignment.bindings.namespace)
                .context("remote component missing")?;
            if let Some(bundle) = &component.config_bundle_dir {
                let remote = self
                    .configs
                    .get(bundle)
                    .context("missing explicit remote config bundle mapping")?;
                used_configs.insert(bundle.clone());
                mappings.insert(bundle.clone(), remote.clone());
                let origin = assignment
                    .config_origin
                    .as_mut()
                    .context("missing config origin")?;
                remote
                    .join(Path::new(&origin.directory).strip_prefix(bundle)?)
                    .to_str()
                    .context("remote config UTF-8")?
                    .clone_into(&mut origin.directory);
                if let Some(file) = &mut origin.config_file {
                    remote
                        .join(Path::new(&*file).strip_prefix(bundle)?)
                        .to_str()
                        .context("remote config UTF-8")?
                        .clone_into(file);
                }
            }
        }
        ensure!(
            used_configs.len() == self.configs.len(),
            "unused remote config mapping"
        );
        Ok(mappings)
    }

    pub(super) fn script(&self, mappings: BTreeMap<PathBuf, PathBuf>) -> Result<(String, String)> {
        let mut script = String::from("set -eu; ");
        let mut manifests = BTreeMap::new();
        for (local, remote) in mappings {
            let mut entries = BTreeMap::new();
            files(&local, &local, &mut entries)?;
            ensure!(!entries.is_empty(), "empty remote bundle");
            let remote_q = path(&remote)?;
            // Assignments preserve find's exit status under set -e. A pipeline
            // to a successful counter would hide traversal/permission failures.
            write!(
                script,
                "test -d {remote_q}; special=$(find {remote_q} ! -type d ! -type f -printf x); test -z \"$special\"; regular=$(find {remote_q} -type f -printf x); test \"${{#regular}}\" -eq {}; ",
                entries.len()
            )?;
            for (relative, hash) in &entries {
                let filename = path(&remote.join(relative))?;
                write!(script, "test \"$(sha256sum < {filename})\" = '{hash}  -'; ")?;
            }
            manifests.insert(remote, entries);
        }
        write!(
            script,
            "cd {}; printf 'REINY_VERIFIED\\n'; unset REINY_CONFIG_DIR REINY_MODULE_REPORT; export REINY_BUNDLE_DIR={}; exec {} 1>&2",
            path(&self.working_directory)?,
            path(&self.bundle)?,
            path(&self.executable)?
        )?;
        ensure!(
            script.len() <= 96 * 1024,
            "SSH verification manifest exceeds command bound"
        );
        // No secret belongs in the command line. Bootstrap is gated separately.
        let target_digest = digest(&serde_json::to_vec(&(self, manifests))?);
        Ok((script, target_digest))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    #[test]
    fn pinned_trust_path_is_one_literal_config_value() -> Result<()> {
        assert_eq!(
            known_hosts_option(Path::new("/tmp/project with spaces/known_hosts"))?,
            "-oUserKnownHostsFile=\"/tmp/project with spaces/known_hosts\""
        );
        for value in [
            "/tmp/%h/key",
            "/tmp/${HOME}/key",
            "/tmp/\"/key",
            "/tmp/\\/key",
        ] {
            assert!(known_hosts_option(Path::new(value)).is_err());
        }
        Ok(())
    }

    #[test]
    fn failed_inventory_never_emits_verification_marker() -> Result<()> {
        let root = tempfile::tempdir()?;
        let local = root.path().join("local");
        let remote = root.path().join("remote");
        std::fs::create_dir(&local)?;
        std::fs::create_dir(&remote)?;
        for directory in [&local, &remote] {
            std::fs::write(directory.join("program"), b"fixture bytes")?;
        }
        let hidden = remote.join("unreadable");
        std::fs::create_dir(&hidden)?;
        std::fs::write(hidden.join("extra"), b"unverified")?;
        let target = SshTarget {
            ssh: "/usr/bin/ssh".into(),
            address: "127.0.0.1".into(),
            user: "unused".into(),
            port: 22,
            host_key: String::new(),
            identity_file: root.path().join("unused"),
            executable: remote.join("program"),
            bundle: remote.clone(),
            working_directory: remote.clone(),
            configs: BTreeMap::new(),
            control_port: 1,
        };
        let (script, _) = target.script(BTreeMap::from([(local, remote)]))?;
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o0))?;
        let output = Command::new("/bin/sh").arg("-c").arg(script).output();
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o700))?;
        let output = output?;
        assert!(!output.status.success());
        assert_eq!(output.stdout, [] as [u8; 0]);
        Ok(())
    }
}
