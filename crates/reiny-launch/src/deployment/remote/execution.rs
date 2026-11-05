//! OpenSSH invocation and its bounded bootstrap-release gate.

use super::{
    SshTarget,
    verification::{known_hosts_option, quote},
};
use anyhow::{Context, Result, ensure};
use std::{
    io::{Read, Write},
    path::Path,
    process::Command,
    sync::mpsc,
};

const VERIFIED: &[u8] = b"REINY_VERIFIED\n";

impl SshTarget {
    pub(in crate::deployment) fn command(
        &self,
        host: &crate::PreparedComponentHost,
        prepared: &crate::PreparedDeployment,
        assignments: &mut [reiny_core::host_protocol::ComponentAssignment],
        directory: &Path,
        local_port: u16,
    ) -> Result<(Command, String)> {
        let key = self.validate(host)?;
        let (script, target_digest) = self.script(self.mappings(host, prepared, assignments)?)?;
        // Do not interpret the verification program in the account's configured
        // login shell (which may be fish). Replace it with an explicit POSIX shell,
        // which in turn execs only the verified native executable.
        let remote_command = format!("exec /bin/sh -c {}", quote(&script)?);
        ensure!(
            remote_command.len() <= 96 * 1024,
            "SSH verification command exceeds argument bound"
        );
        let known_hosts = directory.join("known_hosts");
        let known_hosts_argument = known_hosts_option(&known_hosts)?;
        std::fs::write(
            &known_hosts,
            format!("[{}]:{} {key}\n", self.address, self.port),
        )?;
        let mut command = Command::new(&self.ssh);
        command
            .args([
                "-F",
                "/dev/null",
                "-T",
                "-oBatchMode=yes",
                "-oStrictHostKeyChecking=yes",
                "-oGlobalKnownHostsFile=/dev/null",
                "-oUpdateHostKeys=no",
                "-oIdentitiesOnly=yes",
                "-oIdentityAgent=none",
                "-oPasswordAuthentication=no",
                "-oKbdInteractiveAuthentication=no",
                "-oForwardAgent=no",
                "-oForwardX11=no",
                "-oPermitLocalCommand=no",
                "-oControlMaster=no",
                "-oControlPath=none",
                "-oExitOnForwardFailure=yes",
                "-oServerAliveInterval=5",
                "-oServerAliveCountMax=1",
            ])
            .arg(known_hosts_argument)
            .arg("-i")
            .arg(&self.identity_file)
            .arg("-p")
            .arg(self.port.to_string())
            .arg("-l")
            .arg(&self.user)
            .arg("-R")
            .arg(format!(
                "127.0.0.1:{}:127.0.0.1:{local_port}",
                self.control_port
            ))
            .arg("--")
            .arg(&self.address)
            .arg(remote_command);
        Ok((command, target_digest))
    }
}

/// The reader owns no process. Its pipe closes when the owned SSH client is
/// reaped. Sessions must be dropped only after their local children are reaped.
pub(in crate::deployment) struct Gate {
    incoming: mpsc::Receiver<Result<()>>,
    reader: Option<std::thread::JoinHandle<()>>,
    bootstrap: Option<(std::io::PipeWriter, Vec<u8>)>,
    pub(in crate::deployment) verified: bool,
}

impl Gate {
    pub(in crate::deployment) fn new(
        mut stdout: std::io::PipeReader,
        stdin: std::io::PipeWriter,
        bootstrap: Vec<u8>,
    ) -> Self {
        let (sender, incoming) = mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            let result = (|| {
                let mut bytes = vec![0; VERIFIED.len()];
                stdout
                    .read_exact(&mut bytes)
                    .context("remote verification EOF")?;
                ensure!(bytes == VERIFIED, "remote verification marker mismatch");
                Ok(())
            })();
            let _ = sender.send(result);
        });
        Self {
            incoming,
            reader: Some(reader),
            bootstrap: Some((stdin, bootstrap)),
            verified: false,
        }
    }

    pub(in crate::deployment) fn poll(&mut self) -> Result<()> {
        if self.bootstrap.is_none() {
            return Ok(());
        }
        match self.incoming.try_recv() {
            Ok(result) => {
                result?;
                if let Some(reader) = self.reader.take() {
                    reader
                        .join()
                        .map_err(|_| anyhow::anyhow!("SSH verification reader panicked"))?;
                }
                let (mut writer, bytes) = self.bootstrap.take().context("missing SSH bootstrap")?;
                writer.write_all(&bytes)?;
                self.verified = true;
                Ok(())
            }
            Err(mpsc::TryRecvError::Empty) => Ok(()),
            Err(mpsc::TryRecvError::Disconnected) => {
                anyhow::bail!("SSH verification reader disappeared")
            }
        }
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.bootstrap = None;
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
