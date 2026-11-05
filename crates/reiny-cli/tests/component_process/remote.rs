//! Same-machine Linux SSH fixture: real SSH, `ProcessHost` and named Zenoh traffic.
//! This is not evidence of remote hardware safety or descendant containment.
#![cfg(target_os = "linux")]

use super::fixture;

use anyhow::{Context, Result, ensure};
use fixture::{DEADLINE, Events, Fixture, Hosts};
use reiny::zenoh::{self, Wait};
use reiny_launch::{DeploymentClient, ModuleObserver, PreparedDeployment, SshTarget};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::io::{AsyncBufReadExt, BufReader};

#[path = "remote_pid.rs"]
mod remote_pid;

struct Observer;
impl ModuleObserver for Observer {
    fn ready(&self, _: &str) -> Result<bool> {
        anyhow::bail!("legacy observer invoked")
    }
    fn request_stop(&self, _: &str) -> Result<()> {
        anyhow::bail!("legacy stop invoked")
    }
}

struct Ssh {
    root: tempfile::TempDir,
    child: tokio::process::Child,
    drain: tokio::task::JoinHandle<()>,
    port: u16,
    key: String,
    user: String,
}

impl Ssh {
    async fn start() -> Result<Self> {
        let root = tempfile::tempdir()?;
        for key in ["host", "client", "wrong"] {
            let output = tokio::process::Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(root.path().join(key))
                .output()
                .await?;
            ensure!(output.status.success(), "fixture key generation failed");
        }
        std::fs::copy(
            root.path().join("client.pub"),
            root.path().join("authorized_keys"),
        )?;
        let user = String::from_utf8(
            tokio::process::Command::new("id")
                .arg("-un")
                .output()
                .await?
                .stdout,
        )?
        .trim()
        .to_owned();
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let config = format!(
            "ListenAddress 127.0.0.1\nPort {port}\nHostKey {}\nPidFile {}\nAuthorizedKeysFile {}\nStrictModes no\nPasswordAuthentication no\nKbdInteractiveAuthentication no\nUsePAM no\nAllowUsers {user}\nAllowTcpForwarding yes\nGatewayPorts no\nLogLevel VERBOSE\n",
            root.path().join("host").display(),
            root.path().join("pid").display(),
            root.path().join("authorized_keys").display()
        );
        std::fs::write(root.path().join("sshd_config"), config)?;
        drop(listener);
        let mut child = tokio::process::Command::new("/usr/sbin/sshd")
            .args(["-D", "-e", "-f"])
            .arg(root.path().join("sshd_config"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut lines = BufReader::new(child.stderr.take().context("sshd stderr")?).lines();
        tokio::time::timeout(DEADLINE, async {
            loop {
                let line = lines
                    .next_line()
                    .await?
                    .context("sshd exited before listening")?;
                eprintln!("fixture sshd: {line}");
                if line.contains("Server listening on") {
                    return Ok::<_, anyhow::Error>(());
                }
            }
        })
        .await??;
        let drain = tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("fixture sshd: {line}");
            }
        });
        let key = std::fs::read_to_string(root.path().join("host.pub"))?
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ");
        Ok(Self {
            root,
            child,
            drain,
            port,
            key,
            user,
        })
    }

    fn targets(
        &self,
        prepared: &PreparedDeployment,
        root: &Path,
    ) -> Result<BTreeMap<String, SshTarget>> {
        let mut targets = BTreeMap::new();
        let mut reservations = Vec::new();
        for (name, host) in &prepared.component_hosts {
            let remote = root.join(format!("remote-{name}"));
            let bundle = remote.join("bin");
            fixture::copy_tree(&host.bundle_dir, &bundle)?;
            let mut configs = BTreeMap::new();
            for component in prepared
                .components
                .iter()
                .filter(|component| component.host == *name)
            {
                if let Some(local) = &component.config_bundle_dir {
                    let destination = remote.join(format!("config-{}", configs.len()));
                    fixture::copy_tree(local, &destination)?;
                    configs.insert(local.clone(), destination);
                }
            }
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            let control_port = listener.local_addr()?.port();
            reservations.push(listener);
            let executable = bundle.join(host.executable.file_name().context("host executable")?);
            targets.insert(
                name.clone(),
                SshTarget {
                    ssh: "/usr/bin/ssh".into(),
                    address: "127.0.0.1".into(),
                    user: self.user.clone(),
                    port: self.port,
                    host_key: self.key.clone(),
                    identity_file: self.root.path().join("client"),
                    bundle,
                    executable,
                    working_directory: remote,
                    configs,
                    control_port,
                },
            );
        }
        Ok(targets)
    }

    async fn finish(mut self) -> Result<()> {
        self.child.kill().await?;
        let status = self.child.wait().await?;
        ensure!(
            !status.success(),
            "fixture daemon should have been terminated"
        );
        self.drain.await?;
        Ok(())
    }
}

struct Owner {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<Result<()>>>,
    client: DeploymentClient,
}

impl Owner {
    fn start(prepared: PreparedDeployment, targets: BTreeMap<String, SshTarget>) -> Result<Self> {
        let root = prepared.root.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stop);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            reiny_launch::serve_remote(prepared, targets, &Observer, &signal, &|| {
                sender.send(()).context("owner rendezvous")
            })
        });
        receiver.recv_timeout(DEADLINE)?;
        let client = DeploymentClient::find(&root)?.context("owner endpoint")?;
        Ok(Self {
            stop,
            thread: Some(thread),
            client,
        })
    }

    fn finish(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        let result = self
            .thread
            .take()
            .context("owner thread")?
            .join()
            .map_err(|_| anyhow::anyhow!("owner panicked"))?;
        ensure!(
            result.is_err(),
            "Unknown remote cleanup must not report clean stop"
        );
        Ok(())
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

async fn hub() -> Result<zenoh::Session> {
    let mut config = zenoh::Config::default();
    for (key, value) in [
        ("mode", r#""router""#),
        ("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#),
        ("connect/endpoints", "[]"),
        ("scouting/multicast/enabled", "false"),
    ] {
        config
            .insert_json5(key, value)
            .map_err(anyhow::Error::msg)?;
    }
    zenoh::open(config).await.map_err(anyhow::Error::msg)
}

fn retained(root: &Path) -> Result<()> {
    let records = reiny_launch::remote_ownership(root)?;
    ensure!(!records.is_empty());
    for record in &records {
        assert!(record.local_client_reaped);
        assert_eq!(
            record.tree_cleanup,
            reiny_launch::RemoteTreeCleanup::Unknown
        );
        assert!(record.reconciliation.is_none());
    }
    Ok(())
}

#[path = "remote/cases.rs"]
mod cases;
#[path = "remote/owner_crash.rs"]
mod owner_crash;
