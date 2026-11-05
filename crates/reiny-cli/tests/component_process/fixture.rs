use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use reiny_launch::DeploymentClient;
use serde_json::{Value, json};

#[path = "events.rs"]
mod events;
pub(super) use events::Events;

pub(super) const DEADLINE: Duration = Duration::from_mins(1);
const BIN: &str = env!("CARGO_BIN_EXE_reiny");

pub(super) fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        if matches!(
            entry.file_name().to_str(),
            Some("target" | ".git" | ".reiny")
        ) {
            continue;
        }
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &destination.join(entry.file_name()))?;
        } else if entry.file_type()?.is_file() {
            std::fs::copy(entry.path(), destination.join(entry.file_name()))?;
        }
    }
    Ok(())
}

pub(super) struct Hosts {
    _private: tempfile::TempDir,
    pub(super) example: PathBuf,
    pub(super) binaries: PathBuf,
}

impl Hosts {
    pub(super) async fn build() -> Result<Self> {
        let parent = std::env::var_os("REINY_COMPONENT_TEST_TMP")
            .map_or_else(std::env::temp_dir, PathBuf::from);
        let private = tempfile::Builder::new()
            .prefix("reiny-component-hosts-")
            .tempdir_in(parent)?;
        let source = private.path().join("source");
        let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .context("repository root")?;
        std::fs::create_dir_all(&source)?;
        for file in ["Cargo.toml", "Cargo.lock"] {
            std::fs::copy(repository.join(file), source.join(file))?;
        }
        // A source snapshot, not symlinks into the shared checkout. Cargo may
        // resolve every workspace member even though the example is excluded.
        copy_tree(&repository.join("crates"), &source.join("crates"))?;
        let example = source.join("examples/component-placement");
        copy_tree(&repository.join("examples/component-placement"), &example)?;
        let target = private.path().join("cargo-target");
        let output = tokio::time::timeout(
            Duration::from_mins(15),
            tokio::process::Command::new("cargo")
                .args(["build", "--bins", "--manifest-path"])
                .arg(example.join("Cargo.toml"))
                .arg("--target-dir")
                .arg(&target)
                .env("CARGO_PROFILE_DEV_DEBUG", "0")
                .env_remove("CARGO_ENCODED_RUSTFLAGS")
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("private SDK host build deadline")??;
        ensure!(
            output.status.success(),
            "private SDK host build: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(Self {
            _private: private,
            example,
            binaries: target.join("debug"),
        })
    }
}

pub(super) struct Fixture {
    pub(super) root: tempfile::TempDir,
}

impl Fixture {
    pub(super) fn new(
        hosts: &Hosts,
        endpoint: &str,
        events: &str,
        wrong_type: bool,
        revision: &str,
    ) -> Result<Self> {
        let root = tempfile::tempdir()?;
        copy_tree(&hosts.example, root.path())?;
        let mut manifest: Value =
            serde_yaml::from_str(&std::fs::read_to_string(root.path().join("main.yaml"))?)?;
        manifest["domain"] = format!(
            "placement-{}",
            root.path()
                .file_name()
                .context("fixture directory")?
                .to_string_lossy()
                .trim_start_matches('.')
        )
        .into();
        // Keep both selected ports reserved while allocating the pair, so they
        // cannot alias. The SDK hosts bind them after the fixture is prepared.
        let producer_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let consumer_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let producer_endpoint = format!("tcp/{}", producer_listener.local_addr()?);
        let consumer_endpoint = format!("tcp/{}", consumer_listener.local_addr()?);
        // Zenoh's peer region requires a clique, not a router-mediated star.
        for (provider, listen, peer) in [
            ("producer", &producer_endpoint, &consumer_endpoint),
            ("consumer", &consumer_endpoint, &producer_endpoint),
        ] {
            let config = format!("{provider}-zenoh.json");
            manifest["providers"][provider]["bin_dir"] =
                hosts.binaries.display().to_string().into();
            manifest["providers"][provider]["connect"] = json!([endpoint, peer]);
            manifest["providers"][provider]["zenoh_config"] = config.clone().into();
            std::fs::write(
                root.path().join(config),
                json!({
                    "connect": {"endpoints": []},
                    "listen": {"endpoints": [listen]},
                    "scouting": {"multicast": {"enabled": false}}
                })
                .to_string(),
            )?;
        }
        std::fs::write(
            root.path().join("main.yaml"),
            serde_yaml::to_string(&manifest)?,
        )?;
        for role in ["sender", "receiver"] {
            let path = root.path().join(role).join("main.yaml");
            let mut leaf: Value = serde_yaml::from_str(&std::fs::read_to_string(&path)?)?;
            leaf["component"]["config"] = "settings.toml".into();
            if role == "sender" {
                leaf["component"]["factory_revision"] = revision.into();
            }
            std::fs::write(path, serde_yaml::to_string(&leaf)?)?;
            std::fs::write(
                root.path().join(role).join("settings.toml"),
                format!(
                    "events = {events:?}\nrun_marker = {:?}\nwrong_type = {}\n",
                    root.path()
                        .join(format!("{role}.ran"))
                        .display()
                        .to_string(),
                    wrong_type && role == "sender"
                ),
            )?;
        }
        Ok(Self { root })
    }

    pub(super) fn path(&self) -> &Path {
        self.root.path()
    }

    pub(super) fn block_prepare(&self, role: &str) -> Result<()> {
        let path = self.path().join(role).join("settings.toml");
        let mut settings = std::fs::read_to_string(&path)?;
        settings.push_str("prepare_gate = true\n");
        std::fs::write(path, settings)?;
        Ok(())
    }

    pub(super) async fn cli(&self, arguments: &[&str]) -> Result<Output> {
        tokio::time::timeout(
            DEADLINE,
            tokio::process::Command::new(BIN)
                .args(arguments)
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("CLI command deadline")?
        .context("spawn real reiny CLI")
    }

    pub(super) async fn prepare(&self) -> Result<()> {
        let output = tokio::time::timeout(
            DEADLINE,
            tokio::process::Command::new(BIN)
                .arg("prepare")
                .arg(self.path())
                .arg("--out")
                .arg(self.path().join("prepared.json"))
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("prepare deadline")??;
        success(&output, "prepare")?;
        Ok(())
    }

    pub(super) async fn apply(&self) -> Result<Output> {
        tokio::time::timeout(
            DEADLINE,
            tokio::process::Command::new(BIN)
                .args(["apply", "--prepared"])
                .arg(self.path().join("prepared.json"))
                .args(["--detach", "--ready-timeout", "25"])
                .env("CARGO", self.path().join("compiler-is-unavailable"))
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("apply deadline")?
        .context("spawn prepared apply")
    }

    pub(super) async fn status(&self) -> Result<Value> {
        let output = tokio::time::timeout(
            DEADLINE,
            tokio::process::Command::new(BIN)
                .arg("status")
                .arg(self.path())
                .arg("--json")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("status deadline")??;
        success(&output, "status")?;
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    pub(super) async fn stop(&self) -> Result<Value> {
        let output = tokio::time::timeout(
            DEADLINE,
            tokio::process::Command::new(BIN)
                .arg("stop")
                .arg(self.path())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("stop deadline")??;
        success(&output, "stop")?;
        self.status().await
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        match DeploymentClient::find(self.path()) {
            Ok(Some(client)) => {
                if let Err(error) = client.stop() {
                    eprintln!("component fixture owner cleanup: {error:#}");
                }
            }
            Ok(None) => {}
            Err(error) => eprintln!("component fixture control cleanup: {error:#}"),
        }
    }
}

pub(super) fn success(output: &Output, operation: &str) -> Result<()> {
    ensure!(
        output.status.success(),
        "{operation}: {}: stdout {}; stderr {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
