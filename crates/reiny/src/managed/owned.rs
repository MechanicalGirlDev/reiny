//! Exclusive endpoint delegation, compiled child reports, and owned process reaping.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::Context;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use super::{BUNDLE_DIR_ENV, CONFIG_DIR_ENV, MODULE_REPORT_ENV, Ports, config};
use crate::bindings::{
    CONTRACT_VERSION, EndpointContract, ModuleBindings, ModuleReport, PortReport,
};
use crate::engine::{Engine, Guard, Key, Presence, QueryParams, READY_CHUNK, STOP_CHUNK};
use crate::{Cloudy, Result};

pub(super) struct ChildState {
    pub(super) ready: bool,
    pub(super) exited: bool,
    pub(super) report: Option<ModuleReport>,
    pub(super) kill: mpsc::UnboundedSender<()>,
    _watch: Guard,
}

#[derive(Clone)]
enum ProcessState {
    Running,
    Exited(ExitStatus),
    Failed(String),
}

/// An owned helper process. Dropping the handle requests termination; its supervisor reaps it.
///
/// Keep this handle alive while the helper implements delegated endpoints. [`Self::wait`] and
/// [`Self::kill`] finish only after the child has been reaped, not merely after a stop request.
pub struct OwnedChild {
    name: String,
    pid: u32,
    bindings: ModuleBindings,
    report_path: PathBuf,
    ports: Arc<Mutex<Ports>>,
    ready: watch::Receiver<bool>,
    process: watch::Receiver<ProcessState>,
    kill: mpsc::UnboundedSender<()>,
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.request_kill();
    }
}

impl OwnedChild {
    fn request_kill(&self) {
        if self.kill.send(()).is_err() && matches!(*self.process.borrow(), ProcessState::Running) {
            tracing::warn!(
                child = self.name,
                pid = self.pid,
                "owned child supervisor ended before reporting process exit"
            );
        }
        // A closed channel after Exited/Failed is normal: the supervisor has already reaped.
    }

    /// The child process id, retained after reaping for diagnostics.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.pid
    }

    /// The child's distinct runtime and readiness namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.bindings.namespace
    }

    /// Wait at most ten seconds for exact live readiness, then verify the child's compiled report.
    pub async fn wait_ready(&mut self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                anyhow::ensure!(
                    matches!(*self.process.borrow(), ProcessState::Running),
                    "owned child '{}' exited before readiness",
                    self.name
                );
                if *self.ready.borrow_and_update() {
                    break;
                }
                tokio::select! {
                    result = self.ready.changed() => result.context("child readiness watch ended")?,
                    result = self.process.changed() => result.context("child process watch ended")?,
                }
            }
            let report: ModuleReport = serde_json::from_slice(
                &std::fs::read(&self.report_path).context("reading owned child report")?,
            )
            .context("parsing owned child report")?;
            validate_report(&self.bindings, &report)?;
            let mut ports = self.ports.lock().unwrap_or_else(PoisonError::into_inner);
            let child = ports
                .children
                .get_mut(&self.name)
                .ok_or_else(|| anyhow::anyhow!("owned child registration disappeared"))?;
            anyhow::ensure!(child.ready && !child.exited, "owned child lost readiness");
            child.report = Some(report);
            Ok(())
        })
        .await
        .context("owned child readiness deadline elapsed")?
    }

    /// Wait for the process supervisor to reap the child.
    pub async fn wait(&mut self) -> Result<ExitStatus> {
        loop {
            match self.process.borrow_and_update().clone() {
                ProcessState::Running => {}
                ProcessState::Exited(status) => return Ok(status),
                ProcessState::Failed(message) => anyhow::bail!(message),
            }
            self.process
                .changed()
                .await
                .context("child process supervisor ended")?;
        }
    }

    /// Terminate the owned child and wait for reaping.
    pub async fn kill(&mut self) -> Result<ExitStatus> {
        self.request_kill();
        self.wait().await
    }
}

/// A child connects to the same fabric but must never reuse the host's listening socket or id.
#[cfg(feature = "zenoh")]
pub(crate) fn child_zenoh_config(mut config: zenoh::Config) -> Result<String> {
    let json = serde_json::to_value(&config)?;
    let mode = json
        .get("mode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("peer");
    let endpoints = |section: &str| -> Vec<String> {
        let value = &json[section]["endpoints"];
        let selected = if value.is_object() {
            &value[mode]
        } else {
            value
        };
        selected
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(str::to_string)
            .collect()
    };
    let mut connect = endpoints("connect");
    for listener in endpoints("listen") {
        // Port zero is not an address peers can connect to; retain the configured discovery path.
        let address = listener.split(['?', '#']).next().unwrap_or(&listener);
        if address.ends_with(":0") {
            continue;
        }
        let listener = listener
            .replace("/0.0.0.0:", "/127.0.0.1:")
            .replace("/[::]:", "/[::1]:");
        if !connect.contains(&listener) {
            connect.push(listener);
        }
    }
    config
        .insert_json5("listen/endpoints", "[]")
        .map_err(anyhow::Error::msg)?;
    config
        .insert_json5("connect/endpoints", &serde_json::to_string(&connect)?)
        .map_err(anyhow::Error::msg)?;
    config
        .insert_json5("id", "null")
        .map_err(anyhow::Error::msg)?;
    Ok(serde_json::to_string(&config)?)
}

impl Cloudy {
    /// Resolve a declared executable from the prepared bundle, never from a developer directory.
    pub fn artifact(&self, name: &str) -> Result<PathBuf> {
        let bindings = self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("artifacts require managed module bindings"))?;
        anyhow::ensure!(
            bindings.executables.iter().any(|item| item == name),
            "undeclared executable '{name}'"
        );
        crate::validate_segment("executable", name)?;
        anyhow::ensure!(!matches!(name, "." | ".."), "invalid executable name");
        let directory = self
            .module
            .bundle_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("{BUNDLE_DIR_ENV} is required for artifacts"))?;
        anyhow::ensure!(directory.is_absolute(), "{BUNDLE_DIR_ENV} must be absolute");
        let directory = directory
            .canonicalize()
            .context("resolving prepared bundle")?;
        let filename = if cfg!(windows)
            && !Path::new(name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("exe"))
        {
            format!("{name}.exe")
        } else {
            name.to_string()
        };
        let executable = directory
            .join(filename)
            .canonicalize()
            .context("resolving declared executable")?;
        anyhow::ensure!(
            executable.parent() == Some(directory.as_path()) && executable.is_file(),
            "executable escapes prepared bundle"
        );
        Ok(executable)
    }

    /// The optional prepared configuration asset directory.
    pub fn config_dir(&self) -> Result<Option<&Path>> {
        anyhow::ensure!(
            self.module.bindings.is_some(),
            "prepared configuration requires managed bindings"
        );
        if let Some(path) = &self.module.config_dir {
            anyhow::ensure!(
                path.is_absolute() && path.is_dir(),
                "{CONFIG_DIR_ENV} must be an absolute directory"
            );
        }
        Ok(self.module.config_dir.as_deref())
    }

    /// Spawn a declared owned child using an executable returned by [`Self::artifact`].
    ///
    /// `directory` must be an absolute writable runtime directory. A fresh child subdirectory
    /// contains its dedicated bindings and report; stale files are never accepted. The command's
    /// application arguments are preserved. SDK identity, domain and contract arguments are set
    /// here, and the child's endpoint address remains the host's public namespace.
    pub fn spawn_owned_child(
        &self,
        name: &str,
        command: &mut Command,
        directory: &Path,
    ) -> Result<OwnedChild> {
        let bindings = self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("owned children require module bindings"))?;
        let child_bindings = delegated_bindings(bindings, name)?;
        anyhow::ensure!(
            directory.is_absolute(),
            "owned child runtime directory must be absolute"
        );
        let program = Path::new(command.as_std().get_program());
        anyhow::ensure!(
            program.is_absolute(),
            "owned child executable must come from artifact()"
        );
        let program = program.canonicalize().context("resolving child program")?;
        let mut declared = false;
        for executable in &bindings.executables {
            if self.artifact(executable)? == program {
                declared = true;
                break;
            }
        }
        anyhow::ensure!(
            declared,
            "owned child executable is not a declared staged artifact"
        );
        let handle = tokio::runtime::Handle::try_current()
            .context("owned child requires a Tokio runtime")?;
        let mut ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        anyhow::ensure!(
            !self.shutdown.is_triggered() && ports.ready.is_none(),
            "cannot spawn a child after readiness or shutdown"
        );
        anyhow::ensure!(
            !ports.children.contains_key(name),
            "owned child '{name}' already spawned"
        );
        let child_dir = directory.join(name);
        std::fs::create_dir(&child_dir).context("creating fresh owned child runtime directory")?;
        let bindings_path = child_dir.join("bindings.json");
        let report_path = child_dir.join("report.json");
        std::fs::write(&bindings_path, serde_json::to_vec(&child_bindings)?)?;
        if let Some(config) = &self.module.child_zenoh_config {
            let config_path = child_dir.join("zenoh.json");
            std::fs::write(&config_path, config)?;
            command.arg("--zenoh-config").arg(config_path);
        }
        command
            .arg("--name")
            .arg(&child_bindings.namespace)
            .arg("--domain")
            .arg(&self.domain)
            .arg("--module-bindings")
            .arg(&bindings_path)
            .env(MODULE_REPORT_ENV, &report_path)
            .kill_on_drop(true);
        if let Some(path) = &self.module.bundle_dir {
            command.env(BUNDLE_DIR_ENV, path);
        }
        if let Some(path) = &self.module.config_dir {
            command.env(CONFIG_DIR_ENV, path);
        } else {
            command.env_remove(CONFIG_DIR_ENV);
        }
        let (guard, ready) = self.watch_owned_child(name, &child_bindings.namespace)?;
        let process = command.spawn().context("spawning owned child")?;
        let pid = process
            .id()
            .ok_or_else(|| anyhow::anyhow!("spawned child has no process id"))?;
        let (kill, kills) = mpsc::unbounded_channel();
        let (process_tx, process_rx) = watch::channel(ProcessState::Running);
        ports.children.insert(
            name.to_string(),
            ChildState {
                ready: false,
                exited: false,
                report: None,
                kill: kill.clone(),
                _watch: guard,
            },
        );
        drop(ports);
        supervise_child(&handle, self, name, process, kills, process_tx);
        Ok(OwnedChild {
            name: name.to_string(),
            pid,
            bindings: child_bindings,
            report_path,
            ports: Arc::clone(&self.module.ports),
            ready,
            process: process_rx,
            kill,
        })
    }

    fn watch_owned_child(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<(Guard, watch::Receiver<bool>)> {
        let (ready_tx, ready) = watch::channel(false);
        let observed_ports = Arc::downgrade(&self.module.ports);
        let observed_name = name.to_string();
        let shutdown = self.shutdown.clone();
        let key = Key::topic(&self.domain, Some(namespace), READY_CHUNK);
        let guard = self.engine.watch_alive(
            &key,
            Box::new(move |event| {
                let is_ready = matches!(event, Presence::Joined(_));
                if let Some(ports) = observed_ports.upgrade() {
                    let mut ports = ports.lock().unwrap_or_else(PoisonError::into_inner);
                    if let Some(child) = ports.children.get_mut(&observed_name) {
                        let lost = child.ready && !is_ready;
                        child.ready = is_ready && !child.exited;
                        if lost {
                            child.report = None;
                            ports.ready = None;
                            shutdown.trigger();
                        }
                    }
                }
                ready_tx.send_replace(is_ready);
            }),
        )?;
        Ok((guard, ready))
    }
}

fn supervise_child(
    handle: &tokio::runtime::Handle,
    cloudy: &Cloudy,
    name: &str,
    mut process: Child,
    mut kills: mpsc::UnboundedReceiver<()>,
    process_tx: watch::Sender<ProcessState>,
) {
    let process_ports = Arc::downgrade(&cloudy.module.ports);
    let process_name = name.to_string();
    let shutdown = cloudy.shutdown.clone();
    let engine = Arc::clone(&cloudy.engine);
    let stop_key = Key::topic(
        &cloudy.domain,
        Some(&format!("{}/owned/{name}", cloudy.id)),
        STOP_CHUNK,
    );
    handle.spawn(async move {
        let status = tokio::select! {
            result = process.wait() => result,
            _ = kills.recv() => kill_and_reap(&mut process).await,
            () = shutdown.wait() => stop_and_reap(&mut process, &engine, &stop_key).await,
        };
        if let Some(ports) = process_ports.upgrade() {
            let mut ports = ports.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(child) = ports.children.get_mut(&process_name) {
                child.exited = true;
                child.ready = false;
                child.report = None;
            }
            ports.ready = None;
        }
        shutdown.trigger();
        process_tx.send_replace(match status {
            Ok(status) => ProcessState::Exited(status),
            Err(error) => ProcessState::Failed(error.to_string()),
        });
    });
}

async fn kill_and_reap(process: &mut Child) -> std::io::Result<ExitStatus> {
    process.kill().await?;
    process
        .try_wait()?
        .ok_or_else(|| std::io::Error::other("child was not reaped"))
}

async fn stop_and_reap(
    process: &mut Child,
    engine: &Arc<dyn Engine>,
    key: &Key,
) -> std::io::Result<ExitStatus> {
    // Leave room for the enclosing deployment owner's two-second tree deadline.
    let grace = Duration::from_secs(1);
    let deadline = tokio::time::Instant::now() + grace;
    // Keep the query alive, but wait for process exit rather than treating its ack as cleanup.
    let _request = match engine.query(
        key,
        QueryParams {
            payload: Some(Vec::new()),
            attachment: None,
            timeout: grace,
        },
    ) {
        Ok(replies) => Some(replies),
        Err(error) => {
            tracing::warn!(%key, %error, "owned child cooperative stop unavailable");
            None
        }
    };
    match tokio::time::timeout_at(deadline, process.wait()).await {
        Ok(status) => status,
        Err(_) => kill_and_reap(process).await,
    }
}

pub(super) fn delegated_bindings(bindings: &ModuleBindings, name: &str) -> Result<ModuleBindings> {
    let delegation = bindings
        .children
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("undeclared owned child '{name}'"))?;
    let child = ModuleBindings {
        version: CONTRACT_VERSION,
        namespace: format!("{}/owned/{name}", bindings.namespace),
        endpoint_namespace: bindings.endpoint_namespace.clone(),
        inputs: delegation
            .inputs
            .iter()
            .map(|name| (name.clone(), bindings.inputs[name].clone()))
            .collect(),
        outputs: delegation
            .outputs
            .iter()
            .map(|name| (name.clone(), bindings.outputs[name].clone()))
            .collect(),
        children: BTreeMap::new(),
        executables: bindings.executables.clone(),
    };
    config::validate_bindings(&child, &child.namespace)?;
    Ok(child)
}

fn validate_report(bindings: &ModuleBindings, report: &ModuleReport) -> Result<()> {
    anyhow::ensure!(
        report.version == CONTRACT_VERSION && report.namespace == bindings.namespace,
        "owned child report identity differs"
    );
    for (declared, actual) in [
        (
            bindings
                .inputs
                .iter()
                .map(|(name, port)| (name, &port.contract))
                .collect::<BTreeMap<_, _>>(),
            &report.inputs,
        ),
        (
            bindings
                .outputs
                .iter()
                .map(|(name, port)| (name, &port.contract))
                .collect(),
            &report.outputs,
        ),
    ] {
        anyhow::ensure!(
            declared.len() == actual.len(),
            "owned child reported a different endpoint set"
        );
        for (name, contract) in declared {
            let port = actual
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("owned child omitted endpoint '{name}'"))?;
            validate_port_report(contract, port)?;
        }
    }
    Ok(())
}

fn validate_port_report(contract: &EndpointContract, report: &PortReport) -> Result<()> {
    anyhow::ensure!(
        contract == &report.contract,
        "owned child changed the endpoint contract"
    );
    let matches = |declared: &str, actual: &str| {
        declared == actual || actual.rsplit('.').next() == Some(declared)
    };
    anyhow::ensure!(
        matches(&contract.type_name, &report.type_name),
        "owned child compiled type differs"
    );
    match (&contract.response, &report.response) {
        (None, None) => {}
        (Some(declared), Some(actual)) => anyhow::ensure!(
            matches(declared, &actual.type_name),
            "owned child compiled response differs"
        ),
        _ => anyhow::bail!("owned child response contract differs"),
    }
    Ok(())
}
