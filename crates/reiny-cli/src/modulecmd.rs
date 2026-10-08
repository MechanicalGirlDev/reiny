//! The CLI's engine adapter and authenticated deployment owner bootstrap.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use reiny::zenoh::{self, Wait};
use reiny::{RuntimeOptions, ZenohSource};
use reiny_launch::{DeploymentClient, DeploymentPlan, ModuleObserver, PreparedDeployment};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::bus::KEY_ROOT;

pub(crate) fn plan(path: &Path, update: bool, json: bool) -> Result<()> {
    let plan = DeploymentPlan::load(path, update)?;
    let current = DeploymentClient::find(&plan.root)?
        .map(|client| client.status())
        .transpose()?;
    if let Some(current) = &current {
        anyhow::ensure!(
            current.deployment == plan.deployment && current.domain == plan.domain,
            "stop the existing owner before changing deployment or domain"
        );
    }
    let mut changes = Vec::new();
    for node in &plan.nodes {
        let active = current
            .as_ref()
            .and_then(|state| state.modules.get(&node.namespace))
            .is_some_and(|module| module.pid.is_some());
        changes.push(PlannedChange {
            namespace: node.namespace.clone(),
            action: if !active {
                "create"
            } else if node.build.is_some() {
                "prepare_and_compare"
            } else {
                "verify_and_compare"
            },
        });
    }
    if let Some(current) = &current {
        for (namespace, module) in &current.modules {
            if module.pid.is_some() && !plan.nodes.iter().any(|node| &node.namespace == namespace) {
                changes.push(PlannedChange {
                    namespace: namespace.clone(),
                    action: "remove",
                });
            }
        }
    }
    if json {
        print_json(&PlanView {
            declaration: &plan,
            changes: &changes,
        })?;
    } else {
        println!("deployment {} (domain {})", plan.deployment, plan.domain);
        for node in &plan.nodes {
            let preparation = if node.build.is_some() {
                "check Cargo build"
            } else {
                "verify installed binary"
            };
            println!("  {}: {} — {preparation}", node.namespace, node.run.bin);
            for (port, input) in &node.bindings.inputs {
                println!(
                    "    in.{port}: {} <- {}",
                    input.contract.type_name,
                    input.sources.join(", ")
                );
            }
            for (port, output) in &node.bindings.outputs {
                println!(
                    "    out.{port}: {} {:?}",
                    output.contract.type_name, output.contract.kind
                );
            }
        }
        for resource in &plan.resources {
            println!(
                "  artifact {} ← {}",
                resource.address,
                resource.source.display()
            );
        }
        for change in &changes {
            println!("  {}: {}", change.action, change.namespace);
        }
        print_flow(&plan);
    }
    Ok(())
}

fn print_flow(plan: &DeploymentPlan) {
    let indices: BTreeMap<_, _> = plan
        .nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.namespace.clone(), index))
        .collect();
    let mut connections: BTreeMap<(String, String, Option<String>), Vec<usize>> = BTreeMap::new();
    for (index, node) in plan.nodes.iter().enumerate() {
        for input in node.bindings.inputs.values() {
            for source in &input.sources {
                let subscribers = connections
                    .entry((
                        source.clone(),
                        input.contract.type_name.clone(),
                        input.contract.response.clone(),
                    ))
                    .or_default();
                if !subscribers.contains(&index) {
                    subscribers.push(index);
                }
            }
        }
    }
    let edges: Vec<_> = connections
        .into_iter()
        .map(|((source, ty, reply), clients)| {
            let server = vec![indices[&source]];
            let (pubs, subs) = if reply.is_some() {
                (clients, server)
            } else {
                (server, clients)
            };
            crate::flowart::Edge {
                ty,
                pubs,
                subs,
                reply,
            }
        })
        .collect();
    let prefix = format!("{}/", plan.deployment);
    let names: Vec<_> = plan
        .nodes
        .iter()
        .map(|node| {
            node.namespace
                .strip_prefix(&prefix)
                .unwrap_or(&node.namespace)
                .to_string()
        })
        .collect();
    let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    for line in crate::flowart::render(&names, &edges, color) {
        println!("{line}");
    }
}

#[derive(Serialize)]
struct PlannedChange {
    namespace: String,
    action: &'static str,
}

#[derive(Serialize)]
struct PlanView<'a> {
    #[serde(flatten)]
    declaration: &'a DeploymentPlan,
    changes: &'a [PlannedChange],
}

pub(crate) fn apply(
    path: &Path,
    detach: bool,
    bin_dir: Option<PathBuf>,
    ready_timeout: f64,
) -> Result<()> {
    let timeout =
        Duration::try_from_secs_f64(ready_timeout).context("invalid readiness deadline")?;
    if timeout.is_zero() {
        anyhow::bail!("managed readiness cannot be disabled");
    }
    let mut plan = DeploymentPlan::load(path, false)?;
    if let Some(dir) = bin_dir {
        let dir = std::fs::canonicalize(&dir).context("resolving binary directory")?;
        for node in &mut plan.nodes {
            node.provider.bin_dir.clone_from(&dir);
        }
    }
    let prepared = plan.prepare()?;
    let client = match DeploymentClient::find(&prepared.root)? {
        Some(client) => {
            client.apply(&prepared)?;
            client
        }
        None => start_owner(&prepared, timeout)?,
    };
    let state = match client.wait_ready(timeout) {
        Ok(state) => state,
        Err(error) => {
            if let Err(cleanup) = client.stop() {
                eprintln!("cleaning failed deployment start: {cleanup:#}");
            }
            return Err(error);
        }
    };
    print_json(&state)?;
    if detach {
        return Ok(());
    }
    let termination = client.watch_terminal()?;
    let client = Arc::new(client);
    let stop_client = Arc::clone(&client);
    ctrlc::set_handler(move || {
        if let Err(error) = stop_client.stop() {
            eprintln!("stopping deployment: {error:#}");
        }
    })
    .context("installing deployment stop handler")?;
    let stopped = termination.wait()?;
    print_json(&stopped)
}

pub(crate) fn status(path: &Path, json: bool) -> Result<()> {
    let root = root_dir(path)?;
    let state = match DeploymentClient::find(&root)? {
        Some(client) => client.status()?,
        None => reiny_launch::last_status(&root)?.context("deployment has no recorded state")?,
    };
    if json {
        print_json(&state)
    } else {
        println!("{}", serde_json::to_string_pretty(&state)?);
        Ok(())
    }
}

pub(crate) fn stop(path: &Path, json: bool) -> Result<()> {
    let root = root_dir(path)?;
    let client = DeploymentClient::find(&root)?.context("deployment has no live owner")?;
    let state = client.stop()?;
    if json {
        print_json(&state)
    } else {
        println!("{}", serde_json::to_string_pretty(&state)?);
        Ok(())
    }
}

pub(crate) fn supervise(path: &Path) -> Result<()> {
    let prepared: PreparedDeployment =
        serde_json::from_slice(&std::fs::read(path)?).context("reading prepared deployment")?;
    let observer = BusObserver::open(&prepared)?;
    let stop = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&stop);
    ctrlc::set_handler(move || signal.store(true, Ordering::SeqCst))
        .context("installing owner stop handler")?;
    let rendezvous = match std::env::var("REINY_OWNER_RENDEZVOUS") {
        Ok(value) => {
            Some(serde_json::from_str::<Rendezvous>(&value).context("reading owner rendezvous")?)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(error @ std::env::VarError::NotUnicode(_)) => return Err(error.into()),
    };
    reiny_launch::serve(prepared, &observer, &stop, &|| {
        if let Some(rendezvous) = &rendezvous {
            let mut stream =
                std::net::TcpStream::connect_timeout(&rendezvous.address, Duration::from_secs(5))?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            writeln!(
                stream,
                "{}",
                serde_json::to_string(&OwnerReady {
                    nonce: rendezvous.nonce.clone(),
                    pid: std::process::id(),
                })?
            )?;
            let mut accepted = [0u8; 9];
            stream.read_exact(&mut accepted)?;
            anyhow::ensure!(
                &accepted == b"accepted\n",
                "owner bootstrap was not accepted"
            );
        }
        Ok(())
    })
}

#[derive(Serialize, Deserialize)]
struct Rendezvous {
    address: SocketAddr,
    nonce: String,
}

#[derive(Serialize, Deserialize)]
struct OwnerReady {
    nonce: String,
    pid: u32,
}

fn start_owner(prepared: &PreparedDeployment, timeout: Duration) -> Result<DeploymentClient> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let listener = runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let rendezvous = Rendezvous {
        address: listener.local_addr()?,
        nonce: nonce()?,
    };
    let directory = prepared.root.join(".reiny").join("prepared");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{}.json", rendezvous.nonce));
    std::fs::write(&path, serde_json::to_vec(prepared)?)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(prepared.root.join(".reiny").join("owner.log"))?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .arg("__supervise")
        .arg(&path)
        .env(
            "REINY_OWNER_RENDEZVOUS",
            serde_json::to_string(&rendezvous)?,
        )
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    let mut owner = runtime
        .block_on(async { command.spawn() })
        .context("starting deployment owner")?;
    let owner_pid = owner.id().context("owner process has no PID")?;
    let result = runtime.block_on(async {
        tokio::select! {
            ready = tokio::time::timeout(timeout, async {
                let (stream, _) = listener.accept().await?;
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await?;
                let ready: OwnerReady = serde_json::from_str(&line)?;
                anyhow::ensure!(ready.nonce == rendezvous.nonce && ready.pid == owner_pid, "invalid owner rendezvous");
                reader.get_mut().write_all(b"accepted\n").await?;
                Ok::<(), anyhow::Error>(())
            }) => ready.context("owner bootstrap timed out")?,
            exited = owner.wait() => Err(anyhow::anyhow!("owner exited before bootstrap: {}", exited?)),
        }
    });
    if let Err(error) = result {
        runtime.block_on(terminate_bootstrap(&mut owner));
        if let Err(cleanup) = std::fs::remove_file(&path) {
            eprintln!("removing prepared bootstrap: {cleanup}");
        }
        return Err(error.context(format!(
            "owner log: {}",
            prepared.root.join(".reiny/owner.log").display()
        )));
    }
    std::fs::remove_file(&path)?;
    let client = DeploymentClient::find(&prepared.root)
        .and_then(|client| client.context("owner rendezvous completed without a control endpoint"));
    if client.is_err() {
        runtime.block_on(terminate_bootstrap(&mut owner));
    }
    client
}

async fn terminate_bootstrap(owner: &mut tokio::process::Child) {
    match owner.try_wait() {
        Ok(Some(_)) => {}
        Ok(None) => {
            if let Err(error) = owner.kill().await {
                eprintln!("terminating failed owner bootstrap: {error}");
            }
        }
        Err(error) => eprintln!("checking failed owner bootstrap: {error}"),
    }
}

fn nonce() -> Result<String> {
    use std::fmt::Write as _;
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(anyhow::Error::msg)?;
    let mut token = String::with_capacity(64);
    for byte in bytes {
        write!(&mut token, "{byte:02x}")?;
    }
    Ok(token)
}

fn root_dir(path: &Path) -> Result<PathBuf> {
    let path = if path.is_file() {
        anyhow::ensure!(
            path.file_name().is_some_and(|name| name == "main.yaml"),
            "expected main.yaml"
        );
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    } else {
        path
    };
    std::fs::canonicalize(path).context("resolving module root")
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

struct BusObserver {
    domain: String,
    sessions: RwLock<BTreeMap<String, (String, zenoh::Session)>>,
}

impl BusObserver {
    fn open(prepared: &PreparedDeployment) -> Result<Self> {
        let observer = Self {
            domain: prepared.domain.clone(),
            sessions: RwLock::new(BTreeMap::new()),
        };
        observer.configure(prepared)?;
        Ok(observer)
    }

    fn replace_sessions(&self, prepared: &PreparedDeployment) -> Result<()> {
        anyhow::ensure!(prepared.domain == self.domain, "deployment domain changed");
        let previous = self
            .sessions
            .read()
            .map_err(|_| anyhow::anyhow!("observer registry poisoned"))?;
        let mut sessions = BTreeMap::new();
        for node in &prepared.nodes {
            let signature = serde_json::to_string(&node.provider)?;
            if let Some((existing, session)) = previous.get(&node.namespace)
                && existing == &signature
            {
                sessions.insert(node.namespace.clone(), (signature, session.clone()));
                continue;
            }
            let mut options = RuntimeOptions::new("module-owner");
            options.domain.clone_from(&prepared.domain);
            if let Some(config) = &node.provider.zenoh_config {
                options.zenoh = ZenohSource::File(config.clone());
            }
            if !node.provider.connect.is_empty() {
                options.zenoh_overrides.push((
                    "connect/endpoints".into(),
                    serde_json::to_string(&node.provider.connect)?,
                ));
            }
            let session = zenoh::open(options.zenoh_config()?)
                .wait()
                .map_err(anyhow::Error::msg)?;
            sessions.insert(node.namespace.clone(), (signature, session));
        }
        drop(previous);
        *self
            .sessions
            .write()
            .map_err(|_| anyhow::anyhow!("observer registry poisoned"))? = sessions;
        Ok(())
    }

    fn session(&self, namespace: &str) -> Result<zenoh::Session> {
        self.sessions
            .read()
            .map_err(|_| anyhow::anyhow!("observer registry poisoned"))?
            .get(namespace)
            .map(|(_, session)| session.clone())
            .with_context(|| format!("unknown managed namespace {namespace}"))
    }
}

impl ModuleObserver for BusObserver {
    fn configure(&self, prepared: &PreparedDeployment) -> Result<()> {
        self.replace_sessions(prepared)
    }

    fn ready(&self, namespace: &str) -> Result<bool> {
        let key = format!("{KEY_ROOT}/{}/{namespace}/@ready", self.domain);
        let replies = self
            .session(namespace)?
            .liveliness()
            .get(&key)
            .timeout(Duration::from_millis(200))
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(replies.iter().any(|reply| {
            reply
                .result()
                .is_ok_and(|sample| sample.key_expr().as_str() == key)
        }))
    }

    fn request_stop(&self, namespace: &str) -> Result<()> {
        let key = format!("{KEY_ROOT}/{}/{namespace}/@stop", self.domain);
        let replies = self
            .session(namespace)?
            .get(&key)
            .payload(Vec::<u8>::new())
            .timeout(Duration::from_secs(2))
            .wait()
            .map_err(anyhow::Error::msg)?;
        let reply = replies
            .recv()
            .map_err(anyhow::Error::msg)
            .context("managed module did not acknowledge stop")?;
        reply
            .result()
            .map_err(|error| anyhow::anyhow!("managed stop rejected: {error:?}"))?;
        Ok(())
    }
}
