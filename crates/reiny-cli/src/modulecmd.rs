//! The CLI's engine adapter and authenticated deployment owner bootstrap.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use reiny::zenoh::{self, Wait};
use reiny::{RuntimeOptions, ZenohSource};
use reiny_launch::{DeploymentClient, DeploymentPlan, ModuleObserver, PreparedDeployment};
use serde::{Deserialize, Serialize};

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
                "check declared build"
            } else {
                "verify installed binary"
            };
            println!("  {}: {} — {preparation}", node.namespace, node.run.bin);
            for (port, input) in &node.bindings.inputs {
                let routes = plan
                    .connections
                    .iter()
                    .filter(|route| {
                        route.destination.namespace == node.namespace
                            && route.destination.name == *port
                    })
                    .map(|route| {
                        let via = route
                            .via
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>();
                        if via.is_empty() {
                            route.source.to_string()
                        } else {
                            format!("{} via {}", route.source, via.join(" <- "))
                        }
                    })
                    .collect::<Vec<_>>();
                println!(
                    "    in.{port}: {} <- {}",
                    input.contract.type_name,
                    routes.join(", ")
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
    let mut connections: BTreeMap<(String, String, String, Option<String>), Vec<usize>> =
        BTreeMap::new();
    for route in &plan.connections {
        let index = indices[&route.destination.namespace];
        let subscribers = connections
            .entry((
                route.source.namespace.clone(),
                route.source.name.clone(),
                route.contract.type_name.clone(),
                route.contract.response.clone(),
            ))
            .or_default();
        if !subscribers.contains(&index) {
            subscribers.push(index);
        }
    }
    let edges: Vec<_> = connections
        .into_iter()
        .map(|((source, port, ty, reply), clients)| {
            let server = vec![indices[&source]];
            let (pubs, subs) = if reply.is_some() {
                (clients, server)
            } else {
                (server, clients)
            };
            crate::flowart::Edge {
                ty: format!("{port}: {ty}"),
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
    apply_args(crate::StartArgs {
        path: Some(path.to_owned()),
        prepared: None,
        detach,
        build: crate::preparecmd::BuildOptions {
            bin_dir,
            ..Default::default()
        },
        events: None,
        control_stdin: false,
        log_level: "info".into(),
        ready_timeout,
    })
}

pub(crate) fn apply_args(args: crate::StartArgs) -> Result<()> {
    let control = crate::control::Control::install(args.control_stdin)?;
    let protocol = crate::preparecmd::Protocol::new(args.events);
    let timeout = Duration::try_from_secs_f64(args.ready_timeout)
        .context("invalid readiness deadline")
        .and_then(|timeout| {
            anyhow::ensure!(!timeout.is_zero(), "managed readiness cannot be disabled");
            Ok(timeout)
        });
    let timeout = match timeout {
        Ok(timeout) => timeout,
        Err(error) => return crate::applycmd::failed(&protocol, error),
    };
    let prepared = (|| {
        if let Some(path) = args.prepared {
            PreparedDeployment::load_file(&path)
        } else {
            let path = args.path.as_deref().unwrap_or(Path::new("."));
            let plan = crate::preparecmd::load(path, &args.build, &control)?;
            crate::preparecmd::materialize(
                &plan,
                &args.build,
                &control,
                &crate::preparecmd::Protocol::new(None),
            )
        }
    })();
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => return crate::applycmd::failed(&protocol, control.failure(error)),
    };
    crate::applycmd::apply(
        &prepared,
        args.detach,
        args.control_stdin,
        timeout,
        &control,
        &protocol,
    )
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
    prepared.verify()?;
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
    let lease = std::sync::Mutex::new(None::<std::net::TcpStream>);
    let lease_reader = std::sync::Mutex::new(None::<std::thread::JoinHandle<()>>);
    let result = reiny_launch::serve(prepared, &observer, &stop, &|| {
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
            if rendezvous.lease {
                stream.set_read_timeout(None)?;
                let mut reader = stream.try_clone()?;
                let signal = Arc::clone(&stop);
                let thread = std::thread::Builder::new()
                    .name("reiny-parent-lease".into())
                    .spawn(move || {
                        let mut byte = [0u8; 1];
                        // EOF, reset, or unexpected data all revoke the parent's lease.
                        if let Err(error) = reader.read(&mut byte) {
                            tracing::debug!(%error, "parent lease closed");
                        }
                        signal.store(true, Ordering::SeqCst);
                    })?;
                *lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stream);
                *lease_reader
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread);
            }
        }
        Ok(())
    });
    if let Some(stream) = lease
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        && let Err(error) = stream.shutdown(std::net::Shutdown::Both)
    {
        tracing::debug!(%error, "closing parent lease");
    }
    if let Some(thread) = lease_reader
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        thread
            .join()
            .map_err(|_| anyhow::anyhow!("parent lease reader panicked"))?;
    }
    result
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Rendezvous {
    pub(crate) address: SocketAddr,
    pub(crate) nonce: String,
    pub(crate) lease: bool,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct OwnerReady {
    pub(crate) nonce: String,
    pub(crate) pid: u32,
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
