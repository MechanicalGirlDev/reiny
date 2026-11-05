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

mod observer;
mod planning;
use observer::BusObserver;
pub(crate) use planning::plan;

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
