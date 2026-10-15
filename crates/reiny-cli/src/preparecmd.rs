//! Shared CLI preparation driver and versioned, flushed JSONL publication.

use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, mpsc};

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use reiny_launch::{
    BuildSpec, DeploymentPlan, PrepareError, PrepareEvent, PrepareObserver, PrepareOptions,
    PreparePhase, PrepareStatus, PreparedDeployment,
};
use serde::Serialize;

use crate::control::Control;

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Events {
    Jsonl,
}

#[derive(Args, Default)]
pub(crate) struct BuildOptions {
    /// Override installed binaries only for modules without a build declaration.
    #[arg(long)]
    pub(crate) bin_dir: Option<PathBuf>,
    /// Override Cargo profiles only.
    #[arg(long)]
    pub(crate) release: bool,
    /// Persistent compiler cache, independent of the prepared output directory.
    #[arg(long)]
    pub(crate) build_cache: Option<PathBuf>,
    /// Maximum number of independent build groups.
    #[arg(long, default_value = "1")]
    pub(crate) jobs: Option<NonZeroUsize>,
}

pub(crate) struct Protocol {
    enabled: bool,
    seq: Mutex<u64>,
}

impl Protocol {
    pub(crate) fn new(events: Option<Events>) -> Self {
        Self {
            enabled: events.is_some(),
            seq: Mutex::new(0),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn emit(&self, event: &impl Serialize) -> Result<()> {
        #[derive(Serialize)]
        struct Envelope<'a, T> {
            v: u32,
            seq: u64,
            #[serde(flatten)]
            payload: &'a T,
        }
        if !self.enabled {
            return Ok(());
        }
        let mut seq = self
            .seq
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *seq = seq.checked_add(1).context("protocol sequence overflow")?;
        let mut stdout = std::io::stdout().lock();
        serde_json::to_writer(
            &mut stdout,
            &Envelope {
                v: 1,
                seq: *seq,
                payload: event,
            },
        )?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
        Ok(())
    }
}

struct Observer(mpsc::Sender<PrepareEvent>);

impl PrepareObserver for Observer {
    fn event(&self, event: &PrepareEvent) {
        // The materializer never waits for a pipe reader or filesystem logger.
        if let Err(error) = self.0.send(event.clone()) {
            eprintln!("preparation event receiver closed: {error}");
        }
    }
}

pub(crate) fn load(
    path: &Path,
    options: &BuildOptions,
    control: &Control,
) -> Result<DeploymentPlan> {
    control.check()?;
    let mut plan = DeploymentPlan::load_with(path, false, &control.cancel)?;
    let bin_dir = options
        .bin_dir
        .as_ref()
        .map(std::fs::canonicalize)
        .transpose()
        .context("resolving binary directory")?;
    for node in &mut plan.nodes {
        if node.build.is_none()
            && let Some(dir) = &bin_dir
        {
            node.provider.bin_dir.clone_from(dir);
        }
        if options.release
            && let Some(BuildSpec::Cargo(build)) = &mut node.build
        {
            "release".clone_into(&mut build.profile);
        }
    }
    Ok(plan)
}

pub(crate) fn materialize(
    plan: &DeploymentPlan,
    options: &BuildOptions,
    control: &Control,
    protocol: &Protocol,
) -> Result<PreparedDeployment> {
    let logs = plan.root.join(".reiny/logs");
    std::fs::create_dir_all(&logs)?;
    let (mut log, log_path) = tempfile::Builder::new()
        .prefix("prepare-")
        .suffix(".jsonl")
        .tempfile_in(logs)?
        .keep()
        .context("retaining preparation log")?;
    eprintln!("preparation log: {}", log_path.display());
    std::thread::scope(|scope| {
        let (sender, receiver) = mpsc::channel();
        let observer = Observer(sender);
        let writer = scope.spawn(|| {
            let result = (|| -> Result<()> {
                for event in receiver {
                    serde_json::to_writer(&mut log, &event)?;
                    log.write_all(b"\n")?;
                    log.flush()?;
                    match event {
                        PrepareEvent::Started | PrepareEvent::Finished { .. } => {}
                        PrepareEvent::Diagnostic { group, level, text } => {
                            if !protocol.enabled() {
                                eprintln!("{text}");
                            }
                            let mut start = text.len().saturating_sub(16 * 1024);
                            while !text.is_char_boundary(start) {
                                start += 1;
                            }
                            protocol.emit(&PrepareEvent::Diagnostic {
                                group,
                                level,
                                text: text[start..].to_owned(),
                            })?;
                        }
                        event => protocol.emit(&event)?,
                    }
                }
                Ok(())
            })();
            if let Err(error) = &result {
                control.request(Some(format!("writing preparation events: {error:#}")));
            }
            result
        });
        let result = plan.prepare_with(&PrepareOptions {
            cancel: control.cancel.clone(),
            observer: Some(&observer),
            jobs: options.jobs.map_or(1, NonZeroUsize::get),
            build_cache: options.build_cache.clone(),
        });
        drop(observer);
        writer
            .join()
            .map_err(|_| anyhow::anyhow!("preparation logger panicked"))??;
        result.map_err(anyhow::Error::from)
    })
}

pub(crate) fn prepare(
    path: &Path,
    out: Option<&Path>,
    options: &BuildOptions,
    control: &Control,
    protocol: &Protocol,
) -> Result<PreparedDeployment> {
    protocol.emit(&PrepareEvent::Started)?;
    let result = (|| {
        protocol.emit(&PrepareEvent::Phase {
            phase: PreparePhase::Resolving,
        })?;
        if let Some(out) = out {
            anyhow::ensure!(
                !out.exists(),
                "prepared output already exists: {}",
                out.display()
            );
        }
        let plan = load(path, options, control)?;
        let prepared = materialize(&plan, options, control, protocol)?;
        protocol.emit(&PrepareEvent::Phase {
            phase: PreparePhase::Writing,
        })?;
        // Verification and serialization can be large. Keep cancellation live
        // until only the absent-destination publication remains under the lock.
        let staging = out
            .map(|out| -> Result<_> {
                let parent = out
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                let staging = tempfile::Builder::new()
                    .prefix(".prepare-cli-")
                    .tempfile_in(parent)?
                    .into_temp_path();
                std::fs::remove_file(&staging)?;
                prepared.write_file(&staging)?;
                Ok(staging)
            })
            .transpose()?;
        control.publish(|| {
            if let (Some(staging), Some(out)) = (staging, out) {
                staging
                    .persist_noclobber(out)
                    .context("publishing prepared output")?;
            }
            if let Err(error) = protocol.emit(&PrepareEvent::Finished {
                status: PrepareStatus::Success,
                error: None,
            }) {
                if let Some(out) = out {
                    std::fs::remove_file(out).context("removing unpublished prepared output")?;
                }
                return Err(error);
            }
            Ok(prepared)
        })
    })();
    match result {
        Ok(prepared) => Ok(prepared),
        Err(error) => {
            let error = control.failure(error);
            let status = if matches!(
                error.downcast_ref::<PrepareError>(),
                Some(PrepareError::Cancelled)
            ) {
                PrepareStatus::Cancelled
            } else {
                PrepareStatus::Failed
            };
            protocol.emit(&PrepareEvent::Finished {
                status,
                error: Some(format!("{error:#}")),
            })?;
            Err(error)
        }
    }
}
