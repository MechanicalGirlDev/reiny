//! Explicit job ownership through actual component startup and shutdown.
//! Run: `cargo run -p reiny --no-default-features --example component_owned_jobs`

use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use reiny::{
    bindings::{MODULE_BINDINGS_VERSION, ModuleBindings},
    component::{
        Component, ComponentContext, ComponentFactory, ComponentFuture, ComponentHost,
        ComponentInstanceSpec, ComponentRegistry, HostOptions, HostOutcome, StopReason,
    },
    job::{JobContext, JobHandle, JobOutcome, JobOwner},
};
use tokio::{sync::mpsc, time::timeout};

const DEADLINE: Duration = Duration::from_secs(5);
type Work = JobHandle<usize, u64, std::convert::Infallible>;

struct Factory {
    started: mpsc::Sender<Work>,
}

struct Worker {
    jobs: JobOwner,
    started: mpsc::Sender<Work>,
}

impl ComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        Ok(Box::new(Worker {
            jobs: JobOwner::new(NonZeroUsize::MIN),
            started: self.started.clone(),
        }))
    }
}

impl Component for Worker {
    fn prepare<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            let job = self.jobs.start(
                |work: JobContext<usize, u64, std::convert::Infallible>| async move {
                    let samples = [2_u64, 3, 5, 7, 11, 13];
                    let energy = samples.iter().map(|sample| sample * sample).sum();
                    work.report(samples.len());
                    // Model an owned service job whose resources remain live
                    // until cancellation, not a detached component borrow.
                    work.cancelled().await;
                    Ok(energy)
                },
            )?;
            self.started.send(job).await?;
            ctx.shutdown().await;
            Ok(())
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async move {
            let report = self.jobs.shutdown(DEADLINE).await?;
            ensure!(
                report.unresponsive.is_empty() && report.join_failures.is_empty(),
                "owned job cleanup incomplete: {report:?}"
            );
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (started, mut jobs) = mpsc::channel(1);
    let mut registry = ComponentRegistry::new();
    registry.register("work".into(), Arc::new(Factory { started }))?;
    let host = ComponentHost::launch(
        Arc::new(registry),
        HostOptions {
            domain: "component-owned-jobs".into(),
            engine: Arc::new(reiny::engine::Local::new()),
            startup_timeout: DEADLINE,
            stop_timeout: DEADLINE,
        },
        vec![ComponentInstanceSpec {
            factory_id: "work".into(),
            bindings: ModuleBindings {
                version: MODULE_BINDINGS_VERSION,
                namespace: "worker".into(),
                endpoint_namespace: "worker".into(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                children: BTreeMap::new(),
                executables: Vec::new(),
            },
            config: None,
            config_origin: None,
        }],
    )?;
    timeout(DEADLINE, host.wait_running()).await??;
    let mut job = timeout(DEADLINE, jobs.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("component did not start its job"))?;
    timeout(DEADLINE, async {
        while job.snapshot().progress.as_deref() != Some(&6) {
            job.changed().await;
        }
    })
    .await?;
    // Host stop must run the component's explicit owner shutdown. An observer
    // neither keeps the component alive nor certifies native teardown itself.
    ensure!(
        matches!(timeout(DEADLINE, host.stop()).await?, HostOutcome::Stopped),
        "component shutdown failed"
    );
    ensure!(
        matches!(
            timeout(DEADLINE, job.wait()).await?.as_ref(),
            JobOutcome::Cancelled
        ),
        "component stop did not cancel its owned job"
    );
    println!("component-owned job: progress observed, cancelled, and joined in stop");
    Ok(())
}
