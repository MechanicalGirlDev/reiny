use super::*;
use std::sync::{Condvar, Mutex, PoisonError};

use crate::Publisher;
use crate::bindings::{EndpointContract, OutputBinding};

use super::super::graph::Probe;

#[derive(Default)]
struct CommitBarrier {
    released: Mutex<bool>,
    changed: Condvar,
}

impl CommitBarrier {
    fn wait(&self) {
        let mut released = self.released.lock().unwrap_or_else(PoisonError::into_inner);
        while !*released {
            released = self
                .changed
                .wait(released)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_all();
    }
}

struct Release(Arc<CommitBarrier>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct ExposedFactory {
    output: Arc<Mutex<Option<Publisher<Probe>>>>,
    records: mpsc::Sender<Record>,
}

struct Exposed {
    output: Arc<Mutex<Option<Publisher<Probe>>>>,
    records: mpsc::Sender<Record>,
}

impl ComponentFactory for ExposedFactory {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        Ok(Box::new(Exposed {
            output: self.output.clone(),
            records: self.records.clone(),
        }))
    }
}

impl Component for Exposed {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            *self.output.lock().unwrap_or_else(PoisonError::into_inner) = Some(ctx.output("out")?);
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.records
                .send(Record {
                    id: ctx.id().to_owned(),
                    stage: Stage::Run,
                })
                .await?;
            std::future::pending().await
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loss_of_first_readiness_during_second_commit_prevents_every_run() {
    let barrier = Arc::new(CommitBarrier::default());
    let release = Release(barrier.clone());
    let (entered, mut entering) = watch::channel(false);
    let engine = Arc::new(SecondReady {
        local: Local::new(),
        attempts: AtomicUsize::new(0),
        on_second: Box::new(move || {
            entered.send_replace(true);
            barrier.wait();
            Ok(())
        }),
    });
    let output = Arc::new(Mutex::new(None));
    let (records, mut observed) = mpsc::channel(16);
    let (_gate, gate) = watch::channel(false);
    let mut registry = ComponentRegistry::new();
    registry
        .register(
            "exposed".to_owned(),
            Arc::new(ExposedFactory {
                output: output.clone(),
                records: records.clone(),
            }),
        )
        .expect("source factory");
    registry
        .register(
            "peer".to_owned(),
            Arc::new(Factory {
                behavior: Behavior::Idle,
                records,
                gate,
            }),
        )
        .expect("peer factory");
    let mut source = instance("app/first", "exposed");
    source.bindings.outputs.insert(
        "out".to_owned(),
        OutputBinding {
            contract: EndpointContract {
                type_name: Probe::TYPE_NAME.to_owned(),
                ..EndpointContract::default()
            },
        },
    );
    let handle = ComponentHost::launch(
        Arc::new(registry),
        options(engine.clone()),
        vec![source, instance("app/second", "peer")],
    )
    .expect("launch");
    timeout(PATIENCE, entering.wait_for(|entered| *entered))
        .await
        .expect("second commit deadline")
        .expect("second commit entered");

    // The first readiness is committed; the second commit is paused in its transport.
    drop(output.lock().unwrap_or_else(PoisonError::into_inner).take());
    drop(release);

    let (primary, cleanup) = failure(terminal(&handle).await);
    assert_eq!(primary.code, HostErrorCode::ReadinessLost);
    assert_eq!(primary.instance.as_deref(), Some("app/first"));
    assert_eq!(cleanup, Vec::<HostError>::new());
    while let Ok(record) = observed.try_recv() {
        assert_ne!(record.stage, Stage::Run);
    }
    assert_eq!(handle.status().phase, HostPhase::Failed);
    assert_eq!(
        engine
            .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
            .await
            .expect("readiness withdrawn"),
        Vec::<Key>::new()
    );
}
