use super::*;
use std::sync::{Condvar, Mutex, PoisonError};

#[tokio::test]
async fn startup_deadline_fails_after_joining_started_tasks() {
    let (registry, mut records, _gate) = fixture(Behavior::PrepareWait);
    let mut options = options(Arc::new(Local::new()));
    options.startup_timeout = Duration::from_millis(50);
    let handle = ComponentHost::launch(registry, options, vec![instance("app/wait", "test")])
        .expect("launch");
    let (primary, cleanup) = failure(terminal(&handle).await);

    assert_eq!(primary.code, HostErrorCode::Timeout);
    assert_eq!(cleanup, Vec::<HostError>::new());
    assert_eq!(
        handle.status().components["app/wait"].phase,
        ComponentPhase::Stopped
    );
    while let Ok(record) = records.try_recv() {
        assert_ne!(record.stage, Stage::Run);
    }
}

#[tokio::test]
async fn a_stop_hook_that_never_finishes_is_unresponsive() {
    let (registry, mut records, _gate) = fixture(Behavior::StopWait);
    let mut options = options(Arc::new(Local::new()));
    options.stop_timeout = Duration::from_millis(20);
    let handle = ComponentHost::launch(registry, options, vec![instance("app/wait", "test")])
        .expect("launch");
    handle.wait_running().await.expect("running");

    let result = timeout(PATIENCE, handle.stop())
        .await
        .expect("bounded stop");

    match result {
        HostOutcome::Unresponsive {
            primary,
            cleanup,
            instances,
        } => {
            assert!(primary.is_none());
            assert_eq!(cleanup, Vec::<HostError>::new());
            assert_eq!(instances, vec!["app/wait"]);
        }
        HostOutcome::Stopped | HostOutcome::Failed { .. } => {
            unreachable!("must remain unresponsive")
        }
    }
    assert_eq!(
        handle.status().components["app/wait"].phase,
        ComponentPhase::Unresponsive
    );
    stage(&mut records, Stage::Dropped).await;
}

#[derive(Default)]
struct BlockingGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl BlockingGate {
    fn block(&self) {
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

struct ReleaseOnDrop(Arc<BlockingGate>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct BlockingFactory {
    gate: Arc<BlockingGate>,
    records: mpsc::Sender<Record>,
}

struct Blocking {
    gate: Arc<BlockingGate>,
    records: mpsc::Sender<Record>,
}

impl ComponentFactory for BlockingFactory {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        Ok(Box::new(Blocking {
            gate: self.gate.clone(),
            records: self.records.clone(),
        }))
    }
}

impl Component for Blocking {
    fn prepare<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.records.try_send(Record {
                id: ctx.id().to_owned(),
                stage: Stage::Run,
            })?;
            // Deliberately violates the cooperative contract to exercise honest timeout reporting.
            self.gate.block();
            std::future::pending().await
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

impl Drop for Blocking {
    fn drop(&mut self) {
        self.records
            .try_send(Record {
                id: "app/native".to_owned(),
                stage: Stage::Dropped,
            })
            .expect("drop signal");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocking_native_work_is_reported_unjoined_until_the_test_releases_it() {
    let gate = Arc::new(BlockingGate::default());
    let release = ReleaseOnDrop(gate.clone());
    let (records, mut rx) = mpsc::channel(4);
    let mut registry = ComponentRegistry::new();
    registry
        .register(
            "native".to_owned(),
            Arc::new(BlockingFactory { gate, records }),
        )
        .expect("register");
    let mut options = options(Arc::new(Local::new()));
    options.stop_timeout = Duration::from_millis(20);
    let handle = ComponentHost::launch(
        Arc::new(registry),
        options,
        vec![instance("app/native", "native")],
    )
    .expect("launch");
    stage(&mut rx, Stage::Run).await;

    let outcome = timeout(PATIENCE, handle.stop())
        .await
        .expect("supervisor remains responsive");

    assert!(
        matches!(outcome, HostOutcome::Unresponsive { instances, .. } if instances == ["app/native"])
    );
    assert!(
        rx.try_recv().is_err(),
        "native component is still owned by its blocked task"
    );
    drop(release);
    stage(&mut rx, Stage::Dropped).await;
    assert_eq!(
        handle.status().components["app/native"].phase,
        ComponentPhase::Unresponsive
    );
}
