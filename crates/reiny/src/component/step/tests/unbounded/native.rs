use super::*;
use crate::step::StepOutputs;
use std::sync::{Condvar, Mutex, PoisonError};

#[path = "native/driver.rs"]
mod driver;

#[derive(Default)]
struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
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

struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Clone)]
struct NativeLifetime {
    callback: Arc<Gate>,
    destructor: Arc<Gate>,
    events: mpsc::UnboundedSender<&'static str>,
}

struct Native(NativeLifetime);

impl StepComponentFactory for NativeLifetime {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn StepComponent>> {
        Ok(Box::new(Native(self.clone())))
    }
}

impl StepComponent for Native {
    fn prepare<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn step<'a>(
        &'a mut self,
        _: &'a ComponentContext,
        _: StepTick,
        _: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async {
            self.0.events.send("callback").map_err(|error| NodeError {
                message: error.to_string(),
            })?;
            // Deliberately non-cooperative native callback, released by the test.
            self.0.callback.block();
            Ok(BTreeMap::new())
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

impl Drop for Native {
    fn drop(&mut self) {
        if self.0.events.send("destructor").is_err() {
            return;
        }
        self.0.destructor.block();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unbounded_stop_joins_native_callback_and_component_destructor() -> crate::Result<()> {
    let callback = Arc::new(Gate::default());
    let destructor = Arc::new(Gate::default());
    let release_callback = Release(callback.clone());
    let release_destructor = Release(destructor.clone());
    let (events, mut received) = mpsc::unbounded_channel();
    let mut registry = ComponentRegistry::new();
    let schema = StepSchema {
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
    };
    registry.register_step(
        "native".into(),
        schema.clone(),
        Arc::new(NativeLifetime {
            callback,
            destructor,
            events,
        }),
    )?;
    let bindings = ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: "native".into(),
        endpoint_namespace: "native".into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    };
    let (host, steps) = ComponentHost::launch_local_with_steps(
        Arc::new(registry),
        local_options(),
        vec![ComponentInstanceSpec {
            factory_id: "native".into(),
            bindings,
            config: None,
            config_origin: None,
        }],
        StepGraph {
            nodes: BTreeMap::from([("native".into(), schema)]),
            inputs: BTreeMap::new(),
        },
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step_with_timeout(
        StepRequest {
            dt: Duration::from_secs(1),
            inputs: BTreeMap::new(),
        },
        None,
    )?;
    assert_eq!(
        tokio::time::timeout(BUDGET, received.recv()).await?,
        Some("callback")
    );
    let mut stopping = Box::pin(host.stop());
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(stopping.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    failed(
        tokio::time::timeout(BUDGET, run.wait()).await?,
        StepFailureKind::HostStopped,
    )?;
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(stopping.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(release_callback);
    assert_eq!(
        tokio::time::timeout(BUDGET, received.recv()).await?,
        Some("destructor")
    );
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(stopping.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(release_destructor);
    assert!(matches!(
        tokio::time::timeout(BUDGET, stopping).await?,
        HostOutcome::Stopped
    ));
    Ok(())
}

struct Remote;
impl ManagedStepExecutor for Remote {
    fn invoke<'a>(
        &'a mut self,
        _: StepDispatchContext,
        _: &'a str,
        _: StepInputs,
    ) -> ManagedStepFuture<'a> {
        Box::pin(std::future::pending::<Result<StepOutputs, DispatchFailure>>())
    }
}

#[tokio::test]
async fn session_handles_reject_unbounded_without_reserving_the_slot() -> crate::Result<()> {
    let (session, steps) = ManagedStepSession::prepare(graph(), Duration::ZERO, Box::new(Remote))?;
    session.activate();
    assert!(matches!(
        steps.try_step_with_timeout(request(), None),
        Err(StepStartError::InvalidTimeout)
    ));
    let run = steps.try_step(request(), BUDGET)?;
    assert_eq!(run.cancel(), CancelStatus::Requested);
    assert_eq!(
        run.wait().await,
        Err(StepRunError::CancelledBeforeDispatch { tick: run.tick() })
    );
    session.stop().await?;
    Ok(())
}
