use super::*;
use crate::component::step::{
    control,
    driver::{Driver, DriverExecutor},
    worker,
};
use crate::step::{StepExecutor, StepScheduler};

struct Executor {
    entered: mpsc::UnboundedSender<&'static str>,
    released: watch::Receiver<bool>,
    destructor: Arc<Gate>,
}

impl StepExecutor for Executor {
    fn invoke<'a>(&'a mut self, _: &'a str, _: StepTick, _: StepInputs) -> StepFuture<'a> {
        Box::pin(std::future::pending())
    }
}

impl DriverExecutor for Executor {
    fn begin(&mut self, _: &control::Run) {}

    fn failure(&self) -> Option<StepFailureKind> {
        None
    }

    fn shutdown(&mut self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {
            if self.entered.send("shutdown").is_ok() {
                match self.released.wait_for(|released| *released).await {
                    Ok(_) | Err(_) => {}
                }
            }
        })
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        if self.entered.send("driver destructor").is_ok() {
            self.destructor.block();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unbounded_host_stop_joins_driver_shutdown_and_disposal() -> crate::Result<()> {
    let fixture = fixture(Behavior::Sum)?;
    let mut registry = ComponentRegistry::new();
    let mut senders = Vec::new();
    for (id, (_, factory)) in &fixture.registry.step_factories {
        let (calls, received) = mpsc::unbounded_channel();
        senders.push(calls);
        registry.register(
            id.clone(),
            Arc::new(worker::Factory {
                implementation: factory.clone(),
                calls: Mutex::new(Some(received)),
                prepared: None,
            }),
        )?;
    }
    let scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
    let gate = control::Gate::new(scheduler.clock());
    let (_requests, received) = mpsc::unbounded_channel();
    let (entered, mut events) = mpsc::unbounded_channel();
    let (release, released) = watch::channel(false);
    let destructor = Arc::new(Gate::default());
    let release_destructor = Release(destructor.clone());
    let driver = Driver {
        scheduler,
        gate,
        requests: received,
        executor: Box::new(Executor {
            entered,
            released,
            destructor,
        }),
    };
    let bindings = fixture
        .instances
        .iter()
        .map(|instance| instance.bindings.clone())
        .collect();
    let (host, control) = ComponentHost::prepare_steps(
        Arc::new(registry),
        local_options().into(),
        fixture.instances,
        Some(driver),
    )?;
    tokio::spawn(control.admit_local(bindings));
    host.wait_running().await?;
    let mut stopping = Box::pin(host.stop());
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(stopping.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert_eq!(
        tokio::time::timeout(BUDGET, events.recv()).await?,
        Some("shutdown")
    );
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(stopping.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    release.send_replace(true);
    assert_eq!(
        tokio::time::timeout(BUDGET, events.recv()).await?,
        Some("driver destructor")
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
    drop(senders);
    Ok(())
}
