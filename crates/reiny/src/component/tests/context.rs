use super::*;
use crate::bindings::{EndpointContract, OutputBinding};
use crate::encoded::EncodedType;
use crate::engine::{Guard, Key, Presence, QueryParams};
use crate::{DeclaredPublisher, MessageType};

struct RetainingFactory(mpsc::Sender<ComponentContext>);

struct RetainingComponent {
    contexts: mpsc::Sender<ComponentContext>,
    output: Option<DeclaredPublisher>,
}

impl ComponentFactory for RetainingFactory {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        Ok(Box::new(RetainingComponent {
            contexts: self.0.clone(),
            output: None,
        }))
    }
}

impl Component for RetainingComponent {
    fn prepare<'a>(&'a mut self, context: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.output = Some(context.raw_output("out", &EncodedType::of::<graph::Probe>()?)?);
            self.contexts.send(context.clone()).await?;
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(std::future::pending())
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.output.take();
            Ok(())
        })
    }
}

fn observe(bus: &Local, key: &Key) -> crate::Result<(Guard, flume::Receiver<Presence>)> {
    let (events, received) = flume::unbounded();
    let guard = bus.watch_alive(
        key,
        Box::new(move |event| {
            let _ = events.send(event);
        }),
    )?;
    Ok((guard, received))
}

#[tokio::test]
async fn retained_context_shares_stop_and_releases_resources_on_last_drop() -> crate::Result<()> {
    let bus = Arc::new(Local::new());
    let launch_key = Key::launch("components", Some("retained"));
    let ready_key = Key::module("components", Some("retained"), "@ready");
    let (_launch_watch, launch_events) = observe(&bus, &launch_key)?;
    let (_ready_watch, ready_events) = observe(&bus, &ready_key)?;
    let (contexts, mut retained) = mpsc::channel(1);
    let mut registry = ComponentRegistry::new();
    registry.register("retain".into(), Arc::new(RetainingFactory(contexts)))?;
    let mut spec = instance("retained", "retain");
    spec.config_origin = Some(std::env::current_dir()?);
    spec.bindings.outputs.insert(
        "out".into(),
        OutputBinding {
            contract: EndpointContract {
                type_name: graph::Probe::TYPE_NAME.into(),
                ..EndpointContract::default()
            },
        },
    );
    let host = ComponentHost::launch(Arc::new(registry), options(bus.clone()), vec![spec])?;
    timeout(PATIENCE, host.wait_running()).await??;
    let retained = retained
        .recv()
        .await
        .ok_or_else(|| anyhow::anyhow!("context missing"))?;
    assert!(!retained.is_shutdown());
    assert_eq!(
        timeout(PATIENCE, launch_events.recv_async()).await??,
        Presence::Joined(launch_key.clone())
    );
    assert_eq!(
        timeout(PATIENCE, ready_events.recv_async()).await??,
        Presence::Joined(ready_key.clone())
    );

    // The ordinary OS thread owns its context across cancellation of the run future.
    let clone = retained.clone();
    let runtime = tokio::runtime::Handle::current();
    let (release, released) = std::sync::mpsc::channel();
    let (completed, completion) = tokio::sync::oneshot::channel();
    let lane = std::thread::spawn(move || {
        let result = (|| -> crate::Result<()> {
            released.recv_timeout(PATIENCE)?;
            anyhow::ensure!(clone.is_shutdown(), "clone did not observe shutdown");
            runtime.block_on(clone.shutdown());
            anyhow::ensure!(clone.id() == "retained", "identity changed");
            anyhow::ensure!(clone.config_dir().is_some(), "origin lost");
            anyhow::ensure!(
                clone
                    .raw_output("out", &EncodedType::of::<graph::Probe>()?)
                    .is_err(),
                "fresh port opened after shutdown"
            );
            Ok(())
        })();
        drop(clone);
        let _ = completed.send(result);
    });
    assert!(matches!(
        timeout(PATIENCE, host.stop()).await?,
        HostOutcome::Stopped
    ));
    assert!(retained.is_shutdown());
    timeout(PATIENCE, retained.shutdown()).await?;
    assert_eq!(
        timeout(PATIENCE, ready_events.recv_async()).await??,
        Presence::Left(ready_key.clone())
    );
    release.send(())?;
    let result = timeout(PATIENCE, completion).await??;
    lane.join()
        .map_err(|_| anyhow::anyhow!("owned context lane panicked"))?;
    result?;
    assert_eq!(
        bus.alive(&ready_key, PATIENCE).await?,
        Vec::<crate::engine::Key>::new()
    );
    assert_eq!(
        bus.alive(&launch_key, PATIENCE).await?,
        vec![launch_key.clone()]
    );

    let runtime_lifetime = Arc::downgrade(&retained.inner);
    drop(retained);
    assert!(runtime_lifetime.upgrade().is_none());
    assert_eq!(
        timeout(PATIENCE, launch_events.recv_async()).await??,
        Presence::Left(launch_key)
    );
    let mut replies = bus.query(
        &Key::module("components", Some("retained"), "@stop"),
        QueryParams {
            payload: None,
            attachment: None,
            timeout: PATIENCE,
        },
    )?;
    assert!(timeout(PATIENCE, replies.next()).await?.is_none());
    Ok(())
}
