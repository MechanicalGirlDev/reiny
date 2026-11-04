use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};

use super::*;
use crate::{
    bindings::{MODULE_BINDINGS_VERSION, ModuleBindings},
    component::{
        ComponentHost, ComponentInstanceSpec, ComponentRegistry, HostOptions, HostOutcome,
        HostPhase,
    },
    step::{
        ClockPhase, NodeError, StepFailureKind, StepGraph, StepInput, StepOutput, StepSchema,
        StepSource, StepType, StepValue,
    },
};

mod lifecycle;

const BUDGET: Duration = Duration::from_secs(5);

#[derive(Clone, PartialEq, prost::Message)]
struct Number {
    #[prost(sint64, tag = "1")]
    value: i64,
}

impl crate::MessageType for Number {
    const TYPE_NAME: &'static str = "host_step.Number";
    const SCHEMA_FINGERPRINT: u64 = 42;
    const DESCRIPTOR: Option<crate::Descriptor> = None;
}

fn value(value: i64) -> StepValue {
    StepValue::encode(&Number { value })
}
fn input(instance: &str, port: &str) -> StepInput {
    StepInput {
        instance: instance.into(),
        port: port.into(),
    }
}
fn output(instance: &str) -> StepOutput {
    StepOutput {
        instance: instance.into(),
        port: "out".into(),
    }
}
fn schema(inputs: &[&str]) -> StepSchema {
    StepSchema {
        inputs: inputs
            .iter()
            .map(|port| ((*port).into(), StepType::of::<Number>()))
            .collect(),
        outputs: BTreeMap::from([("out".into(), StepType::of::<Number>())]),
    }
}
pub(super) fn graph() -> StepGraph {
    StepGraph {
        nodes: BTreeMap::from([
            ("controller".into(), schema(&["action", "state"])),
            ("physics".into(), schema(&["command"])),
        ]),
        inputs: BTreeMap::from([
            (input("controller", "action"), StepSource::External),
            (
                input("controller", "state"),
                StepSource::PreviousTick {
                    source: output("physics"),
                    initial: value(10),
                },
            ),
            (
                input("physics", "command"),
                StepSource::SameTick(output("controller")),
            ),
        ]),
    }
}
pub(super) fn request() -> StepRequest {
    StepRequest {
        dt: Duration::from_secs(1),
        inputs: BTreeMap::from([(input("controller", "action"), value(2))]),
    }
}

#[derive(Clone, Copy)]
enum Behavior {
    Sum,
    Wait,
    Error,
    Panic,
    Invalid,
    LoseReady,
    StopWait,
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Enter(String),
    Stop(String),
    Drop(String),
}

struct Factory {
    behavior: Behavior,
    events: mpsc::UnboundedSender<Event>,
    release: watch::Receiver<bool>,
    constructions: Arc<AtomicUsize>,
}

impl StepComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn StepComponent>> {
        self.constructions.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Node {
            behavior: self.behavior,
            events: self.events.clone(),
            release: self.release.clone(),
            id: String::new(),
            publisher: None,
        }))
    }
}

struct Node {
    behavior: Behavior,
    events: mpsc::UnboundedSender<Event>,
    release: watch::Receiver<bool>,
    id: String,
    publisher: Option<crate::Publisher<Number>>,
}

impl StepComponent for Node {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.id = ctx.id().into();
            if matches!(self.behavior, Behavior::LoseReady) {
                self.publisher = Some(ctx.output("presence")?);
            }
            Ok(())
        })
    }
    fn step<'a>(
        &'a mut self,
        _: &'a ComponentContext,
        _: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let _ = self.events.send(Event::Enter(self.id.clone()));
            match self.behavior {
                Behavior::Wait | Behavior::StopWait => {
                    self.release
                        .wait_for(|release| *release)
                        .await
                        .map_err(|error| NodeError {
                            message: error.to_string(),
                        })?;
                }
                Behavior::Error => {
                    return Err(NodeError {
                        message: "callback failure".into(),
                    });
                }
                Behavior::Panic => std::panic::resume_unwind(Box::new("callback unwind")),
                Behavior::Invalid => return Ok(BTreeMap::new()),
                Behavior::LoseReady => {
                    drop(self.publisher.take());
                }
                Behavior::Sum => {}
            }
            let sum = inputs
                .values()
                .try_fold(1, |sum, value| {
                    value.decode::<Number>().map(|value| sum + value.value)
                })
                .map_err(|error| NodeError {
                    message: error.to_string(),
                })?;
            Ok(BTreeMap::from([("out".into(), value(sum))]))
        })
    }
    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async move {
            let _ = self.events.send(Event::Stop(self.id.clone()));
            if matches!(self.behavior, Behavior::StopWait) {
                self.release.wait_for(|release| *release).await?;
            }
            Ok(())
        })
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.events.send(Event::Drop(self.id.clone()));
    }
}

struct Fixture {
    registry: Arc<ComponentRegistry>,
    instances: Vec<ComponentInstanceSpec>,
    options: HostOptions,
    events: mpsc::UnboundedReceiver<Event>,
    release: watch::Sender<bool>,
    constructions: Arc<AtomicUsize>,
}

fn fixture(behavior: Behavior) -> crate::Result<Fixture> {
    let (events, received) = mpsc::unbounded_channel();
    let (release, released) = watch::channel(false);
    let constructions = Arc::new(AtomicUsize::new(0));
    let mut registry = ComponentRegistry::new();
    let mut instances = Vec::new();
    for (id, behavior) in [("controller", behavior), ("physics", Behavior::Sum)] {
        registry.register_step(
            id.into(),
            graph().nodes[id].clone(),
            Arc::new(Factory {
                behavior,
                events: events.clone(),
                release: released.clone(),
                constructions: constructions.clone(),
            }),
        )?;
        let mut bindings = ModuleBindings {
            version: MODULE_BINDINGS_VERSION,
            namespace: id.into(),
            endpoint_namespace: id.into(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            children: BTreeMap::new(),
            executables: Vec::new(),
        };
        if matches!(behavior, Behavior::LoseReady) {
            bindings.outputs.insert(
                "presence".into(),
                crate::bindings::OutputBinding {
                    contract: crate::bindings::EndpointContract {
                        type_name: <Number as crate::MessageType>::TYPE_NAME.into(),
                        ..crate::bindings::EndpointContract::default()
                    },
                },
            );
        }
        instances.push(ComponentInstanceSpec {
            factory_id: id.into(),
            bindings,
            config: None,
            config_origin: None,
        });
    }
    Ok(Fixture {
        registry: Arc::new(registry),
        instances,
        options: HostOptions {
            domain: "steps".into(),
            engine: Arc::new(crate::engine::Local::new()),
            startup_timeout: BUDGET,
            stop_timeout: BUDGET,
        },
        events: received,
        release,
        constructions,
    })
}

async fn event(events: &mut mpsc::UnboundedReceiver<Event>, expected: Event) -> crate::Result<()> {
    let observed = tokio::time::timeout(BUDGET, events.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("event stream closed"))?;
    assert_eq!(observed, expected);
    Ok(())
}

fn failed(
    result: Result<Arc<StepReceipt>, StepRunError>,
    kind: StepFailureKind,
) -> crate::Result<StepFailure> {
    let Err(StepRunError::Failed(failure)) = result else {
        anyhow::bail!("expected failed step, got {result:?}");
    };
    assert_eq!(failure.kind, kind);
    Ok(*failure)
}

#[tokio::test]
async fn real_graph_two_ticks_and_repeatable_wait() -> crate::Result<()> {
    let fixture = fixture(Behavior::Sum)?;
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    assert!(matches!(
        steps.try_step(request(), BUDGET),
        Err(StepStartError::NotRunning)
    ));
    host.wait_running().await?;
    for (index, expected) in [(0, 14), (1, 18)] {
        let run = steps.try_step(request(), BUDGET)?;
        // An unpolled waiter is harmless; execution is owned by the host.
        drop(run.wait());
        let receipt = tokio::time::timeout(BUDGET, run.wait()).await??;
        assert_eq!(receipt.tick.id.index, index);
        assert_eq!(
            receipt.outputs[&output("physics")]
                .decode::<Number>()?
                .value,
            expected
        );
        assert!(Arc::ptr_eq(&receipt, &run.wait().await?));
        assert_eq!(run.cancel(), CancelStatus::Finished);
        assert_eq!(steps.clock().time, Duration::from_secs(index + 1));
    }
    assert!(matches!(host.stop().await, HostOutcome::Stopped));
    assert_eq!(steps.clock().phase, ClockPhase::Stopped);
    Ok(())
}

#[tokio::test]
async fn busy_cancel_before_dispatch_then_retry_same_tick() -> crate::Result<()> {
    let fixture = fixture(Behavior::Sum)?;
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step(request(), BUDGET)?;
    assert!(
        matches!(steps.try_step(request(), BUDGET), Err(StepStartError::Busy { in_flight }) if in_flight == run.id())
    );
    assert_eq!(run.cancel(), CancelStatus::Requested);
    assert_eq!(run.cancel(), CancelStatus::AlreadyRequested);
    assert_eq!(
        run.wait().await,
        Err(StepRunError::CancelledBeforeDispatch { tick: run.tick() })
    );
    let retry = steps.try_step(request(), BUDGET)?;
    assert_eq!(run.tick(), retry.tick());
    assert_ne!(run.id(), retry.id());
    retry.wait().await?;
    assert!(matches!(host.stop().await, HostOutcome::Stopped));
    Ok(())
}

#[tokio::test]
async fn invalid_requests_and_schema_reject_without_construction_or_dispatch() -> crate::Result<()>
{
    let fixture = fixture(Behavior::Sum)?;
    let mut invalid = graph();
    invalid
        .nodes
        .get_mut("controller")
        .ok_or_else(|| anyhow::anyhow!("missing controller"))?
        .outputs
        .clear();
    let result = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        invalid,
        Duration::ZERO,
    );
    assert!(matches!(
        result,
        Err(StepLaunchError::SchemaMismatch { .. })
    ));
    assert_eq!(fixture.constructions.load(Ordering::SeqCst), 0);
    let fixture = self::fixture(Behavior::Sum)?;
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let mut invalid = request();
    invalid.dt = Duration::ZERO;
    assert!(matches!(
        steps.try_step(invalid, BUDGET),
        Err(StepStartError::InvalidRequest(StepRequestError::ZeroDt))
    ));
    assert!(matches!(
        steps.try_step(request(), Duration::ZERO),
        Err(StepStartError::InvalidTimeout)
    ));
    assert_eq!(steps.clock().next_index, 0);
    assert!(matches!(host.stop().await, HostOutcome::Stopped));
    Ok(())
}
