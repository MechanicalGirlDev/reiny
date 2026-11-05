//! Managed controller/physics stepping with explicit external input and feedback.
//! Local startup and joined cleanup have no deadline; each step selects its budget.
//! Run: `cargo run -p reiny --no-default-features --example component_steps`

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use reiny::{
    MessageType,
    bindings::{MODULE_BINDINGS_VERSION, ModuleBindings},
    component::{
        ComponentContext, ComponentFuture, ComponentHost, ComponentInstanceSpec, ComponentRegistry,
        HostOutcome, LocalHostOptions, StopReason,
        step::{StepComponent, StepComponentFactory},
    },
    step::{
        NodeError, StepFuture, StepGraph, StepInput, StepInputs, StepOutput, StepRequest,
        StepSchema, StepSource, StepTick, StepType, StepValue,
    },
};

#[derive(Clone, PartialEq, prost::Message)]
struct Scalar {
    #[prost(sint64, tag = "1")]
    value: i64,
}

impl MessageType for Scalar {
    const TYPE_NAME: &'static str = "managed_step.Scalar";
    const SCHEMA_FINGERPRINT: u64 = 0x5354_4550;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

struct Factory {
    physics: bool,
}
struct Node {
    physics: bool,
    position: i64,
}

impl StepComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> reiny::Result<Box<dyn StepComponent>> {
        Ok(Box::new(Node {
            physics: self.physics,
            position: 0,
        }))
    }
}

impl StepComponent for Node {
    fn prepare<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn step<'a>(
        &'a mut self,
        _: &'a ComponentContext,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let read = |port: &str| -> Result<i64, NodeError> {
                inputs
                    .get(port)
                    .ok_or_else(|| NodeError {
                        message: format!("missing {port}"),
                    })?
                    .decode::<Scalar>()
                    .map(|value| value.value)
                    .map_err(|error| NodeError {
                        message: error.to_string(),
                    })
            };
            let overflow = || NodeError {
                message: "simulation arithmetic overflow".into(),
            };
            let (port, value) = if self.physics {
                let seconds = i64::try_from(tick.dt.as_secs()).map_err(|_| overflow())?;
                let delta = read("command")?.checked_mul(seconds).ok_or_else(overflow)?;
                self.position = self.position.checked_add(delta).ok_or_else(overflow)?;
                ("state", self.position)
            } else {
                (
                    "command",
                    read("target")?
                        .checked_sub(read("state")?)
                        .ok_or_else(overflow)?,
                )
            };
            Ok(BTreeMap::from([(
                port.into(),
                StepValue::encode(&Scalar { value }),
            )]))
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

fn input(instance: &str, port: &str) -> StepInput {
    StepInput {
        instance: instance.into(),
        port: port.into(),
    }
}
fn output(instance: &str, port: &str) -> StepOutput {
    StepOutput {
        instance: instance.into(),
        port: port.into(),
    }
}
fn schema(inputs: &[&str], output: &str) -> StepSchema {
    StepSchema {
        inputs: inputs
            .iter()
            .map(|port| ((*port).into(), StepType::of::<Scalar>()))
            .collect(),
        outputs: BTreeMap::from([(output.into(), StepType::of::<Scalar>())]),
    }
}
fn instance(id: &str) -> ComponentInstanceSpec {
    ComponentInstanceSpec {
        factory_id: id.into(),
        bindings: ModuleBindings {
            version: MODULE_BINDINGS_VERSION,
            namespace: id.into(),
            endpoint_namespace: id.into(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            children: BTreeMap::new(),
            executables: Vec::new(),
        },
        config: None,
        config_origin: None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let graph = StepGraph {
        nodes: BTreeMap::from([
            ("controller".into(), schema(&["target", "state"], "command")),
            ("physics".into(), schema(&["command"], "state")),
        ]),
        inputs: BTreeMap::from([
            (input("controller", "target"), StepSource::External),
            (
                input("controller", "state"),
                StepSource::PreviousTick {
                    source: output("physics", "state"),
                    initial: StepValue::encode(&Scalar { value: 0 }),
                },
            ),
            (
                input("physics", "command"),
                StepSource::SameTick(output("controller", "command")),
            ),
        ]),
    };
    let mut registry = ComponentRegistry::new();
    registry.register_step(
        "controller".into(),
        graph.nodes["controller"].clone(),
        Arc::new(Factory { physics: false }),
    )?;
    registry.register_step(
        "physics".into(),
        graph.nodes["physics"].clone(),
        Arc::new(Factory { physics: true }),
    )?;
    let (host, steps) = ComponentHost::launch_local_with_steps(
        Arc::new(registry),
        LocalHostOptions {
            domain: "managed-steps".into(),
            engine: Arc::new(reiny::engine::Local::new()),
            startup_timeout: None,
            stop_timeout: None,
        },
        vec![instance("controller"), instance("physics")],
        graph,
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    for (target, expected_command, timeout) in [(5, 5, Some(Duration::from_secs(5))), (8, 3, None)]
    {
        let run = steps.try_step_with_timeout(
            StepRequest {
                dt: Duration::from_secs(1),
                inputs: BTreeMap::from([(
                    input("controller", "target"),
                    StepValue::encode(&Scalar { value: target }),
                )]),
            },
            timeout,
        )?;
        let receipt = run.wait().await?;
        let command = receipt.outputs[&output("controller", "command")]
            .decode::<Scalar>()?
            .value;
        let state = receipt.outputs[&output("physics", "state")]
            .decode::<Scalar>()?
            .value;
        ensure!(
            command == expected_command && state == target,
            "unexpected feedback result"
        );
        println!(
            "epoch={} tick={} [{:?}, {:?}] command={command} state={state}",
            receipt.tick.id.epoch.get(),
            receipt.tick.id.index,
            receipt.tick.start,
            receipt.tick.end,
        );
    }
    ensure!(
        matches!(host.stop().await, HostOutcome::Stopped),
        "host cleanup failed"
    );
    Ok(())
}
