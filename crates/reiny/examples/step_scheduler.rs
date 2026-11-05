//! Pure local controller/physics stepping, with no component host or transport.
//! Run: `cargo run -p reiny --no-default-features --example step_scheduler`

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, ensure};
use reiny::{
    Descriptor, MessageType,
    step::{
        NodeError, StepExecutor, StepFuture, StepGraph, StepInput, StepInputs, StepOutput,
        StepRequest, StepScheduler, StepSchema, StepSource, StepTick, StepType, StepValue,
    },
};

#[derive(Clone, PartialEq, prost::Message)]
struct Scalar {
    #[prost(sint64, tag = "1")]
    value: i64,
}

// Self-contained example metadata. Applications normally generate this contract
// from their protobuf catalog with reiny-build.
impl MessageType for Scalar {
    const TYPE_NAME: &'static str = "step_example.Scalar";
    const SCHEMA_FINGERPRINT: u64 = 0x5354_4550_5343_414c;
    const DESCRIPTOR: Option<Descriptor> = None;
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

struct Simulation {
    position: i64,
}

impl StepExecutor for Simulation {
    fn invoke<'a>(
        &'a mut self,
        instance: &'a str,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let read = |port: &str| -> Result<i64, NodeError> {
                let value = inputs.get(port).ok_or_else(|| NodeError {
                    message: format!("missing input {port}"),
                })?;
                value
                    .decode::<Scalar>()
                    .map(|value| value.value)
                    .map_err(|error| NodeError {
                        message: error.to_string(),
                    })
            };
            let overflow = || NodeError {
                message: "simulation arithmetic overflow".into(),
            };
            let (port, value) = match instance {
                "controller" => (
                    "command",
                    read("target")?
                        .checked_sub(read("state")?)
                        .ok_or_else(overflow)?,
                ),
                "physics" => {
                    let seconds = i64::try_from(tick.dt.as_secs()).map_err(|_| overflow())?;
                    let change = read("command")?.checked_mul(seconds).ok_or_else(overflow)?;
                    self.position = self.position.checked_add(change).ok_or_else(overflow)?;
                    ("state", self.position)
                }
                other => {
                    return Err(NodeError {
                        message: format!("unknown node {other}"),
                    });
                }
            };
            Ok(BTreeMap::from([(
                port.into(),
                StepValue::encode(&Scalar { value }),
            )]))
        })
    }
}

#[tokio::main(flavor = "current_thread")]
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
    let mut scheduler = StepScheduler::new(graph, Duration::ZERO)?;
    let mut simulation = Simulation { position: 0 };
    for (target, expected_command) in [(5, 5), (8, 3)] {
        let receipt = scheduler
            .advance(
                &mut simulation,
                StepRequest {
                    dt: Duration::from_secs(1),
                    inputs: BTreeMap::from([(
                        input("controller", "target"),
                        StepValue::encode(&Scalar { value: target }),
                    )]),
                },
            )
            .await?;
        let command = receipt.outputs[&output("controller", "command")]
            .decode::<Scalar>()?
            .value;
        let state = receipt.outputs[&output("physics", "state")]
            .decode::<Scalar>()?
            .value;
        ensure!(
            command == expected_command && state == target,
            "unexpected simulation state"
        );
        println!(
            "epoch={} tick={} [{:?}, {:?}] command={command} state={state}",
            receipt.tick.id.epoch.get(),
            receipt.tick.id.index,
            receipt.tick.start,
            receipt.tick.end
        );
    }
    Ok(())
}
