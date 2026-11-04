use std::{collections::BTreeMap, time::Duration};

use crate::{Descriptor, MessageType};

use super::{
    NodeError, StepExecutor, StepFuture, StepGraph, StepInput, StepInputs, StepOutput, StepRequest,
    StepSchema, StepSource, StepTick, StepType, StepValue,
};

mod failures;
mod fault_executor;
mod planning;
mod requests;
mod routing;

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Number {
    #[prost(sint64, tag = "1")]
    pub(super) value: i64,
}

impl MessageType for Number {
    const TYPE_NAME: &'static str = "step.Number";
    const SCHEMA_FINGERPRINT: u64 = 1;
    const DESCRIPTOR: Option<Descriptor> = None;
}

#[derive(Clone, PartialEq, prost::Message)]
struct OtherNumber {
    #[prost(sint64, tag = "1")]
    value: i64,
}

impl MessageType for OtherNumber {
    const TYPE_NAME: &'static str = "step.OtherNumber";
    const SCHEMA_FINGERPRINT: u64 = Number::SCHEMA_FINGERPRINT;
    const DESCRIPTOR: Option<Descriptor> = None;
}

#[derive(Clone, PartialEq, prost::Message)]
struct ChangedNumber {
    #[prost(sint64, tag = "1")]
    value: i64,
}

impl MessageType for ChangedNumber {
    const TYPE_NAME: &'static str = Number::TYPE_NAME;
    const SCHEMA_FINGERPRINT: u64 = 2;
    const DESCRIPTOR: Option<Descriptor> = None;
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

fn value(value: i64) -> StepValue {
    StepValue::encode(&Number { value })
}

fn schema(inputs: &[&str]) -> StepSchema {
    StepSchema {
        inputs: inputs
            .iter()
            .map(|name| ((*name).into(), StepType::of::<Number>()))
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

pub(super) fn request(action: i64) -> StepRequest {
    StepRequest {
        dt: Duration::from_secs(1),
        inputs: BTreeMap::from([(input("controller", "action"), value(action))]),
    }
}

/// A real integer dataflow executor: each node sums its inputs and adds one.
#[derive(Default)]
pub(super) struct Arithmetic {
    pub(super) visits: Vec<(String, StepTick)>,
}

impl StepExecutor for Arithmetic {
    fn invoke<'a>(
        &'a mut self,
        instance: &'a str,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        self.visits.push((instance.into(), tick));
        Box::pin(async move {
            let sum = inputs.values().try_fold(1_i64, |sum, value| {
                let number = value.decode::<Number>().map_err(|error| NodeError {
                    message: error.to_string(),
                })?;
                sum.checked_add(number.value).ok_or_else(|| NodeError {
                    message: "integer addition overflow".into(),
                })
            })?;
            Ok(BTreeMap::from([("out".into(), value(sum))]))
        })
    }
}
