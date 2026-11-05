//! One stateful factory and its explicit feedback graph.

use super::{
    BTreeMap, ComponentContext, ComponentFuture, Duration, FactoryRegistration,
    MODULE_BINDINGS_VERSION, MessageType, ModuleBindings, NodeError, Result, StepComponent,
    StepComponentFactory, StepFuture, StepGraph, StepInput, StepInputs, StepOutput, StepOutputs,
    StepRequest, StepSchema, StepSource, StepTick, StepType, StepValue, StopReason, WireSchema,
    WireStepType,
};

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Number {
    #[prost(sint64, tag = "1")]
    pub(super) value: i64,
}
impl MessageType for Number {
    const TYPE_NAME: &'static str = "process_steps.Number";
    const SCHEMA_FINGERPRINT: u64 = 234;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

pub(super) struct Factory;
struct Node {
    calls: i64,
    seed: i64,
}
impl StepComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> reiny::Result<Box<dyn StepComponent>> {
        Ok(Box::new(Node { calls: 0, seed: 17 }))
    }
}
impl StepComponent for Node {
    fn prepare<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
    fn prepared_outputs(&self) -> Result<StepOutputs, NodeError> {
        Ok(BTreeMap::from([("out".into(), value(self.seed))]))
    }
    fn prepared_contract(&self) -> Result<Vec<u8>, NodeError> {
        Ok(b"counter-v1:seed=17".to_vec())
    }
    fn step<'a>(
        &'a mut self,
        _: &'a ComponentContext,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            self.calls += 1;
            if std::env::args().any(|arg| arg == "--block") {
                println!("MUTATED");
                std::io::Write::flush(&mut std::io::stdout()).map_err(|error| NodeError {
                    message: error.to_string(),
                })?;
                // Deliberately unresponsive native callback, contained only by
                // terminating this owned fixture process.
                loop {
                    std::thread::park();
                }
            }
            let total = inputs.values().try_fold(0i64, |sum, v| {
                v.decode::<Number>()
                    .map(|n| sum + n.value)
                    .map_err(|e| NodeError {
                        message: e.to_string(),
                    })
            })?;
            // Independent instance state catches accidental factory-object sharing.
            let output = total
                + self.calls
                + i64::try_from(tick.id.index).map_err(|e| NodeError {
                    message: e.to_string(),
                })?;
            Ok(BTreeMap::from([("out".into(), value(output))]))
        })
    }
    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
pub(super) fn value(value: i64) -> StepValue {
    StepValue::encode(&Number { value })
}
pub(super) fn schema() -> StepSchema {
    StepSchema {
        inputs: BTreeMap::from([
            ("x".into(), StepType::of::<Number>()),
            ("feedback".into(), StepType::of::<Number>()),
        ]),
        outputs: BTreeMap::from([("out".into(), StepType::of::<Number>())]),
    }
}
pub(super) fn wire_schema() -> WireSchema {
    let ty = || WireStepType {
        type_name: Number::TYPE_NAME.into(),
        fingerprint_version: reiny::SCHEMA_FINGERPRINT_VERSION,
        fingerprint: Number::SCHEMA_FINGERPRINT,
        assertions: BTreeMap::new(),
    };
    WireSchema {
        inputs: BTreeMap::from([("x".into(), ty()), ("feedback".into(), ty())]),
        outputs: BTreeMap::from([("out".into(), ty())]),
    }
}
pub(super) fn registration() -> FactoryRegistration {
    FactoryRegistration {
        factory: "counter".into(),
        revision: "1".into(),
    }
}
pub(super) fn bindings(id: &str) -> ModuleBindings {
    ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: id.into(),
        endpoint_namespace: id.into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    }
}
pub(super) fn input(id: &str, port: &str) -> StepInput {
    StepInput {
        instance: id.into(),
        port: port.into(),
    }
}
pub(super) fn output(id: &str) -> StepOutput {
    StepOutput {
        instance: id.into(),
        port: "out".into(),
    }
}
pub(super) fn graph(seed: StepValue) -> StepGraph {
    StepGraph {
        nodes: ["a", "b", "c"]
            .into_iter()
            .map(|id| (id.into(), schema()))
            .collect(),
        inputs: BTreeMap::from([
            (input("a", "x"), StepSource::External),
            (
                input("a", "feedback"),
                StepSource::PreviousTick {
                    source: output("c"),
                    initial: seed,
                },
            ),
            (input("b", "x"), StepSource::SameTick(output("a"))),
            (input("b", "feedback"), StepSource::External),
            (input("c", "x"), StepSource::SameTick(output("b"))),
            (input("c", "feedback"), StepSource::External),
        ]),
    }
}
pub(super) fn request() -> StepRequest {
    StepRequest {
        dt: Duration::from_millis(25),
        inputs: BTreeMap::from([
            (input("a", "x"), value(2)),
            (input("b", "feedback"), value(0)),
            (input("c", "feedback"), value(0)),
        ]),
    }
}
