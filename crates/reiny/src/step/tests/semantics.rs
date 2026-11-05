use super::*;
use crate::step::{
    SchedulerError, StepFailureKind, StepPlanError, StepRequestError, StepScheduler,
};

fn assertions(value: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("units".into(), value.into())])
}

fn asserted_graph() -> crate::Result<StepGraph> {
    let mut graph = graph();
    for schema in graph.nodes.values_mut() {
        for ty in schema
            .inputs
            .values_mut()
            .chain(schema.outputs.values_mut())
        {
            *ty = ty.clone().with_semantics(assertions("rad"))?;
        }
    }
    for source in graph.inputs.values_mut() {
        if let StepSource::PreviousTick { initial, .. } = source {
            *initial = initial.clone().with_semantics(assertions("rad"))?;
        }
    }
    Ok(graph)
}

struct AssertedArithmetic {
    inner: Arithmetic,
    output_semantics: BTreeMap<String, String>,
}

impl StepExecutor for AssertedArithmetic {
    fn invoke<'a>(
        &'a mut self,
        instance: &'a str,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            self.inner
                .invoke(instance, tick, inputs)
                .await?
                .into_iter()
                .map(|(port, value)| {
                    value
                        .with_semantics(self.output_semantics.clone())
                        .map(|value| (port, value))
                        .map_err(|error| NodeError {
                            message: error.to_string(),
                        })
                })
                .collect()
        })
    }
}

#[test]
fn graph_assertions_reject_differing_and_one_sided_edges_and_initial_values() -> crate::Result<()> {
    for map in [
        BTreeMap::new(),
        assertions("deg"),
        BTreeMap::from([("order".into(), "rad".into())]),
    ] {
        for instance in ["controller", "physics"] {
            let mut graph = asserted_graph()?;
            graph
                .nodes
                .get_mut(instance)
                .ok_or_else(|| anyhow::anyhow!("missing node"))?
                .outputs
                .insert(
                    "out".into(),
                    StepType::of::<Number>().with_semantics(map.clone())?,
                );
            assert!(matches!(
                StepScheduler::new(graph, Duration::ZERO),
                Err(StepPlanError::TypeMismatch { .. })
            ));
        }
        let mut graph = asserted_graph()?;
        let StepSource::PreviousTick { initial, .. } = graph
            .inputs
            .get_mut(&input("controller", "state"))
            .ok_or_else(|| anyhow::anyhow!("missing input"))?
        else {
            anyhow::bail!("expected delayed edge");
        };
        *initial = value(10).with_semantics(map)?;
        assert!(matches!(
            StepScheduler::new(graph, Duration::ZERO),
            Err(StepPlanError::InitialTypeMismatch { .. })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn assertions_survive_two_ticks_and_external_mismatch_has_no_effects() -> crate::Result<()> {
    let mut scheduler = StepScheduler::new(asserted_graph()?, Duration::ZERO)?;
    let mut executor = AssertedArithmetic {
        inner: Arithmetic::default(),
        output_semantics: assertions("rad"),
    };
    for map in [BTreeMap::new(), assertions("deg")] {
        let mut request = request(2);
        request
            .inputs
            .insert(input("controller", "action"), value(2).with_semantics(map)?);
        assert!(matches!(
            scheduler.advance(&mut executor, request).await,
            Err(SchedulerError::Request(
                StepRequestError::TypeMismatch { .. }
            ))
        ));
        assert_eq!(executor.inner.visits.len(), 0);
        assert_eq!(scheduler.clock().next_index, 0);
    }
    for expected in [14, 18] {
        let mut request = request(2);
        request.inputs.insert(
            input("controller", "action"),
            value(2).with_semantics(assertions("rad"))?,
        );
        let receipt = scheduler.advance(&mut executor, request).await?;
        let result = &receipt.outputs[&output("physics")];
        assert_eq!(result.decode::<Number>()?.value, expected);
        assert_eq!(result.step_type().semantics(), &assertions("rad"));
    }
    assert_eq!(executor.inner.visits.len(), 4);
    Ok(())
}

#[tokio::test]
async fn output_assertion_mismatch_prevents_downstream_dispatch_and_commit() -> crate::Result<()> {
    for map in [BTreeMap::new(), assertions("deg")] {
        let mut scheduler = StepScheduler::new(asserted_graph()?, Duration::ZERO)?;
        let mut executor = AssertedArithmetic {
            inner: Arithmetic::default(),
            output_semantics: map,
        };
        let mut request = request(2);
        request.inputs.insert(
            input("controller", "action"),
            value(2).with_semantics(assertions("rad"))?,
        );
        let Err(SchedulerError::Failed(failure)) = scheduler.advance(&mut executor, request).await
        else {
            anyhow::bail!("expected rejected output");
        };
        assert_eq!(failure.kind, StepFailureKind::InvalidOutputs);
        assert_eq!(failure.instance.as_deref(), Some("controller"));
        assert_eq!(executor.inner.visits.len(), 1);
        assert_eq!(scheduler.clock().next_index, 0);
    }
    Ok(())
}

#[test]
fn assertions_are_owned_validated_and_do_not_replace_decode_identity() -> crate::Result<()> {
    let mut map = assertions("rad");
    let value = value(42).with_semantics(map.clone())?;
    map.clear();
    let copied = value.clone();
    drop(value);
    assert_eq!(copied.step_type().semantics(), &assertions("rad"));
    assert_eq!(copied.decode::<Number>()?.value, 42);
    assert!(copied.decode::<OtherNumber>().is_err());
    assert!(copied.decode::<ChangedNumber>().is_err());
    for (key, value) in [("", "rad"), ("units", " \t"), (" \n", "rad")] {
        assert!(
            StepType::of::<Number>()
                .with_semantics(BTreeMap::from([(key.into(), value.into())]))
                .is_err()
        );
    }
    Ok(())
}
