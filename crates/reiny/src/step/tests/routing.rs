use super::*;
use crate::step::{ClockPhase, StepScheduler, StepValueError};

#[tokio::test]
async fn feedback_uses_initial_then_previous_successful_tick() -> anyhow::Result<()> {
    let start = Duration::from_secs(20);
    let mut scheduler = StepScheduler::new(graph(), start)?;
    let mut executor = Arithmetic::default();

    let first = scheduler.advance(&mut executor, request(2)).await?;
    let second = scheduler.advance(&mut executor, request(3)).await?;

    assert_eq!(
        first.outputs[&output("controller")]
            .decode::<Number>()?
            .value,
        13
    );
    assert_eq!(
        first.outputs[&output("physics")].decode::<Number>()?.value,
        14
    );
    assert_eq!(
        second.outputs[&output("controller")]
            .decode::<Number>()?
            .value,
        18
    );
    assert_eq!(
        second.outputs[&output("physics")].decode::<Number>()?.value,
        19
    );
    assert_eq!(first.tick.id.index, 0);
    assert_eq!(second.tick.id.index, 1);
    assert_eq!(first.tick.start, start);
    assert_eq!(first.tick.end, second.tick.start);
    assert_eq!(second.tick.end, start + Duration::from_secs(2));
    assert_eq!(scheduler.clock().time, second.tick.end);
    assert_eq!(scheduler.clock().next_index, 2);
    assert_eq!(scheduler.clock().phase, ClockPhase::Ready);
    assert_eq!(first.tick.id.epoch, second.tick.id.epoch);
    assert_eq!(
        executor.visits,
        vec![
            ("controller".into(), first.tick),
            ("physics".into(), first.tick),
            ("controller".into(), second.tick),
            ("physics".into(), second.tick),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn lexical_topology_handles_fanout_and_multiple_edges_between_nodes() -> anyhow::Result<()> {
    let graph = StepGraph {
        nodes: BTreeMap::from([
            ("a_left".into(), schema(&["left", "right"])),
            ("b_source".into(), schema(&["in"])),
            ("c_right".into(), schema(&["in"])),
            ("d_free".into(), schema(&[])),
        ]),
        inputs: BTreeMap::from([
            (
                input("a_left", "left"),
                StepSource::SameTick(output("b_source")),
            ),
            (
                input("a_left", "right"),
                StepSource::SameTick(output("b_source")),
            ),
            (input("b_source", "in"), StepSource::External),
            (
                input("c_right", "in"),
                StepSource::SameTick(output("b_source")),
            ),
        ]),
    };
    let mut scheduler = StepScheduler::new(graph, Duration::ZERO)?;
    let mut executor = Arithmetic::default();

    let receipt = scheduler
        .advance(
            &mut executor,
            StepRequest {
                dt: Duration::from_nanos(1),
                inputs: BTreeMap::from([(input("b_source", "in"), value(4))]),
            },
        )
        .await?;

    assert_eq!(
        executor
            .visits
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["b_source", "a_left", "c_right", "d_free"]
    );
    let numbers: BTreeMap<_, _> = receipt
        .outputs
        .iter()
        .map(|(port, value)| {
            Ok::<_, StepValueError>((port.instance.as_str(), value.decode::<Number>()?.value))
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(
        numbers,
        BTreeMap::from([
            ("a_left", 11),
            ("b_source", 5),
            ("c_right", 6),
            ("d_free", 1),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn delayed_self_edge_is_valid_and_advances_from_initial() -> anyhow::Result<()> {
    let graph = StepGraph {
        nodes: BTreeMap::from([("counter".into(), schema(&["prior"]))]),
        inputs: BTreeMap::from([(
            input("counter", "prior"),
            StepSource::PreviousTick {
                source: output("counter"),
                initial: value(7),
            },
        )]),
    };
    let mut scheduler = StepScheduler::new(graph, Duration::ZERO)?;
    let mut executor = Arithmetic::default();
    let request = StepRequest {
        dt: Duration::from_secs(1),
        inputs: BTreeMap::new(),
    };

    let first = scheduler.advance(&mut executor, request.clone()).await?;
    let second = scheduler.advance(&mut executor, request).await?;

    assert_eq!(
        first.outputs[&output("counter")].decode::<Number>()?.value,
        8
    );
    assert_eq!(
        second.outputs[&output("counter")].decode::<Number>()?.value,
        9
    );
    Ok(())
}

#[test]
fn snapshots_do_not_alias_the_message_and_decode_checks_full_identity() -> Result<(), StepValueError>
{
    let mut number = Number { value: 41 };
    let snapshot = StepValue::encode(&number);
    number.value = 99;
    assert_ne!(snapshot, StepValue::encode(&number));

    assert_eq!(snapshot.decode::<Number>()?.value, 41);
    assert!(matches!(
        snapshot.decode::<OtherNumber>(),
        Err(StepValueError::TypeMismatch { .. })
    ));
    assert!(matches!(
        snapshot.decode::<ChangedNumber>(),
        Err(StepValueError::TypeMismatch { .. })
    ));
    assert_eq!(snapshot.step_type().type_name(), Number::TYPE_NAME);
    assert_eq!(
        snapshot.step_type().fingerprint(),
        Number::SCHEMA_FINGERPRINT
    );
    assert_eq!(
        snapshot.step_type().fingerprint_version(),
        crate::SCHEMA_FINGERPRINT_VERSION
    );
    Ok(())
}

#[test]
fn scheduler_epochs_are_monotonically_unique() -> anyhow::Result<()> {
    let first = StepScheduler::new(graph(), Duration::ZERO)?.clock().epoch;
    let second = StepScheduler::new(graph(), Duration::ZERO)?.clock().epoch;
    assert!(second.get() > first.get());
    Ok(())
}
