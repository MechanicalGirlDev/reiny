use super::*;
use crate::step::{StepPlanError, StepScheduler};

#[test]
fn graph_compilation_rejects_missing_unknown_and_mistyped_routes() {
    let base = graph();
    let mut missing = base.clone();
    missing.inputs.remove(&input("controller", "state"));
    let mut unknown_input = base.clone();
    unknown_input
        .inputs
        .insert(input("controller", "extra"), StepSource::External);
    let mut unknown_node = base.clone();
    unknown_node
        .inputs
        .insert(input("absent", "extra"), StepSource::External);
    let mut unknown_source = base.clone();
    unknown_source.inputs.insert(
        input("physics", "command"),
        StepSource::SameTick(output("absent")),
    );
    let mut unknown_port = base.clone();
    let absent_output = StepOutput {
        instance: "controller".into(),
        port: "absent".into(),
    };
    unknown_port.inputs.insert(
        input("physics", "command"),
        StepSource::SameTick(absent_output.clone()),
    );
    let mut mismatch = base.clone();
    mismatch.nodes.insert(
        "controller".into(),
        StepSchema {
            outputs: BTreeMap::from([("out".into(), StepType::of::<OtherNumber>())]),
            ..schema(&["action", "state"])
        },
    );
    let mut initial = base;
    initial.inputs.insert(
        input("controller", "state"),
        StepSource::PreviousTick {
            source: output("physics"),
            initial: StepValue::encode(&ChangedNumber { value: 10 }),
        },
    );

    for (graph, expected) in [
        (
            missing,
            StepPlanError::MissingInput {
                input: input("controller", "state"),
            },
        ),
        (
            unknown_input,
            StepPlanError::UnknownInput {
                input: input("controller", "extra"),
            },
        ),
        (
            unknown_node,
            StepPlanError::UnknownInput {
                input: input("absent", "extra"),
            },
        ),
        (
            unknown_source,
            StepPlanError::UnknownSource {
                output: output("absent"),
            },
        ),
        (
            unknown_port,
            StepPlanError::UnknownSource {
                output: absent_output,
            },
        ),
        (
            mismatch,
            StepPlanError::TypeMismatch {
                input: input("physics", "command"),
                expected: StepType::of::<Number>(),
                actual: StepType::of::<OtherNumber>(),
            },
        ),
        (
            initial,
            StepPlanError::InitialTypeMismatch {
                input: input("controller", "state"),
                expected: StepType::of::<Number>(),
                actual: StepType::of::<ChangedNumber>(),
            },
        ),
    ] {
        assert_eq!(
            StepScheduler::new(graph, Duration::ZERO).err(),
            Some(expected)
        );
    }
}

#[test]
fn same_tick_self_and_multinode_cycles_are_rejected() {
    let mut multi = graph();
    multi.inputs.insert(
        input("controller", "state"),
        StepSource::SameTick(output("physics")),
    );
    let single = StepGraph {
        nodes: BTreeMap::from([("self".into(), schema(&["in"]))]),
        inputs: BTreeMap::from([(input("self", "in"), StepSource::SameTick(output("self")))]),
    };
    for (graph, instances) in [
        (multi, vec!["controller".into(), "physics".into()]),
        (single, vec!["self".into()]),
    ] {
        assert_eq!(
            StepScheduler::new(graph, Duration::ZERO).err(),
            Some(StepPlanError::SameTickCycle { instances })
        );
    }
}

#[test]
fn graph_names_follow_reiny_namespace_and_port_contracts() {
    assert_eq!(
        StepScheduler::new(
            StepGraph {
                nodes: BTreeMap::new(),
                inputs: BTreeMap::new()
            },
            Duration::ZERO
        )
        .err(),
        Some(StepPlanError::EmptyGraph),
    );
    for name in ["", "a//b", "../node", "wild*"] {
        let graph = StepGraph {
            nodes: BTreeMap::from([(name.into(), schema(&[]))]),
            inputs: BTreeMap::new(),
        };
        assert_eq!(
            StepScheduler::new(graph, Duration::ZERO).err(),
            Some(StepPlanError::InvalidInstance {
                instance: name.into()
            })
        );
    }
    for name in ["", "a/b", "white space", "@reserved"] {
        let graph = StepGraph {
            nodes: BTreeMap::from([(
                "a/b".into(),
                StepSchema {
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::from([(name.into(), StepType::of::<Number>())]),
                },
            )]),
            inputs: BTreeMap::new(),
        };
        assert_eq!(
            StepScheduler::new(graph, Duration::ZERO).err(),
            Some(StepPlanError::InvalidPort {
                instance: "a/b".into(),
                port: name.into()
            })
        );
    }
}
