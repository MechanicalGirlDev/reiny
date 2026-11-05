use std::collections::{BTreeMap, BTreeSet};

use crate::bindings::{validate_namespace, validate_port_name};

use super::{StepGraph, StepInput, StepOutput, StepPlanError, StepSchema, StepSource, StepType};

pub(crate) struct PlanRoute {
    pub output: Option<StepOutput>,
    pub initial: Option<StepType>,
    pub same_tick: bool,
}

pub(crate) struct Plan {
    pub(super) graph: StepGraph,
    pub(crate) order: Vec<String>,
}

impl Plan {
    pub(crate) fn compile(graph: StepGraph) -> Result<Self, StepPlanError> {
        let inputs = graph
            .inputs
            .iter()
            .map(|(input, source)| {
                let route = match source {
                    StepSource::External => PlanRoute {
                        output: None,
                        initial: None,
                        same_tick: false,
                    },
                    StepSource::SameTick(output) => PlanRoute {
                        output: Some(output.clone()),
                        initial: None,
                        same_tick: true,
                    },
                    StepSource::PreviousTick { source, initial } => PlanRoute {
                        output: Some(source.clone()),
                        initial: Some(initial.step_type().clone()),
                        same_tick: false,
                    },
                };
                (input.clone(), route)
            })
            .collect();
        let order = Self::validate_routes(&graph.nodes, &inputs)?;
        Ok(Self { graph, order })
    }

    pub(crate) fn validate_routes(
        nodes: &BTreeMap<String, StepSchema>,
        inputs: &BTreeMap<StepInput, PlanRoute>,
    ) -> Result<Vec<String>, StepPlanError> {
        if nodes.is_empty() {
            return Err(StepPlanError::EmptyGraph);
        }
        let mut dependencies = BTreeMap::<String, BTreeSet<String>>::new();
        for (instance, schema) in nodes {
            validate_namespace(instance).map_err(|_| StepPlanError::InvalidInstance {
                instance: instance.clone(),
            })?;
            for port in schema.inputs.keys().chain(schema.outputs.keys()) {
                validate_port_name(port).map_err(|_| StepPlanError::InvalidPort {
                    instance: instance.clone(),
                    port: port.clone(),
                })?;
            }
            for port in schema.inputs.keys() {
                let input = StepInput {
                    instance: instance.clone(),
                    port: port.clone(),
                };
                if !inputs.contains_key(&input) {
                    return Err(StepPlanError::MissingInput { input });
                }
            }
            dependencies.insert(instance.clone(), BTreeSet::new());
        }
        for (input, route) in inputs {
            let expected = nodes
                .get(&input.instance)
                .and_then(|schema| schema.inputs.get(&input.port))
                .ok_or_else(|| StepPlanError::UnknownInput {
                    input: input.clone(),
                })?;
            if let Some(initial) = &route.initial
                && initial != expected
            {
                return Err(StepPlanError::InitialTypeMismatch {
                    input: input.clone(),
                    expected: expected.clone(),
                    actual: initial.clone(),
                });
            }
            let Some(output) = &route.output else {
                continue;
            };
            let actual = nodes
                .get(&output.instance)
                .and_then(|schema| schema.outputs.get(&output.port))
                .ok_or_else(|| StepPlanError::UnknownSource {
                    output: output.clone(),
                })?;
            if actual != expected {
                return Err(StepPlanError::TypeMismatch {
                    input: input.clone(),
                    expected: expected.clone(),
                    actual: actual.clone(),
                });
            }
            if route.same_tick {
                dependencies
                    .entry(input.instance.clone())
                    .or_default()
                    .insert(output.instance.clone());
            }
        }
        // Removing one ready node at a time preserves lexical tie-breaking even
        // when that removal makes an earlier lexical node ready.
        let mut order = Vec::with_capacity(nodes.len());
        while !dependencies.is_empty() {
            let next = dependencies
                .iter()
                .find(|(_, sources)| sources.is_empty())
                .map(|(instance, _)| instance.clone());
            let Some(next) = next else {
                return Err(StepPlanError::SameTickCycle {
                    instances: dependencies.into_keys().collect(),
                });
            };
            dependencies.remove(&next);
            for sources in dependencies.values_mut() {
                sources.remove(&next);
            }
            order.push(next);
        }
        Ok(order)
    }
}
