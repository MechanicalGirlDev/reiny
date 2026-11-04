use std::collections::{BTreeMap, BTreeSet};

use crate::bindings::{validate_namespace, validate_port_name};

use super::{StepGraph, StepInput, StepPlanError, StepSource};

pub(crate) struct Plan {
    pub(super) graph: StepGraph,
    pub(super) order: Vec<String>,
}

impl Plan {
    pub(super) fn compile(graph: StepGraph) -> Result<Self, StepPlanError> {
        if graph.nodes.is_empty() {
            return Err(StepPlanError::EmptyGraph);
        }
        let mut dependencies = BTreeMap::<String, BTreeSet<String>>::new();
        for (instance, schema) in &graph.nodes {
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
                if !graph.inputs.contains_key(&input) {
                    return Err(StepPlanError::MissingInput { input });
                }
            }
            dependencies.insert(instance.clone(), BTreeSet::new());
        }
        for (input, source) in &graph.inputs {
            let expected = graph
                .nodes
                .get(&input.instance)
                .and_then(|schema| schema.inputs.get(&input.port))
                .ok_or_else(|| StepPlanError::UnknownInput {
                    input: input.clone(),
                })?;
            let output = match source {
                StepSource::External => continue,
                StepSource::SameTick(output) => output,
                StepSource::PreviousTick { source, initial } => {
                    if initial.step_type() != expected {
                        return Err(StepPlanError::InitialTypeMismatch {
                            input: input.clone(),
                            expected: expected.clone(),
                            actual: initial.step_type().clone(),
                        });
                    }
                    source
                }
            };
            let actual = graph
                .nodes
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
            match source {
                StepSource::SameTick(_) => {
                    dependencies
                        .entry(input.instance.clone())
                        .or_default()
                        .insert(output.instance.clone());
                }
                StepSource::External | StepSource::PreviousTick { .. } => {}
            }
        }
        // Removing one ready node at a time preserves lexical tie-breaking even
        // when that removal makes an earlier lexical node ready.
        let mut order = Vec::with_capacity(graph.nodes.len());
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
        Ok(Self { graph, order })
    }
}
