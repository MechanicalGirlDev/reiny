//! Finalized graph conversion and initial-clock-bound identities.

use super::super::protocol::step::MAX_ENTRIES;
use super::{ProcessError, export_schema, export_values, import_schema, import_values, invalid};
use std::collections::BTreeMap;

pub(in crate::component::process) fn export_graph(
    graph: &crate::step::StepGraph,
) -> Result<super::super::protocol::step::WireGraph, ProcessError> {
    use super::super::protocol::step::{WireGraph, WireOutput, WireRoute, WireSource};
    let output = |p: &crate::step::StepOutput| WireOutput {
        instance: p.instance.clone(),
        port: p.port.clone(),
    };
    let inputs = graph
        .inputs
        .iter()
        .map(|(input, source)| {
            let source = match source {
                crate::step::StepSource::External => WireSource::External,
                crate::step::StepSource::SameTick(p) => WireSource::SameTick { source: output(p) },
                crate::step::StepSource::PreviousTick { source, initial } => {
                    let mut values =
                        export_values(BTreeMap::from([("initial".into(), initial.clone())]))?;
                    WireSource::PreviousTick {
                        source: output(source),
                        initial: values.remove("initial").ok_or_else(invalid)?,
                    }
                }
            };
            Ok(WireRoute {
                instance: input.instance.clone(),
                port: input.port.clone(),
                source,
            })
        })
        .collect::<Result<_, ProcessError>>()?;
    Ok(WireGraph {
        nodes: graph
            .nodes
            .iter()
            .map(|(id, s)| (id.clone(), export_schema(s)))
            .collect(),
        inputs,
    })
}

pub(in crate::component::process) fn import_graph(
    graph: super::super::protocol::step::WireGraph,
) -> Result<crate::step::StepGraph, ProcessError> {
    use super::super::protocol::step::{WireOutput, WireSource};
    use crate::step::{StepGraph, StepInput, StepOutput, StepSource};
    if graph.nodes.len() > MAX_ENTRIES || graph.inputs.len() > MAX_ENTRIES * MAX_ENTRIES {
        return Err(invalid());
    }
    let output = |p: WireOutput| StepOutput {
        instance: p.instance,
        port: p.port,
    };
    let nodes = graph
        .nodes
        .into_iter()
        .map(|(id, s)| Ok((id, import_schema(s)?)))
        .collect::<Result<_, ProcessError>>()?;
    let mut inputs = BTreeMap::new();
    for route in graph.inputs {
        let source = match route.source {
            WireSource::External => StepSource::External,
            WireSource::SameTick { source } => StepSource::SameTick(output(source)),
            WireSource::PreviousTick { source, initial } => StepSource::PreviousTick {
                source: output(source),
                initial: import_values(BTreeMap::from([("initial".into(), initial)]))?
                    .remove("initial")
                    .ok_or_else(invalid)?,
            },
        };
        if inputs
            .insert(
                StepInput {
                    instance: route.instance,
                    port: route.port,
                },
                source,
            )
            .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(StepGraph { nodes, inputs })
}

pub(in crate::component::process) fn plan_digest(
    graph: &super::super::protocol::step::WireGraph,
    initial_time: super::super::protocol::step::WireDuration,
) -> Result<super::super::protocol::PreparedDigest, ProcessError> {
    use sha2::{Digest, Sha256};
    let bytes = super::super::codec::encode(&(graph, initial_time))?;
    Ok(super::super::protocol::PreparedDigest(format!(
        "{:x}",
        Sha256::digest(bytes)
    )))
}
