//! Placement is routing data, never a replacement for endpoint bindings.

use std::collections::BTreeSet;

use super::ProcessError;
use super::protocol::{FailureCode, Prepare, TransportSettings};

pub(super) fn validate(request: &Prepare, limit: usize, host: &str) -> Result<(), ProcessError> {
    let mut namespaces = BTreeSet::new();
    for assignment in &request.assignments {
        if !namespaces.insert(&assignment.bindings.namespace) {
            return Err(ProcessError::new(FailureCode::Assignment));
        }
    }
    let (placement, local) = match &request.transport {
        TransportSettings::Zenoh { .. } => return Ok(()),
        TransportSettings::RoutedZenoh { placement, .. } => (placement, false),
        TransportSettings::Local { placement, .. } => (placement, true),
    };
    if placement.len() > limit {
        return Err(ProcessError::new(FailureCode::Assignment));
    }
    for (namespace, owner) in placement {
        crate::bindings::validate_namespace(namespace)
            .map_err(|_| ProcessError::new(FailureCode::Assignment))?;
        if owner.is_empty()
            || owner == "in"
            || !owner
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        {
            return Err(ProcessError::new(FailureCode::Assignment));
        }
    }
    let assigned: BTreeSet<_> = placement
        .iter()
        .filter(|(_, owner)| owner.as_str() == host)
        .map(|(namespace, _)| namespace)
        .collect();
    if assigned != namespaces {
        return Err(ProcessError::new(FailureCode::Assignment));
    }
    for assignment in &request.assignments {
        for source in assignment
            .bindings
            .inputs
            .values()
            .flat_map(|input| &input.sources)
        {
            match placement.get(&source.namespace) {
                Some(owner) if !local || owner == host => {}
                Some(_) | None => return Err(ProcessError::new(FailureCode::Assignment)),
            }
        }
    }
    Ok(())
}
