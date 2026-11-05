//! Typed process-step value and failure conversion.

mod graph;
pub(super) use graph::{export_graph, import_graph, plan_digest};

use super::{
    ProcessError,
    protocol::{
        FailureCode,
        step::{
            MAX_ENTRIES, MAX_VALUE_BYTES, StepFailureCode, WireSchema, WireStepFailure,
            WireStepType, WireValue,
        },
    },
};
use crate::{
    encoded::EncodedType,
    step::{StepFailureKind, StepInputs, StepOutputs, StepSchema, StepType, StepValue},
};
use std::collections::BTreeMap;

pub(super) fn invalid() -> ProcessError {
    ProcessError::new(FailureCode::Assignment)
}

pub(super) fn export_type(ty: &StepType) -> WireStepType {
    WireStepType {
        type_name: ty.type_name().into(),
        fingerprint_version: ty.fingerprint_version(),
        fingerprint: ty.fingerprint(),
        assertions: ty.semantics().clone(),
    }
}

pub(super) fn import_type(ty: WireStepType) -> Result<StepType, ProcessError> {
    let encoded = EncodedType::new(ty.type_name, ty.fingerprint_version, ty.fingerprint)
        .map_err(|_| invalid())?;
    StepType::from_encoded_type(encoded)
        .map_err(|_| invalid())?
        .with_semantics(ty.assertions)
        .map_err(|_| invalid())
}

pub(super) fn export_schema(schema: &StepSchema) -> WireSchema {
    WireSchema {
        inputs: schema
            .inputs
            .iter()
            .map(|(k, v)| (k.clone(), export_type(v)))
            .collect(),
        outputs: schema
            .outputs
            .iter()
            .map(|(k, v)| (k.clone(), export_type(v)))
            .collect(),
    }
}

pub(super) fn import_schema(schema: WireSchema) -> Result<StepSchema, ProcessError> {
    fn ports(
        ports: BTreeMap<String, WireStepType>,
    ) -> Result<BTreeMap<String, StepType>, ProcessError> {
        if ports.len() > MAX_ENTRIES {
            return Err(invalid());
        }
        ports
            .into_iter()
            .map(|(name, ty)| {
                crate::bindings::validate_port_name(&name).map_err(|_| invalid())?;
                Ok((name, import_type(ty)?))
            })
            .collect()
    }
    Ok(StepSchema {
        inputs: ports(schema.inputs)?,
        outputs: ports(schema.outputs)?,
    })
}

pub(super) fn export_values(
    values: StepOutputs,
) -> Result<BTreeMap<String, WireValue>, ProcessError> {
    if values.len() > MAX_ENTRIES {
        return Err(invalid());
    }
    values
        .into_iter()
        .map(|(port, value)| {
            if value.as_encoded().len() > MAX_VALUE_BYTES {
                return Err(invalid());
            }
            Ok((
                port,
                WireValue {
                    ty: export_type(value.step_type()),
                    bytes: value.as_encoded().into(),
                },
            ))
        })
        .collect()
}

pub(super) fn import_values(
    values: BTreeMap<String, WireValue>,
) -> Result<StepInputs, ProcessError> {
    if values.len() > MAX_ENTRIES {
        return Err(invalid());
    }
    values
        .into_iter()
        .map(|(port, value)| {
            if value.bytes.len() > MAX_VALUE_BYTES {
                return Err(invalid());
            }
            Ok((
                port,
                StepValue::from_encoded(import_type(value.ty)?, value.bytes),
            ))
        })
        .collect()
}

pub(super) fn check_values(
    schema: &BTreeMap<String, StepType>,
    values: &StepInputs,
) -> Result<(), ProcessError> {
    if schema.len() != values.len()
        || schema
            .iter()
            .any(|(p, t)| values.get(p).is_none_or(|v| v.step_type() != t))
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn export_failure(kind: StepFailureKind, message: String) -> WireStepFailure {
    let code = match kind {
        StepFailureKind::NodeError => StepFailureCode::NodeError,
        StepFailureKind::NodePanicked => StepFailureCode::NodePanicked,
        StepFailureKind::InvalidOutputs => StepFailureCode::InvalidOutputs,
        StepFailureKind::Cancelled => StepFailureCode::Cancelled,
        StepFailureKind::TimedOut => StepFailureCode::TimedOut,
        StepFailureKind::HostStopped => StepFailureCode::HostStopped,
        StepFailureKind::WorkerLost | StepFailureKind::AdvanceDropped => {
            StepFailureCode::WorkerLost
        }
    };
    WireStepFailure { code, message }
}

pub(super) fn import_failure(failure: WireStepFailure) -> crate::component::step::DispatchFailure {
    let kind = match failure.code {
        StepFailureCode::NodeError => StepFailureKind::NodeError,
        StepFailureCode::NodePanicked => StepFailureKind::NodePanicked,
        StepFailureCode::InvalidOutputs => StepFailureKind::InvalidOutputs,
        StepFailureCode::Cancelled => StepFailureKind::Cancelled,
        StepFailureCode::TimedOut => StepFailureKind::TimedOut,
        StepFailureCode::HostStopped => StepFailureKind::HostStopped,
        StepFailureCode::WorkerLost => StepFailureKind::WorkerLost,
    };
    crate::component::step::DispatchFailure {
        kind,
        message: failure.message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finalized_digest_binds_initial_logical_time_and_protocol_negotiation_is_exact()
    -> crate::Result<()> {
        let graph = super::super::protocol::step::WireGraph {
            nodes: BTreeMap::new(),
            inputs: Vec::new(),
        };
        assert_ne!(
            plan_digest(&graph, std::time::Duration::ZERO.into())?,
            plan_digest(&graph, std::time::Duration::from_secs(1).into())?
        );
        assert!(
            super::super::codec::decode::<super::super::protocol::step::StepProtocol>(b"2")
                .is_err()
        );
        Ok(())
    }
    #[test]
    fn wire_semantics_are_owned_and_protobuf_decode_identity_is_separate() -> crate::Result<()> {
        let ty = StepType::from_encoded_type(EncodedType::new(
            "test.Value",
            crate::SCHEMA_FINGERPRINT_VERSION,
            1,
        )?)?
        .with_semantics(BTreeMap::from([("units".into(), "rad".into())]))?;
        let imported = import_type(export_type(&ty))?;
        assert_eq!(imported, ty);
        let mut erased = export_type(&ty);
        erased.assertions.clear();
        assert_ne!(import_type(erased)?, ty);
        let mut wrong = export_type(&ty);
        wrong.fingerprint_version += 1;
        assert!(import_type(wrong).is_err());
        Ok(())
    }
    #[test]
    fn wire_bounds_and_duplicate_routes_are_rejected() {
        let ty = WireStepType {
            type_name: "test.Value".into(),
            fingerprint_version: crate::SCHEMA_FINGERPRINT_VERSION,
            fingerprint: 1,
            assertions: BTreeMap::new(),
        };
        assert!(
            import_values(BTreeMap::from([(
                "x".into(),
                WireValue {
                    ty,
                    bytes: vec![0; MAX_VALUE_BYTES + 1]
                }
            )]))
            .is_err()
        );
        let route = super::super::protocol::step::WireRoute {
            instance: "node".into(),
            port: "x".into(),
            source: super::super::protocol::step::WireSource::External,
        };
        let graph = super::super::protocol::step::WireGraph {
            nodes: BTreeMap::new(),
            inputs: vec![route.clone(), route],
        };
        assert!(import_graph(graph).is_err());
        assert!(super::super::codec::decode::<WireStepType>(br#"{"type_name":"x","fingerprint_version":2,"fingerprint":1,"assertions":{"a":"b","a":"c"}}"#).is_err());
    }
}
