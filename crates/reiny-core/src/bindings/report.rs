//! Compiled report admission and closed, exact-address graph validation.

use super::{
    MODULE_REPORT_VERSION, ModuleBindings, ModuleReport, PortEndpoint, PortKind, Replay,
    ReportEntry, ReportError, ReportErrorCode as Code, ReportPortSide, Retention,
};
use alloc::{boxed::Box, collections::BTreeMap, format};

/// Validate a compiled report against its resolved bindings, including owned children.
///
/// Bindings and identities are checked first, then inputs followed by outputs.
/// Each direction checks its exact port set before contents in lexical name order.
/// Sources must retain their declared order. Fingerprints are compared at graph
/// edges, since bindings do not contain compiled schemas.
///
/// # Errors
/// Returns the first malformed binding, identity, port, or compiled type.
pub fn validate_report(
    bindings: &ModuleBindings,
    report: &ModuleReport,
) -> Result<(), ReportError> {
    let error = |code, detail| ReportError::new(code, Some(&bindings.namespace), detail);
    bindings
        .validate()
        .map_err(|failure| error(Code::InvalidBindings, format!("{failure}")))?;
    for (valid, code, detail) in [
        (
            report.version == MODULE_REPORT_VERSION,
            Code::ReportVersion,
            "unsupported report version",
        ),
        (
            report.fingerprint_version == crate::SCHEMA_FINGERPRINT_VERSION,
            Code::FingerprintVersion,
            "unsupported fingerprint version",
        ),
        (
            report.namespace == bindings.namespace,
            Code::Namespace,
            "report namespace differs from bindings",
        ),
        (
            report.endpoint_namespace == bindings.endpoint_namespace,
            Code::EndpointNamespace,
            "report endpoint namespace differs from bindings",
        ),
    ] {
        if !valid {
            return Err(error(code, detail.into()));
        }
    }
    let entry = ReportEntry { bindings, report };
    validate_ports(&entry, ReportPortSide::Input)?;
    validate_ports(&entry, ReportPortSide::Output)
}

fn validate_ports(entry: &ReportEntry<'_>, side: ReportPortSide) -> Result<(), ReportError> {
    let bindings = entry.bindings;
    let (input, actual) = match side {
        ReportPortSide::Input => (true, &entry.report.inputs),
        ReportPortSide::Output => (false, &entry.report.outputs),
    };
    let error = |code, name: &str, detail: &str| {
        ReportError::new(code, Some(&bindings.namespace), detail.into()).at(side, name)
    };
    let inputs = bindings
        .inputs
        .iter()
        .filter(|_| input)
        .map(|(name, binding)| (name, &binding.contract, Some(binding.sources.as_slice())));
    let outputs = bindings
        .outputs
        .iter()
        .filter(|_| !input)
        .map(|(name, binding)| (name, &binding.contract, None));
    let declared = inputs.chain(outputs);
    let missing = declared
        .clone()
        .find(|(name, _, _)| !actual.contains_key(*name))
        .map(|(name, _, _)| name);
    let extra = actual.keys().find(|name| match side {
        ReportPortSide::Input => !bindings.inputs.contains_key(*name),
        ReportPortSide::Output => !bindings.outputs.contains_key(*name),
    });
    if let Some(name) = missing.into_iter().chain(extra).min() {
        return Err(error(
            Code::PortSet,
            name,
            "declared and compiled port sets differ",
        ));
    }
    for (name, contract, sources) in declared {
        let port = &actual[name];
        let endpoint_matches = match (&port.endpoint, sources) {
            (PortEndpoint::Input { sources: actual }, Some(expected)) => actual == expected,
            (PortEndpoint::Output { address }, None) => {
                address.namespace == bindings.endpoint_namespace && address.port == *name
            }
            (PortEndpoint::Input { .. }, None) | (PortEndpoint::Output { .. }, Some(_)) => false,
        };
        if !endpoint_matches {
            return Err(error(
                Code::Endpoint,
                name,
                "compiled endpoint differs from its binding",
            ));
        }
        if port.contract != *contract {
            return Err(error(
                Code::Contract,
                name,
                "effective contract differs from its declaration",
            ));
        }
        if port.type_name != contract.type_name {
            return Err(error(
                Code::Type,
                name,
                "compiled message or request name differs",
            ));
        }
        match (
            contract.kind,
            contract.response.as_ref(),
            port.response.as_ref(),
        ) {
            (PortKind::Stream, None, None) => {}
            (PortKind::Rpc, Some(expected), Some(response)) if *expected == response.type_name => {}
            (PortKind::Stream | PortKind::Rpc, _, _) => {
                return Err(error(
                    Code::Response,
                    name,
                    "compiled response shape or name differs",
                ));
            }
        }
    }
    Ok(())
}

/// Validate a closed graph of top-level, aggregated compiled module reports.
///
/// Each entry is validated in slice order before duplicate namespaces and graph
/// membership are checked. Edges follow entry order, lexical input order, then
/// declared source order. Owned children must be validated individually and
/// aggregated into the parent report; external sources are never inferred by type.
///
/// # Errors
/// Returns the first invalid report, unsupported member, duplicate namespace, or
/// missing or incompatible exact source output.
pub fn validate_graph(entries: &[ReportEntry<'_>]) -> Result<(), ReportError> {
    if entries.is_empty() {
        return Err(ReportError::new(
            Code::EmptyGraph,
            None,
            "a closed graph requires a module".into(),
        ));
    }
    for entry in entries {
        validate_report(entry.bindings, entry.report)?;
    }
    let mut modules = BTreeMap::new();
    for entry in entries {
        let bindings = entry.bindings;
        let error = |code, detail| ReportError::new(code, Some(&bindings.namespace), detail);
        if bindings.namespace != bindings.endpoint_namespace {
            return Err(error(
                Code::UnsupportedGraphMember,
                "aggregate an owned child's report into its parent".into(),
            ));
        }
        if modules.insert(&bindings.namespace, entry.report).is_some() {
            return Err(error(
                Code::DuplicateNamespace,
                "module namespace is repeated".into(),
            ));
        }
    }
    for entry in entries {
        for (name, binding) in &entry.bindings.inputs {
            let input = &entry.report.inputs[name];
            for address in &binding.sources {
                let error = |code, detail: &str| {
                    let mut error =
                        ReportError::new(code, Some(&entry.bindings.namespace), detail.into())
                            .at(ReportPortSide::Input, name);
                    error.source = Some(Box::new(address.clone()));
                    error
                };
                let source = modules
                    .get(&address.namespace)
                    .ok_or_else(|| error(Code::MissingSourceModule, "source module is absent"))?;
                let output = source
                    .outputs
                    .get(&address.port)
                    .ok_or_else(|| error(Code::MissingSourcePort, "source output is absent"))?;
                if input.type_name != output.type_name {
                    return Err(error(Code::Type, "source message or request name differs"));
                }
                if input.schema != output.schema {
                    return Err(error(
                        Code::Schema,
                        "source message or request fingerprint differs",
                    ));
                }
                if input.contract.kind != output.contract.kind {
                    return Err(error(Code::Policy, "source endpoint kind differs"));
                }
                if input.contract.semantics != output.contract.semantics {
                    return Err(error(Code::Semantics, "source semantic assertions differ"));
                }
                match (&input.response, &output.response) {
                    (None, None) => {}
                    (Some(expected), Some(actual)) => {
                        if expected.type_name != actual.type_name {
                            return Err(error(Code::Response, "source response name differs"));
                        }
                        if expected.schema != actual.schema {
                            return Err(error(Code::Schema, "source response fingerprint differs"));
                        }
                    }
                    (None, Some(_)) | (Some(_), None) => {
                        return Err(error(Code::Response, "source response shape differs"));
                    }
                }
                match (input.contract.replay, output.contract.retention) {
                    (Replay::Last, Retention::Volatile) => {
                        return Err(error(
                            Code::Policy,
                            "source does not retain replayable samples",
                        ));
                    }
                    (Replay::Live, Retention::Volatile | Retention::Last)
                    | (Replay::Last, Retention::Last) => {}
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "test fixtures and assertions fail by panicking"
)]
mod tests;
