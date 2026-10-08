//! Parsing the managed startup boundary and publishing its atomic runtime report.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;

use anyhow::Context;
use reiny_core::bindings::{
    CONTRACT_VERSION, EndpointContract, ModuleBindings, ModuleReport, PortKind, QosProfile,
    ReceiveBuffer, Replay, Retention,
};

use crate::{Result, validate_segment};

/// Validate a canonical path without flattening its module segments.
pub(crate) fn validate_namespace(value: &str) -> Result<()> {
    for segment in value.split('/') {
        validate_segment("module namespace segment", segment)?;
        anyhow::ensure!(
            segment != "." && segment != "..",
            "module namespace cannot contain '.' or '..'"
        );
    }
    Ok(())
}

pub(crate) fn validate_bindings(bindings: &ModuleBindings, id: &str) -> Result<()> {
    anyhow::ensure!(
        bindings.version == CONTRACT_VERSION,
        "unsupported module bindings version"
    );
    validate_namespace(&bindings.namespace)?;
    validate_namespace(&bindings.endpoint_namespace)?;
    let mut executables = BTreeSet::new();
    for executable in &bindings.executables {
        validate_segment("executable", executable)?;
        anyhow::ensure!(
            !matches!(executable.as_str(), "." | "..") && executables.insert(executable),
            "invalid or duplicate executable"
        );
    }
    anyhow::ensure!(
        bindings.namespace == bindings.endpoint_namespace
            || bindings
                .namespace
                .strip_prefix(&format!("{}/owned/", bindings.endpoint_namespace))
                .is_some_and(|name| !name.contains('/') && !name.is_empty()),
        "endpoint namespace must be the module or its owning host"
    );
    anyhow::ensure!(
        bindings.namespace == bindings.endpoint_namespace || bindings.children.is_empty(),
        "owned children cannot delegate endpoints"
    );
    anyhow::ensure!(
        bindings.namespace == id,
        "module bindings namespace '{}' differs from --name '{id}'",
        bindings.namespace
    );
    for (name, input) in &bindings.inputs {
        validate_segment("input port", name)?;
        validate_contract(&input.contract, true)?;
        anyhow::ensure!(!input.sources.is_empty(), "input '{name}' has no sources");
        let mut sources = BTreeSet::new();
        for source in &input.sources {
            validate_namespace(source)?;
            anyhow::ensure!(sources.insert(source), "duplicate input source '{source}'");
        }
        anyhow::ensure!(
            input.contract.kind != PortKind::Rpc || input.sources.len() == 1,
            "RPC input '{name}' requires exactly one source"
        );
    }
    let mut types = BTreeSet::new();
    for (name, output) in &bindings.outputs {
        validate_segment("output port", name)?;
        validate_contract(&output.contract, false)?;
        anyhow::ensure!(
            types.insert(&output.contract.type_name),
            "duplicate output type '{}' in module bindings",
            output.contract.type_name
        );
    }
    let mut delegated_inputs = BTreeSet::new();
    let mut delegated_outputs = BTreeSet::new();
    for (name, child) in &bindings.children {
        validate_segment("owned child", name)?;
        anyhow::ensure!(
            !matches!(name.as_str(), "." | ".."),
            "invalid owned child name"
        );
        for input in &child.inputs {
            anyhow::ensure!(
                bindings.inputs.contains_key(input),
                "child '{name}' delegates undefined input '{input}'"
            );
            anyhow::ensure!(
                delegated_inputs.insert(input),
                "input '{input}' delegated more than once"
            );
        }
        for output in &child.outputs {
            anyhow::ensure!(
                bindings.outputs.contains_key(output),
                "child '{name}' delegates undefined output '{output}'"
            );
            anyhow::ensure!(
                delegated_outputs.insert(output),
                "output '{output}' delegated more than once"
            );
        }
    }
    Ok(())
}

fn validate_contract(contract: &EndpointContract, input: bool) -> Result<()> {
    validate_segment("endpoint type", &contract.type_name)?;
    anyhow::ensure!(
        contract.buffer != ReceiveBuffer::Latest(0),
        "latest buffer depth must be positive"
    );
    match contract.kind {
        PortKind::Stream => anyhow::ensure!(
            contract.response.is_none(),
            "stream cannot declare a response"
        ),
        PortKind::Rpc => {
            let response = contract
                .response
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("RPC requires a response type"))?;
            validate_segment("response type", response)?;
            anyhow::ensure!(
                contract.retention == Retention::Volatile
                    && contract.replay == Replay::Live
                    && contract.buffer == ReceiveBuffer::Fifo
                    && contract.qos == QosProfile::Reliable,
                "stream policies do not apply to RPC"
            );
        }
    }
    anyhow::ensure!(
        !input
            || (contract.qos == QosProfile::Reliable && contract.retention == Retention::Volatile),
        "qos and retention belong to outputs"
    );
    anyhow::ensure!(
        input || (contract.replay == Replay::Live && contract.buffer == ReceiveBuffer::Fifo),
        "replay and buffer belong to inputs"
    );
    Ok(())
}

pub(crate) fn read_bindings(path: &Path) -> Result<ModuleBindings> {
    anyhow::ensure!(
        path.is_absolute(),
        "--module-bindings requires an absolute path"
    );
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading module bindings {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing module bindings {}", path.display()))
}

pub(crate) fn write_report(path: &Path, report: &ModuleReport) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(report)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .with_context(|| format!("creating module report {}", temporary.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err()
        && let Err(error) = std::fs::remove_file(&temporary)
    {
        tracing::warn!(path = %temporary.display(), %error, "module report cleanup failed");
    }
    result.with_context(|| format!("publishing module report {}", path.display()))
}
