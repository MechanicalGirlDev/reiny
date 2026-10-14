//! Forward declared ports between engines, keeping application routing manifest-authoritative.
//!
//! Each input on one engine feeds a matching output on the other. Outputs use the bridge's
//! declared endpoint namespace and policy, not an arbitrary original publisher identity.
//! Message and RPC fingerprints come from the compiled schemas supplied by the caller.
//! Launch, readiness and stop are control-plane routes and retain their original namespaces.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::bindings::{EndpointContract, ModuleBindings, PortKind};
use crate::engine::{
    Engine, Guard, Key, LAUNCH_CHUNK, Presence, QueryParams, READY_CHUNK, RawQuery, SERVICE_CHUNK,
    STOP_CHUNK,
};
use crate::{ModuleContext, Result};

const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Live declared forwarding routes. Dropping them withdraws readiness and stops forwarding.
pub struct Bridge {
    tasks: Vec<JoinHandle<()>>,
    guards: Vec<Guard>,
    ports: Vec<crate::managed::ManagedPort>,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn same_type(a: &EndpointContract, b: &EndpointContract) -> bool {
    a.type_name == b.type_name && a.kind == b.kind && a.response == b.response
}

fn validate_routes(from: &ModuleBindings, into: &ModuleBindings) -> Result<()> {
    for (name, input) in &from.inputs {
        anyhow::ensure!(
            !input
                .sources
                .iter()
                .any(|source| source.namespace == from.endpoint_namespace),
            "bridge input '{name}' cannot consume its own output namespace"
        );
        anyhow::ensure!(
            into.outputs
                .get(name)
                .is_some_and(|output| same_type(&input.contract, &output.contract)),
            "bridge input '{name}' has no matching declared output on the other engine"
        );
    }
    for (name, output) in &into.outputs {
        anyhow::ensure!(
            from.inputs
                .get(name)
                .is_some_and(|input| same_type(&input.contract, &output.contract)),
            "bridge output '{name}' has no matching declared input on the other engine"
        );
    }
    Ok(())
}

/// Join matching declared ports on two engines with the same runtime id and domain.
///
/// `schemas` maps each declared protobuf FQN to its compiled fingerprint, including RPC responses.
/// Register all routes before calling `ready()` on the contexts. No undeclared type or source is
/// forwarded, and missing schema metadata is an error rather than a raw transport fallback.
pub fn forward(
    a: &ModuleContext,
    b: &ModuleContext,
    schemas: &BTreeMap<String, u64>,
) -> Result<Bridge> {
    anyhow::ensure!(
        a.id() == b.id() && a.domain() == b.domain(),
        "bridge contexts must share id and domain"
    );
    let bindings_a = a
        .module
        .bindings
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("bridge requires module bindings"))?;
    let bindings_b = b
        .module
        .bindings
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("bridge requires module bindings"))?;
    validate_routes(bindings_a, bindings_b)?;
    validate_routes(bindings_b, bindings_a)?;
    for bindings in [bindings_a, bindings_b] {
        for contract in bindings
            .inputs
            .values()
            .map(|port| &port.contract)
            .chain(bindings.outputs.values().map(|port| &port.contract))
        {
            for ty in std::iter::once(&contract.type_name).chain(contract.response.iter()) {
                anyhow::ensure!(
                    schemas.contains_key(ty),
                    "bridge lacks compiled schema for '{ty}'"
                );
            }
        }
    }
    let mut bridge = Bridge {
        tasks: Vec::new(),
        guards: Vec::new(),
        ports: Vec::new(),
    };
    forward_ports(a, b, schemas, &mut bridge)?;
    forward_ports(b, a, schemas, &mut bridge)?;
    let into_a = Arc::new(Mutex::new(HashMap::new()));
    let into_b = Arc::new(Mutex::new(HashMap::new()));
    bridge
        .tasks
        .push(control_flow(a, b, into_b.clone(), into_a.clone())?);
    bridge.tasks.push(control_flow(b, a, into_a, into_b)?);
    Ok(bridge)
}

fn forward_ports(
    from: &ModuleContext,
    into: &ModuleContext,
    schemas: &BTreeMap<String, u64>,
    bridge: &mut Bridge,
) -> Result<()> {
    let inputs = &from
        .module
        .bindings
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing bridge inputs"))?
        .inputs;
    let outputs = &into
        .module
        .bindings
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing bridge outputs"))?
        .outputs;
    let mut publishers = BTreeMap::new();
    for (name, input) in inputs {
        let output_name = name;
        let output = outputs
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("missing matching bridge output"))?;
        let schema = schemas[&input.contract.type_name];
        match input.contract.kind {
            PortKind::Stream => {
                let mut subscriber = from.raw_input(name, &input.contract.type_name, schema)?;
                let publisher = if let Some(publisher) = publishers.get(output_name) {
                    Arc::clone(publisher)
                } else {
                    let publisher = Arc::new(into.raw_output(
                        output_name,
                        &output.contract.type_name,
                        schema,
                    )?);
                    publishers.insert(output_name.clone(), publisher.clone());
                    publisher
                };
                let shutdown = into.shutdown_handle();
                bridge.tasks.push(tokio::spawn(async move {
                    loop {
                        let sample = tokio::select! {
                            biased;
                            () = shutdown.wait() => break,
                            sample = subscriber.recv() => match sample {
                                Some(sample) => sample,
                                None => break,
                            },
                        };
                        if let Err(error) = publisher.send(sample.payload) {
                            tracing::warn!(%error, "bridge publication failed");
                            break;
                        }
                    }
                }));
            }
            PortKind::Rpc => forward_rpc(from, into, name, output_name, input, schemas, bridge)?,
        }
    }
    Ok(())
}

fn forward_rpc(
    from: &ModuleContext,
    into: &ModuleContext,
    name: &str,
    output_name: &str,
    input: &crate::bindings::InputBinding,
    schemas: &BTreeMap<String, u64>,
    bridge: &mut Bridge,
) -> Result<()> {
    let schema = schemas[&input.contract.type_name];
    let response = input
        .contract
        .response
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("RPC requires response type"))?;
    let response_schema = schemas[response];
    let source = input
        .sources
        .first()
        .ok_or_else(|| anyhow::anyhow!("RPC requires exact target"))?;
    let ty = &input.contract.type_name;
    let target = Key::output(from.domain(), &source.namespace, &source.port, ty);
    let output_key = Key::output(into.domain(), into.endpoint_namespace(), output_name, ty);
    let engine = from.engine().clone();
    let handle = Handle::current();
    let reply_key = output_key.clone();
    bridge
        .ports
        .push(from.bridge_rpc_port(name, true, schema, response_schema)?);
    bridge
        .ports
        .push(into.bridge_rpc_port(output_name, false, schema, response_schema)?);
    bridge.guards.push(into.engine().respond(
        &output_key,
        Box::new(move |query| {
            if query.payload().is_none() {
                return;
            }
            let engine = engine.clone();
            let target = target.clone();
            let reply_key = reply_key.clone();
            handle.spawn(relay_rpc(
                engine,
                target,
                reply_key,
                query,
                schema,
                response_schema,
            ));
        }),
    )?);
    bridge.guards.push(
        into.engine()
            .declare_alive(&output_key.with_chunk(SERVICE_CHUNK))?,
    );

    Ok(())
}

async fn relay_rpc(
    engine: Arc<dyn Engine>,
    target: Key,
    output: Key,
    query: Box<dyn RawQuery>,
    request_schema: u64,
    response_schema: u64,
) {
    if query.attachment() != Some(request_schema.to_le_bytes().as_slice()) {
        if let Err(error) = query.reply_err(b"request schema mismatch".to_vec()) {
            tracing::warn!(%error, "bridge request rejection failed");
        }
        return;
    }
    let params = QueryParams {
        payload: query.payload().map(<[u8]>::to_vec),
        attachment: Some(request_schema.to_le_bytes().to_vec()),
        timeout: QUERY_TIMEOUT,
    };
    let result = async {
        let mut replies = engine.query(&target, params)?;
        let reply = replies.next().await;
        if let Some(Ok(sample)) = &reply {
            anyhow::ensure!(
                sample.key == target
                    && sample.attachment.as_deref()
                        == Some(response_schema.to_le_bytes().as_slice()),
                "bridge response source or schema mismatch"
            );
        }
        Ok::<_, anyhow::Error>(reply)
    }
    .await;
    let sent = match result {
        Ok(Some(Ok(sample))) => query.reply(&output, sample.payload, sample.attachment),
        Ok(Some(Err(message))) => query.reply_err(message),
        Ok(None) => return,
        Err(error) => query.reply_err(error.to_string().into_bytes()),
    };
    if let Err(error) = sent {
        tracing::warn!(%error, "bridge RPC reply failed");
    }
}

type Injected = Arc<Mutex<HashMap<String, usize>>>;

fn control_flow(
    from: &ModuleContext,
    into: &ModuleContext,
    injected: Injected,
    echoes: Injected,
) -> Result<JoinHandle<()>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut watches = Vec::new();
    for key in [
        Key::launch(from.domain(), None),
        Key::module(from.domain(), None, READY_CHUNK),
    ] {
        let tx = tx.clone();
        watches.push(from.engine().watch_alive(
            &key,
            Box::new(move |event| {
                if let Err(error) = tx.send(event) {
                    tracing::debug!(%error, "bridge control watch closed");
                }
            }),
        )?);
    }
    let source_engine = from.engine().clone();
    let destination = into.engine().clone();
    let id = from.id().to_string();
    let handle = Handle::current();
    Ok(tokio::spawn(async move {
        let _watches = watches;
        let mut tokens = HashMap::new();
        let mut stops = HashMap::new();
        while let Some(event) = rx.recv().await {
            let key = match &event {
                Presence::Joined(key) | Presence::Left(key) => key,
            };
            let Some(source) = key.source.as_ref() else {
                continue;
            };
            if source == &id
                || echoes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains_key(source)
            {
                continue;
            }
            match event {
                Presence::Joined(key) => {
                    if tokens.contains_key(&key) {
                        continue;
                    }
                    let source = key.source.clone().unwrap_or_default();
                    *injected
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .entry(source.clone())
                        .or_default() += 1;
                    match destination.declare_alive(&key) {
                        Ok(token) => {
                            tokens.insert(key.clone(), token);
                        }
                        Err(error) => {
                            tracing::warn!(%error, "bridge control presence failed");
                            remove_injected(&injected, &source);
                            continue;
                        }
                    }
                    if key.ty.as_deref() == Some(LAUNCH_CHUNK) {
                        let target = Key::module(&key.domain, key.source.as_deref(), STOP_CHUNK);
                        let callback_target = target.clone();
                        let engine = source_engine.clone();
                        let handle = handle.clone();
                        match destination.respond(
                            &target,
                            Box::new(move |query| {
                                let engine = engine.clone();
                                let target = callback_target.clone();
                                handle.spawn(relay_stop(engine, target, query));
                            }),
                        ) {
                            Ok(guard) => {
                                stops.insert(key, guard);
                            }
                            Err(error) => tracing::warn!(%error, "bridge stop route failed"),
                        }
                    }
                }
                Presence::Left(key) => {
                    if tokens.remove(&key).is_some() {
                        if let Some(source) = key.source.as_deref() {
                            remove_injected(&injected, source);
                        }
                        stops.remove(&key);
                    }
                }
            }
        }
    }))
}

async fn relay_stop(engine: Arc<dyn Engine>, target: Key, query: Box<dyn RawQuery>) {
    let params = QueryParams {
        payload: query.payload().map(<[u8]>::to_vec),
        attachment: None,
        timeout: QUERY_TIMEOUT,
    };
    let mut replies = match engine.query(&target, params) {
        Ok(replies) => replies,
        Err(error) => {
            tracing::warn!(%error, "bridge stop query failed");
            return;
        }
    };
    let sent = match replies.next().await {
        Some(Ok(sample)) => query.reply(&sample.key, sample.payload, sample.attachment),
        Some(Err(message)) => query.reply_err(message),
        None => return,
    };
    if let Err(error) = sent {
        tracing::warn!(%error, "bridge stop relay failed");
    }
}

fn remove_injected(injected: &Injected, source: &str) {
    let mut sources = injected.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(count) = sources.get_mut(source) {
        *count -= 1;
        if *count == 0 {
            sources.remove(source);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests;
