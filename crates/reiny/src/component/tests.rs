//! Host-level contracts over the real Local engine and deterministic lifecycle signals.

#![expect(
    clippy::expect_used,
    reason = "test setup and bounded observations fail loudly"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

use crate::bindings::{MODULE_BINDINGS_VERSION, ModuleBindings};
use crate::engine::{Engine, Local};

use super::*;

mod admission;
mod context;
mod graph;
mod lifecycle;
mod rejection;
mod staged;
mod support;
mod unresponsive;

const PATIENCE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum Behavior {
    Idle,
    FactoryError,
    FactoryPanic,
    PrepareError,
    PreparePanic,
    PrepareWait,
    RunError,
    RunPanic,
    RunReturn,
    StopError,
    StopPanic,
    StopWait,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Prepare,
    Run,
    Stop(StopReason),
    Dropped,
}

#[derive(Clone, Debug)]
struct Record {
    id: String,
    stage: Stage,
}

struct Factory {
    behavior: Behavior,
    records: mpsc::Sender<Record>,
    gate: watch::Receiver<bool>,
}

struct TestComponent {
    behavior: Behavior,
    records: mpsc::Sender<Record>,
    gate: watch::Receiver<bool>,
    id: String,
}

fn bindings(id: &str) -> ModuleBindings {
    ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: id.to_owned(),
        endpoint_namespace: id.to_owned(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    }
}

fn instance(id: &str, factory: &str) -> ComponentInstanceSpec {
    ComponentInstanceSpec {
        factory_id: factory.to_owned(),
        bindings: bindings(id),
        config: None,
        config_origin: None,
    }
}

fn options(engine: Arc<dyn Engine>) -> HostOptions {
    HostOptions {
        domain: "components".to_owned(),
        engine,
        startup_timeout: PATIENCE,
        stop_timeout: PATIENCE,
    }
}

fn fixture(
    behavior: Behavior,
) -> (
    Arc<ComponentRegistry>,
    mpsc::Receiver<Record>,
    watch::Sender<bool>,
) {
    let (records, rx) = mpsc::channel(128);
    let (gate, gate_rx) = watch::channel(false);
    let mut registry = ComponentRegistry::new();
    registry
        .register(
            "test".to_owned(),
            Arc::new(Factory {
                behavior,
                records: records.clone(),
                gate: gate_rx.clone(),
            }),
        )
        .expect("register test");
    registry
        .register(
            "peer".to_owned(),
            Arc::new(Factory {
                behavior: Behavior::Idle,
                records,
                gate: gate_rx,
            }),
        )
        .expect("register peer");
    (Arc::new(registry), rx, gate)
}

async fn stage(records: &mut mpsc::Receiver<Record>, expected: Stage) -> Record {
    timeout(PATIENCE, async {
        loop {
            let record = records.recv().await.expect("component observation");
            if record.stage == expected {
                return record;
            }
        }
    })
    .await
    .expect("stage deadline")
}

async fn terminal(handle: &HostHandle) -> HostOutcome {
    timeout(PATIENCE, handle.wait_terminal())
        .await
        .expect("terminal deadline")
}

fn failure(outcome: HostOutcome) -> (HostError, Vec<HostError>) {
    match outcome {
        HostOutcome::Failed { primary, cleanup } => (primary, cleanup),
        HostOutcome::Stopped | HostOutcome::Unresponsive { .. } => {
            unreachable!("expected joined failure, got {outcome:?}")
        }
    }
}
