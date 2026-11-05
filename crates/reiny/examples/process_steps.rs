//! Real two-process A(host1) -> B(host2) -> C(host1), with delayed C -> A.
//! Run: `cargo run -p reiny --no-default-features --example process_steps`
use anyhow::{Result, ensure};
use reiny::{
    MessageType,
    bindings::{MODULE_BINDINGS_VERSION, ModuleBindings},
    component::{
        ComponentContext, ComponentFuture, ComponentHost, ComponentInstanceSpec, ComponentRegistry,
        HostOptions, StopReason,
        process::{
            Bootstrap, ProcessHost, ProcessLimits, ProcessRegistry, ProcessStepOwner,
            ProcessStepPeer, ProcessStepPlan, ProcessStepSeed, ProcessStepSource, encode_payload,
            protocol::{
                ComponentAssignment, FactoryRegistration, GraphGeneration, HostGeneration, HostId,
                Milliseconds, Nonce, OwnerGeneration, Prepare, PreparedDigest, SessionIdentity,
                TransportSettings, frame_header,
                step::{StepAssignment, StepSessionKey, WireSchema, WireStepType},
            },
        },
        step::{StepComponent, StepComponentFactory},
    },
    step::{
        NodeError, StepFailureKind, StepFuture, StepGraph, StepInput, StepInputs, StepOutput,
        StepOutputs, StepRequest, StepSchema, StepSource, StepTick, StepType, StepValue,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::TcpListener, process::Command, time::timeout};

const BUDGET: Duration = Duration::from_secs(10);

#[path = "process_steps/component.rs"]
mod component;
#[path = "process_steps/fault.rs"]
mod fault;
#[path = "process_steps/local.rs"]
mod local;
#[path = "process_steps/peers.rs"]
mod peers;
use component::{
    Factory, Number, bindings, graph, input, output, registration, request, schema, value,
    wire_schema,
};
use fault::fault_fixture;
use local::compare_local;
use peers::spawn_hosts;

fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--host") {
        let bootstrap = Bootstrap::read(&mut std::io::stdin().lock())?;
        let mut registry = ProcessRegistry::new();
        registry.register_step_prepared(
            registration(),
            schema(),
            BTreeSet::from(["out".into()]),
            Arc::new(Factory),
        )?;
        let outcome = ProcessHost::run(
            bootstrap,
            registry,
            ProcessLimits {
                startup: BUDGET,
                shutdown: Duration::from_secs(1),
                frame: BUDGET,
                instances: 8,
            },
        )?;
        eprintln!("host pid={} terminal={outcome:?}", std::process::id());
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(owner())
}
async fn owner() -> Result<()> {
    let fault_mode = std::env::args().nth(1);
    ensure!(
        fault_mode
            .as_deref()
            .is_none_or(|mode| matches!(mode, "--cancel-blocked" | "--kill-blocked")),
        "unknown mode"
    );
    let mut entropy = std::fs::File::open("/dev/urandom")?;
    let mut random = [0; 32];
    std::io::Read::read_exact(&mut entropy, &mut random)?;
    let key = StepSessionKey {
        incarnation: random,
        owner_generation: OwnerGeneration(1),
        graph_generation: GraphGeneration(1),
        graph_digest: PreparedDigest("explicit-counter-fixture-v1".into()),
        plan_digest: PreparedDigest("counter-seed-17-v1".into()),
    };
    let (peers, children) = spawn_hosts(&key, fault_mode.as_deref(), &mut entropy).await?;
    let draft = ProcessStepPlan {
        nodes: ["a", "b", "c"]
            .into_iter()
            .map(|id| (id.into(), schema()))
            .collect(),
        inputs: BTreeMap::from([
            (input("a", "x"), ProcessStepSource::External),
            (
                input("a", "feedback"),
                ProcessStepSource::PreviousTick {
                    source: output("c"),
                    initial: ProcessStepSeed::PreparedOutput(output("c")),
                },
            ),
            (input("b", "x"), ProcessStepSource::SameTick(output("a"))),
            (input("b", "feedback"), ProcessStepSource::External),
            (input("c", "x"), ProcessStepSource::SameTick(output("b"))),
            (input("c", "feedback"), ProcessStepSource::External),
        ]),
    };
    let (owner, steps) =
        ProcessStepOwner::prepare(peers, key, draft, Duration::ZERO, BUDGET, |reports, _| {
            let seed = &reports["c"].outputs["out"];
            ensure!(
                Number::decode(seed.bytes.as_slice())?.value == 17,
                "actual prepared seed"
            );
            Ok(())
        })
        .await?;
    if let Some(mode) = fault_mode {
        return fault_fixture(owner, steps, children, &mode).await;
    }
    compare_local(owner, steps, children).await
}

use prost::Message;
