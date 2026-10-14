use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use prost::Message;
use reiny::bindings::{
    EndpointAddress, EndpointContract, InputBinding, MODULE_BINDINGS_VERSION, ModuleBindings,
    OutputBinding,
};
use reiny::engine::{Engine, Key, Local};
use reiny::{ModuleContext, Qos, RuntimeOptions};
use tokio::time::timeout;

const DOMAIN: &str = "schema-fingerprint";
const SOURCE: &str = "qa/source";
const RECEIVER: &str = "qa/receiver";
const TYPE_NAME: &str = "qa.Envelope";

#[derive(Clone, PartialEq, Message)]
struct Child {
    #[prost(uint32, tag = "1")]
    value: u32,
    #[prost(int32, tag = "2")]
    kind: i32,
}

#[derive(Clone, PartialEq, Message)]
struct Envelope {
    #[prost(message, optional, tag = "1")]
    child: Option<Child>,
}

fn bindings(namespace: &str) -> ModuleBindings {
    ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: namespace.to_owned(),
        endpoint_namespace: namespace.to_owned(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    }
}

fn options(bus: Arc<dyn Engine>, bindings: ModuleBindings) -> RuntimeOptions {
    let mut options = RuntimeOptions::new(&bindings.namespace);
    options.domain = DOMAIN.to_owned();
    options.engine = Some(bus);
    options.module_bindings = Some(bindings);
    options.module_report_path = None;
    options.install_tracing = false;
    options
}

fn payload(value: u32) -> Vec<u8> {
    Envelope {
        child: Some(Child { value, kind: 0 }),
    }
    .encode_to_vec()
}

pub async fn verify(expected: u64, incompatible: [u64; 2]) -> Result<()> {
    // Given live named endpoints on the real, ordered Local transport.
    let bus = Arc::new(Local::new());
    let contract = EndpointContract {
        type_name: TYPE_NAME.to_owned(),
        ..EndpointContract::default()
    };
    let mut source_bindings = bindings(SOURCE);
    source_bindings.outputs.insert(
        "events".to_owned(),
        OutputBinding {
            contract: contract.clone(),
        },
    );
    let mut receiver_bindings = bindings(RECEIVER);
    receiver_bindings.inputs.insert(
        "incoming".to_owned(),
        InputBinding {
            contract,
            sources: vec![EndpointAddress {
                namespace: SOURCE.to_owned(),
                port: "events".to_owned(),
            }],
        },
    );
    let source = ModuleContext::open(options(bus.clone(), source_bindings)).await?;
    let receiver = ModuleContext::open(options(bus.clone(), receiver_bindings)).await?;
    let output = source.raw_output("events", TYPE_NAME, expected)?;
    let mut input = receiver.raw_input("incoming", TYPE_NAME, expected)?;
    source.ready()?;
    receiver.ready()?;
    let injector = bus.publisher(
        &Key::output(DOMAIN, SOURCE, "events", TYPE_NAME),
        &Qos::DEFAULT,
    )?;

    // When decodable, incompatible bytes precede the known valid sample.
    for fingerprint in incompatible {
        injector.put(payload(98), Some(fingerprint.to_le_bytes().to_vec()))?;
    }
    output.send(payload(42))?;

    // Then the real SDK delivers only the valid sample, not merely decodable bytes.
    let received = timeout(Duration::from_secs(5), input.recv())
        .await
        .context("bounded receive timed out")?
        .context("input closed")?;
    ensure!(received.source == SOURCE, "wrong source");
    ensure!(received.schema == Some(expected), "wrong fingerprint");
    let decoded = Envelope::decode(received.payload.as_slice())?;
    ensure!(
        decoded.child.context("missing child")?.value == 42,
        "incompatible schema reached the application"
    );
    Ok(())
}
