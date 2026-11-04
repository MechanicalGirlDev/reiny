//! Two independently bound module contexts communicating over one Local engine.
//! Run from the workspace root: `cargo run -p reiny --example local_components`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use prost::Message;
use reiny::bindings::{
    EndpointAddress, EndpointContract, InputBinding, MODULE_BINDINGS_VERSION, ModuleBindings,
    OutputBinding,
};
use reiny::engine::Local;
use reiny::{Descriptor, MessageType, ModuleContext, RuntimeOptions};
use tokio::time::timeout;

const SENSOR: &str = "local-components/sensor";
const DISPLAY: &str = "local-components/display";
const PATIENCE: Duration = Duration::from_secs(5);

#[derive(Clone, PartialEq, Message)]
struct TemperatureReading {
    #[prost(string, tag = "1")]
    sensor: String,
    #[prost(sint32, tag = "2")]
    millidegrees_celsius: i32,
}

// Kept inline for a self-contained example. Applications normally use reiny-build
// to generate MessageType and its schema fingerprint from their protobuf catalog.
impl MessageType for TemperatureReading {
    const TYPE_NAME: &'static str = "local_components.TemperatureReading";
    const SCHEMA_FINGERPRINT: u64 = 0x4c4f_4341_4c54_454d;
    const DESCRIPTOR: Option<Descriptor> = None;
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

fn options(engine: Arc<Local>, bindings: ModuleBindings) -> RuntimeOptions {
    let mut options = RuntimeOptions::new(&bindings.namespace);
    "local-components".clone_into(&mut options.domain);
    options.engine = Some(engine);
    options.module_bindings = Some(bindings);
    // This standalone example does not write a managed deployment report.
    options.module_report_path = None;
    options
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = Arc::new(Local::new());
    let contract = EndpointContract {
        type_name: TemperatureReading::TYPE_NAME.to_owned(),
        ..EndpointContract::default()
    };
    let mut sensor_bindings = bindings(SENSOR);
    sensor_bindings.outputs.insert(
        "temperature".to_owned(),
        OutputBinding {
            contract: contract.clone(),
        },
    );
    let mut display_bindings = bindings(DISPLAY);
    display_bindings.inputs.insert(
        "reading".to_owned(),
        InputBinding {
            contract,
            sources: vec![EndpointAddress {
                namespace: SENSOR.to_owned(),
                port: "temperature".to_owned(),
            }],
        },
    );

    let sensor = ModuleContext::open(options(engine.clone(), sensor_bindings))
        .await
        .context("open sensor context")?;
    let display = ModuleContext::open(options(engine.clone(), display_bindings))
        .await
        .context("open display context")?;
    let outgoing = sensor.output::<TemperatureReading>("temperature")?;
    let mut incoming = display.input::<TemperatureReading>("reading")?;
    sensor.ready().context("sensor readiness")?;
    display.ready().context("display readiness")?;
    println!("Ready: {SENSOR}:temperature -> {DISPLAY}:reading (shared Local engine)");

    let reading = Arc::new(TemperatureReading {
        sensor: "motor-housing".to_owned(),
        millidegrees_celsius: 42_500,
    });
    outgoing
        .send_shared(Arc::clone(&reading))
        .await
        .context("send reading")?;
    let envelope = timeout(PATIENCE, incoming.recv_shared_envelope())
        .await
        .context("timed out receiving temperature")?
        .context("display input closed before delivery")?;
    ensure!(
        Arc::ptr_eq(&envelope.value, &reading),
        "local delivery did not share the published value"
    );
    ensure!(
        envelope.source == SENSOR && envelope.source_port == "temperature",
        "unexpected source {}:{}",
        envelope.source,
        envelope.source_port
    );
    println!(
        "Received: {} = {} millidegrees Celsius from {}:{}; shared value and source verified",
        envelope.value.sensor,
        envelope.value.millidegrees_celsius,
        envelope.source,
        envelope.source_port
    );

    sensor.shutdown_now();
    display.shutdown_now();
    timeout(PATIENCE, async {
        tokio::join!(sensor.shutdown(), display.shutdown());
    })
    .await
    .context("timed out shutting down contexts")?;
    drop((incoming, outgoing, display, sensor, engine));
    println!("Shutdown complete: both contexts and their Local engine handles released");
    Ok(())
}
