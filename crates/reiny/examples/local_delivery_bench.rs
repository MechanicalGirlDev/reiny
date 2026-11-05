//! Local-only microbenchmark evidence, not a timing test or a universal zero-copy claim.
//! Run: `cargo run --release -p reiny --example local_delivery_bench`
//! Envelope receive variants preserve the `recv`/`recv_shared` paths while checking provenance.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use prost::Message;
use reiny::bindings::{
    EndpointAddress, EndpointContract, InputBinding, MODULE_BINDINGS_VERSION, ModuleBindings,
    OutputBinding,
};
use reiny::engine::Local;
use reiny::{Descriptor, MessageType, ModuleContext, Publisher, RuntimeOptions, Subscriber};
use tokio::time::timeout;

const SOURCE: &str = "local-delivery-bench/producer";
const PORT: &str = "payload";
const WARMUP: u32 = 128;
const ITERATIONS: u32 = 2_000;
const DEADLINE: Duration = Duration::from_mins(2);

#[derive(Clone, PartialEq, Message)]
struct Payload {
    #[prost(uint64, tag = "1")]
    value: u64,
    #[prost(bytes = "vec", tag = "2")]
    bytes: Vec<u8>,
}

// Self-contained example contract; applications normally generate this from protobuf.
impl MessageType for Payload {
    const TYPE_NAME: &'static str = "local_delivery_bench.Payload";
    const SCHEMA_FINGERPRINT: u64 = 0x4c44_4245_4e43_4801;
    const DESCRIPTOR: Option<Descriptor> = None;
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    SerializedOwned,
    SharedTyped,
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

fn options(engine: &Arc<Local>, bindings: ModuleBindings) -> RuntimeOptions {
    let mut options = RuntimeOptions::new(&bindings.namespace);
    "local-delivery-bench".clone_into(&mut options.domain);
    options.engine = Some(engine.clone());
    options.module_bindings = Some(bindings);
    options.module_report_path = None;
    options
}

fn validate(actual: &Payload, source: (&str, &str), expected: &Payload) -> Result<()> {
    ensure!(actual.value == expected.value, "incorrect scalar value");
    ensure!(actual.bytes == expected.bytes, "incorrect payload bytes");
    ensure!(
        source == (SOURCE, PORT),
        "incorrect source endpoint: {source:?}"
    );
    Ok(())
}

struct DeliveryRun {
    outgoing: Publisher<Payload>,
    incoming: Vec<Subscriber<Payload>>,
    payload: Arc<Payload>,
}

impl DeliveryRun {
    async fn rounds(&mut self, mode: Mode, iterations: u32) -> Result<()> {
        for _ in 0..iterations {
            match mode {
                Mode::SerializedOwned => self.outgoing.send((*self.payload).clone()).await?,
                Mode::SharedTyped => {
                    self.outgoing.send_shared(Arc::clone(&self.payload)).await?;
                }
            }
            // Receiving every subscriber is the completion handshake before the next send.
            // At most one publication is in flight, so no FIFO overflow or startup race.
            for input in &mut self.incoming {
                match mode {
                    Mode::SerializedOwned => {
                        let received = input.recv_envelope().await.context("owned input closed")?;
                        validate(
                            &received.value,
                            (&received.source, &received.source_port),
                            &self.payload,
                        )?;
                    }
                    Mode::SharedTyped => {
                        let received = input
                            .recv_shared_envelope()
                            .await
                            .context("shared input closed")?;
                        validate(
                            &received.value,
                            (&received.source, &received.source_port),
                            &self.payload,
                        )?;
                        ensure!(
                            Arc::ptr_eq(&received.value, &self.payload),
                            "same-type Local shared delivery lost allocation identity"
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

async fn run_case(mode: Mode, payload_bytes: u32, subscribers: u32) -> Result<()> {
    let engine = Arc::new(Local::new());
    let contract = EndpointContract {
        type_name: Payload::TYPE_NAME.to_owned(),
        ..EndpointContract::default()
    };
    let mut producer_bindings = bindings(SOURCE);
    producer_bindings.outputs.insert(
        PORT.to_owned(),
        OutputBinding {
            contract: contract.clone(),
        },
    );
    let producer = ModuleContext::open(options(&engine, producer_bindings)).await?;
    let mut consumers = Vec::new();
    let mut incoming = Vec::new();
    for index in 0..subscribers {
        let mut consumer_bindings = bindings(&format!("local-delivery-bench/consumer-{index}"));
        consumer_bindings.inputs.insert(
            "incoming".to_owned(),
            InputBinding {
                contract: contract.clone(),
                sources: vec![EndpointAddress {
                    namespace: SOURCE.to_owned(),
                    port: PORT.to_owned(),
                }],
            },
        );
        let consumer = ModuleContext::open(options(&engine, consumer_bindings)).await?;
        incoming.push(consumer.input::<Payload>("incoming")?);
        consumer.ready()?;
        consumers.push(consumer);
    }
    let outgoing = producer.output::<Payload>(PORT)?;
    producer.ready()?;
    let payload = Arc::new(Payload {
        value: 42,
        bytes: (0..usize::try_from(payload_bytes)?)
            .map(|index| u8::try_from(index % 251))
            .collect::<std::result::Result<Vec<_>, _>>()?,
    });
    let encoded_bytes = payload.encoded_len();
    let mut run = DeliveryRun {
        outgoing,
        incoming,
        payload,
    };
    // Local orders declaration before publication. Fully received warmup rounds also
    // establish an explicit delivery handshake before starting the measured interval.
    run.rounds(mode, WARMUP).await?;
    let started = Instant::now();
    run.rounds(mode, ITERATIONS).await?;
    let elapsed = started.elapsed();
    let publications = f64::from(ITERATIONS);
    let deliveries = publications * f64::from(subscribers);
    println!(
        "{mode:?} bytes={payload_bytes} encoded={encoded_bytes} subscribers={subscribers} \
         iterations={ITERATIONS} elapsed_ms={:.3} publications/s={:.1} deliveries/s={:.1} \
         delivered_MiB/s={:.2} completion_us/publication={:.3}",
        elapsed.as_secs_f64() * 1_000.0,
        publications / elapsed.as_secs_f64(),
        deliveries / elapsed.as_secs_f64(),
        deliveries * f64::from(payload_bytes) / elapsed.as_secs_f64() / 1_048_576.0,
        elapsed.as_secs_f64() * 1_000_000.0 / publications,
    );
    drop(run);
    producer.shutdown_now();
    for consumer in &consumers {
        consumer.shutdown_now();
    }
    producer.shutdown().await;
    for consumer in consumers {
        consumer.shutdown().await;
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let build = if cfg!(debug_assertions) {
        "debug (debug assertions enabled)"
    } else {
        "release (debug assertions disabled)"
    };
    println!(
        "Reiny Local delivery microbenchmark; platform={}/{}; build={build}",
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
    println!("Runtime: current-thread Tokio caller + Local dispatcher thread; warmup={WARMUP}");
    println!("Fixed order: serialized then shared; 1 then 4 subscribers; no statistical claims.");
    println!("Payload bytes exclude protobuf framing; identical immutable values are prebuilt.");
    println!(
        "Timed: send (owned clone or Arc clone), dispatch, receive, full bytes/value/source validation."
    );
    println!(
        "Latency = elapsed / publications, through validation at ALL subscribers, not a percentile."
    );
    println!(
        "Throughput = completed publications or deliveries / elapsed; MiB uses delivered payload bytes."
    );
    println!("Sequential completion handshakes bound in-flight publications to one; no sleeps.");
    println!("Shared allocation identity is checked only for this same-type Local path.");
    println!(
        "Evidence only: not a timing assertion, universal zero-copy, IPC, GPU, or real-time claim."
    );
    timeout(DEADLINE, async {
        for subscribers in [1, 4] {
            for payload_bytes in [64, 1_024, 65_536] {
                for mode in [Mode::SerializedOwned, Mode::SharedTyped] {
                    run_case(mode, payload_bytes, subscribers).await?;
                }
            }
        }
        Result::<()>::Ok(())
    })
    .await
    .context("benchmark exceeded bounded overall deadline")??;
    println!("All warmup and measured deliveries validated; contexts shut down.");
    Ok(())
}
