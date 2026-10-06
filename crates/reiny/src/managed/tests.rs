//! Deterministic managed-runtime regressions using the real ordered local engine.

#![expect(
    clippy::expect_used,
    reason = "test fixtures fail loudly on setup errors"
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use prost::Message;
use tokio::time::timeout;

use crate::bindings::{InputBinding, ModuleBindings, ModuleReport, OutputBinding};
use crate::engine::{Engine, Key, Local, Presence, QueryParams};
use crate::{Cloudy, Descriptor, Qos, RuntimeOptions, Topic};

mod bridge;
mod lifecycle;
mod ports;
mod startup;

const PATIENCE: Duration = Duration::from_secs(5);
const DOMAIN: &str = "managed";
const SENDER: &str = "deployment/forwarded/device";
const RECEIVER: &str = "deployment/consumer";

#[derive(Clone, PartialEq, Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    value: u32,
}

impl Topic for Probe {
    const TYPE: &'static str = "Probe";
    const SCHEMA: Option<u64> = Some(0x1234);
    const DESCRIPTOR: Option<Descriptor> = Some(Descriptor {
        message: "test.Probe",
        file_set: b"managed-test-descriptor",
    });
}

#[derive(Clone, PartialEq, Message)]
struct Bare {
    #[prost(uint32, tag = "1")]
    value: u32,
}

impl Topic for Bare {
    const TYPE: &'static str = "Probe";
}

fn bindings(namespace: &str) -> ModuleBindings {
    ModuleBindings {
        version: 1,
        namespace: namespace.to_string(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
    }
}

fn input(source: &str) -> InputBinding {
    InputBinding {
        type_name: "test.Probe".to_string(),
        source: source.to_string(),
    }
}

fn options(bus: Arc<dyn Engine>, bindings: ModuleBindings) -> RuntimeOptions {
    let mut options = RuntimeOptions::new(&bindings.namespace);
    options.domain = DOMAIN.to_string();
    options.engine = Some(bus);
    options.module_bindings = Some(bindings);
    options.module_report_path = None;
    options
}

async fn consumer(bus: Arc<dyn Engine>) -> Cloudy {
    let mut contract = bindings(RECEIVER);
    contract
        .inputs
        .insert("incoming".to_string(), input(SENDER));
    Cloudy::open(options(bus, contract))
        .await
        .expect("consumer")
}

async fn producer(bus: Arc<dyn Engine>, namespace: &str) -> Cloudy {
    let mut contract = bindings(namespace);
    contract.outputs.insert(
        "outgoing".to_string(),
        OutputBinding {
            type_name: "test.Probe".to_string(),
        },
    );
    Cloudy::open(options(bus, contract))
        .await
        .expect("producer")
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "reiny-managed-{}-{}-{}",
            std::process::id(),
            crate::engine::now_unix_ns().expect("clock"),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("isolated directory");
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove isolated fixture");
    }
}
