//! Two registered components exchange one shared message through resolved named ports.
//! Run with `cargo run -p reiny --no-default-features --example component_host`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use reiny::bindings::{
    EndpointAddress, EndpointContract, InputBinding, MODULE_BINDINGS_VERSION, ModuleBindings,
    OutputBinding,
};
use reiny::component::{
    Component, ComponentContext, ComponentFactory, ComponentFuture, ComponentHost,
    ComponentInstanceSpec, ComponentRegistry, HostOptions, HostOutcome, StopReason,
};
use reiny::{MessageType, Publisher, Subscriber};
use tokio::sync::mpsc;

#[derive(Clone, PartialEq, prost::Message)]
struct Greeting {
    #[prost(string, tag = "1")]
    text: String,
}

impl MessageType for Greeting {
    const TYPE_NAME: &'static str = "example.Greeting";
    const SCHEMA_FINGERPRINT: u64 = 0xc04d;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

struct SourceFactory {
    greeting: Arc<Greeting>,
}

struct Source {
    greeting: Arc<Greeting>,
    output: Option<Publisher<Greeting>>,
}

impl ComponentFactory for SourceFactory {
    fn create(&self, _: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        Ok(Box::new(Source {
            greeting: self.greeting.clone(),
            output: None,
        }))
    }
}

impl Component for Source {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.output = Some(ctx.output("greeting")?);
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            let output = self
                .output
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("output was not prepared"))?;
            output.send_shared(self.greeting.clone()).await?;
            std::future::pending().await
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

struct SinkFactory {
    received: mpsc::Sender<reiny::Envelope<Arc<Greeting>>>,
}

struct Sink {
    received: mpsc::Sender<reiny::Envelope<Arc<Greeting>>>,
    input: Option<Subscriber<Greeting>>,
}

impl ComponentFactory for SinkFactory {
    fn create(&self, _: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        Ok(Box::new(Sink {
            received: self.received.clone(),
            input: None,
        }))
    }
}

impl Component for Sink {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.input = Some(ctx.input("incoming")?);
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            let input = self
                .input
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("input was not prepared"))?;
            let greeting = input
                .recv_shared_envelope()
                .await
                .ok_or_else(|| anyhow::anyhow!("input closed before greeting"))?;
            self.received.send(greeting).await?;
            std::future::pending().await
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
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

#[tokio::main]
async fn main() -> reiny::Result<()> {
    let greeting = Arc::new(Greeting {
        text: "hello from one shared Local".to_owned(),
    });
    let (received, mut messages) = mpsc::channel(1);
    let mut registry = ComponentRegistry::new();
    registry.register(
        "source".to_owned(),
        Arc::new(SourceFactory {
            greeting: greeting.clone(),
        }),
    )?;
    registry.register("sink".to_owned(), Arc::new(SinkFactory { received }))?;
    let contract = EndpointContract {
        type_name: Greeting::TYPE_NAME.to_owned(),
        ..EndpointContract::default()
    };
    let mut source = bindings("demo/source");
    source.outputs.insert(
        "greeting".to_owned(),
        OutputBinding {
            contract: contract.clone(),
        },
    );
    let mut sink = bindings("demo/sink");
    sink.inputs.insert(
        "incoming".to_owned(),
        InputBinding {
            contract,
            sources: vec![EndpointAddress {
                namespace: "demo/source".to_owned(),
                port: "greeting".to_owned(),
            }],
        },
    );
    let host = ComponentHost::launch(
        Arc::new(registry),
        HostOptions {
            domain: "component-demo".to_owned(),
            engine: Arc::new(reiny::engine::Local::new()),
            startup_timeout: Duration::from_secs(5),
            stop_timeout: Duration::from_secs(5),
        },
        vec![
            ComponentInstanceSpec {
                factory_id: "source".to_owned(),
                bindings: source,
                config: None,
            },
            ComponentInstanceSpec {
                factory_id: "sink".to_owned(),
                bindings: sink,
                config: None,
            },
        ],
    )?;
    host.wait_running().await?;
    let received = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("sink ended without a message"))?;
    anyhow::ensure!(
        Arc::ptr_eq(&received.value, &greeting),
        "Local did not preserve the shared value"
    );
    println!(
        "{} from {}/{}",
        received.value.text, received.source, received.source_port
    );
    match host.stop().await {
        HostOutcome::Stopped => Ok(()),
        outcome @ (HostOutcome::Failed { .. } | HostOutcome::Unresponsive { .. }) => {
            anyhow::bail!("host shutdown failed: {outcome:?}")
        }
    }
}
