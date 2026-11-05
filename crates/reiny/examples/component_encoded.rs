//! Encoded component ports use the same closed graph as native Rust ports.
//! Run with `cargo run -p reiny --no-default-features --example component_encoded`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use reiny::bindings::{
    EndpointAddress, EndpointContract, InputBinding, MODULE_BINDINGS_VERSION, ModuleBindings,
    OutputBinding, PortKind,
};
use reiny::component::{
    Component, ComponentContext, ComponentFactory, ComponentFuture, ComponentHost,
    ComponentInstanceSpec, ComponentRegistry, HostOptions, HostOutcome, StopReason,
};
use reiny::encoded::EncodedType;
use reiny::{DeclaredPublisher, DeclaredRpcClient, DeclaredRpcServer, MessageType, RawSubscriber};
use tokio::sync::mpsc;

#[derive(Clone, PartialEq, Message)]
struct Value {
    #[prost(string, tag = "1")]
    text: String,
}

impl MessageType for Value {
    const TYPE_NAME: &'static str = "example.Value";
    const SCHEMA_FINGERPRINT: u64 = 0x1234;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

struct Factory {
    server: bool,
    completed: mpsc::Sender<String>,
}

struct Instance {
    server: bool,
    completed: mpsc::Sender<String>,
    output: Option<DeclaredPublisher>,
    input: Option<RawSubscriber>,
    service: Option<DeclaredRpcServer>,
    client: Option<DeclaredRpcClient>,
}

impl ComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        Ok(Box::new(Instance {
            server: self.server,
            completed: self.completed.clone(),
            output: None,
            input: None,
            service: None,
            client: None,
        }))
    }
}

impl Component for Instance {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            let schema = EncodedType::of::<Value>()?;
            if self.server {
                self.output = Some(ctx.raw_output("greeting", &schema)?);
                self.service = Some(ctx.raw_rpc_server("echo", &schema, &schema)?);
            } else {
                self.input = Some(ctx.raw_input("greeting", &schema)?);
                self.client = Some(ctx.raw_rpc_client("echo", &schema, &schema)?);
            }
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            if self.server {
                self.output
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing output"))?
                    .send(
                        Value {
                            text: "hello\0encoded".into(),
                        }
                        .encode_to_vec(),
                    )?;
                let request = self
                    .service
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("missing server"))?
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("server closed"))?;
                let payload = request.payload().to_owned();
                request.reply(payload).await?;
            } else {
                let received = self
                    .input
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("missing input"))?
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("input closed"))?;
                anyhow::ensure!(
                    received.source == "encoded/server" && received.source_port == "greeting",
                    "incorrect stream provenance",
                );
                let reply = self
                    .client
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing client"))?
                    .call_with_timeout(received.payload, Duration::from_secs(5))
                    .await?;
                self.completed
                    .send(Value::decode(reply.as_slice())?.text)
                    .await?;
            }
            std::future::pending().await
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

fn instance(server: bool) -> ComponentInstanceSpec {
    let id = if server { "server" } else { "client" };
    let mut bindings = ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: format!("encoded/{id}"),
        endpoint_namespace: format!("encoded/{id}"),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    };
    for (name, kind) in [("greeting", PortKind::Stream), ("echo", PortKind::Rpc)] {
        let contract = EndpointContract {
            type_name: Value::TYPE_NAME.into(),
            kind,
            response: (kind == PortKind::Rpc).then(|| Value::TYPE_NAME.into()),
            ..EndpointContract::default()
        };
        if server {
            bindings
                .outputs
                .insert(name.into(), OutputBinding { contract });
        } else {
            bindings.inputs.insert(
                name.into(),
                InputBinding {
                    contract,
                    sources: vec![EndpointAddress {
                        namespace: "encoded/server".into(),
                        port: name.into(),
                    }],
                },
            );
        }
    }
    ComponentInstanceSpec {
        factory_id: id.into(),
        bindings,
        config: None,
        config_origin: None,
    }
}

#[tokio::main]
async fn main() -> reiny::Result<()> {
    let (completed, mut result) = mpsc::channel(1);
    let mut registry = ComponentRegistry::new();
    for (name, server) in [("server", true), ("client", false)] {
        registry.register(
            name.into(),
            Arc::new(Factory {
                server,
                completed: completed.clone(),
            }),
        )?;
    }
    let host = ComponentHost::launch(
        Arc::new(registry),
        HostOptions {
            domain: "encoded-example".into(),
            engine: Arc::new(reiny::engine::Local::new()),
            startup_timeout: Duration::from_secs(5),
            stop_timeout: Duration::from_secs(5),
        },
        vec![instance(true), instance(false)],
    )?;
    host.wait_running().await?;
    let received = tokio::time::timeout(Duration::from_secs(5), result.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("component closed without reply"))?;
    anyhow::ensure!(received == "hello\0encoded", "payload changed");
    anyhow::ensure!(
        matches!(host.stop().await, HostOutcome::Stopped),
        "host did not stop"
    );
    println!("Encoded stream and RPC round trip preserved bytes and declared provenance.");
    Ok(())
}
