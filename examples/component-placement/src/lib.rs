//! Application-owned factories shared by the two process-host executables.

pub mod local;

use std::sync::Arc;

use reiny::component::process::protocol::{FactoryRegistration, HostOutcome};
use reiny::component::process::{Bootstrap, ProcessHost, ProcessLimits, ProcessRegistry};
use reiny::component::{
    Component, ComponentContext, ComponentFactory, ComponentFuture, StopReason,
};
use reiny::{MessageType, Publisher, Subscriber};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// The example's explicitly fingerprinted protobuf payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Tick {
    /// Sequence carried unchanged through both named routes.
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
}

impl MessageType for Tick {
    const TYPE_NAME: &'static str = "placement.Tick";
    const SCHEMA_FINGERPRINT: u64 = 0x706c_6163_656d_656e;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

// Deliberately incompatible compiled schema used by the CLI admission regression.
#[derive(Clone, PartialEq, prost::Message)]
struct WrongTick {
    #[prost(string, tag = "1")]
    sequence: String,
}

impl MessageType for WrongTick {
    const TYPE_NAME: &'static str = Tick::TYPE_NAME;
    const SCHEMA_FINGERPRINT: u64 = Tick::SCHEMA_FINGERPRINT + 1;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

struct Factory {
    role: &'static str,
}

impl ComponentFactory for Factory {
    fn create(&self, config: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        let events = config
            .and_then(|table| table.get("events"))
            .and_then(toml::Value::as_str)
            .map(str::to_owned);
        let wrong_type = config
            .and_then(|table| table.get("wrong_type"))
            .and_then(toml::Value::as_bool)
            .unwrap_or(false);
        let run_marker = config
            .and_then(|table| table.get("run_marker"))
            .and_then(toml::Value::as_str)
            .map(std::path::PathBuf::from);
        let prepare_gate = config
            .and_then(|table| table.get("prepare_gate"))
            .and_then(toml::Value::as_bool)
            .unwrap_or(false);
        Ok(Box::new(Endpoint {
            role: self.role,
            events,
            stream: None,
            wrong_type,
            run_marker,
            prepare_gate,
            incompatible: None,
            input: None,
            output: None,
        }))
    }
}

struct Endpoint {
    role: &'static str,
    events: Option<String>,
    stream: Option<BufReader<TcpStream>>,
    wrong_type: bool,
    run_marker: Option<std::path::PathBuf>,
    prepare_gate: bool,
    incompatible: Option<Publisher<WrongTick>>,
    input: Option<Subscriber<Tick>>,
    output: Option<Publisher<Tick>>,
}

impl Endpoint {
    async fn event(&mut self, mut value: serde_json::Value) -> reiny::Result<()> {
        value["role"] = self.role.into();
        value["pid"] = std::process::id().into();
        if let Some(stream) = &mut self.stream {
            let mut bytes = serde_json::to_vec(&value)?;
            bytes.push(b'\n');
            stream.get_mut().write_all(&bytes).await?;
        } else {
            println!("{value}");
        }
        Ok(())
    }

    async fn commands(&mut self) -> reiny::Result<()> {
        let Some(stream) = &mut self.stream else {
            std::future::pending::<()>().await;
            return Ok(());
        };
        let mut command = String::new();
        loop {
            command.clear();
            if stream.read_line(&mut command).await? == 0 {
                return Err(std::io::Error::other("fixture event controller disconnected").into());
            }
            if command.trim() == "exit" {
                // Exercise a real abrupt process exit, not a component outcome.
                std::process::exit(23);
            }
        }
    }
}

impl Component for Endpoint {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            let (input, output) = if self.role == "sender" {
                ("acknowledgement", "data")
            } else {
                ("data", "acknowledgement")
            };
            self.input = Some(ctx.input(input)?);
            if self.wrong_type {
                self.incompatible = Some(ctx.output(output)?);
            } else {
                self.output = Some(ctx.output(output)?);
            }
            if let Some(address) = &self.events {
                self.stream = Some(BufReader::new(TcpStream::connect(address).await?));
            }
            self.event(json!({"event": "prepared"})).await?;
            if self.prepare_gate {
                let stream = self.stream.as_mut().ok_or_else(|| {
                    std::io::Error::other("prepare_gate requires an event controller")
                })?;
                let mut command = String::new();
                stream.read_line(&mut command).await?;
                if command.trim() != "prepare" {
                    return Err(std::io::Error::other("prepare gate was not released").into());
                }
            }
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            if let Some(path) = &self.run_marker {
                std::fs::write(path, b"run\n")?;
            }
            self.event(json!({"event": "run"})).await?;
            let output = self.output.as_ref().ok_or_else(|| {
                std::io::Error::other("incompatible fixture was incorrectly admitted")
            })?;
            if self.role == "sender" {
                output.send(Tick { sequence: 42 }).await?;
            }
            let input = self
                .input
                .as_mut()
                .ok_or_else(|| std::io::Error::other("missing prepared input"))?;
            let envelope = tokio::select! {
                () = ctx.shutdown() => return Ok(()),
                value = input.recv_envelope() => value.ok_or_else(|| {
                    std::io::Error::other("named input closed before delivery")
                })?,
            };
            if self.role == "receiver" {
                output.send(envelope.value.clone()).await?;
            }
            self.event(json!({
                "event": "delivered",
                "sequence": envelope.value.sequence,
                "source": envelope.source,
                "source_port": envelope.source_port
            }))
            .await?;
            tokio::select! {
                () = ctx.shutdown() => Ok(()),
                result = self.commands() => result,
            }
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.input = None;
            self.output = None;
            self.incompatible = None;
            self.event(json!({"event": "stop"})).await
        })
    }
}

/// Serve a private owner bootstrap with one revisioned application factory.
pub fn serve(role: &'static str) -> reiny::Result<()> {
    let bootstrap = Bootstrap::read(&mut std::io::stdin().lock())?;
    let mut registry = ProcessRegistry::new();
    registry.register(
        FactoryRegistration {
            factory: role.into(),
            revision: "1".into(),
        },
        Arc::new(Factory { role }),
    )?;
    match ProcessHost::run(bootstrap, registry, ProcessLimits::default())? {
        HostOutcome::Stopped {} => Ok(()),
        HostOutcome::Failed { .. } | HostOutcome::Unresponsive { .. } => std::process::exit(1),
    }
}
