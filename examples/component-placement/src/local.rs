//! Two public SDK components proving native shared delivery in one process host.
//! The payload is immutable; only its serialization instrumentation is atomic.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use prost::Message;
use reiny::component::process::protocol::{FactoryRegistration, HostOutcome};
use reiny::component::process::{Bootstrap, ProcessHost, ProcessLimits, ProcessRegistry};
use reiny::component::{
    Component, ComponentContext, ComponentFactory, ComponentFuture, StopReason,
};
use reiny::{MessageType, Publisher, Subscriber};
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug, Default)]
struct Counted {
    wire: super::Tick,
    encodes: AtomicUsize,
}

impl Message for Counted {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        self.encodes.fetch_add(1, Ordering::SeqCst);
        self.wire.encode_raw(buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        self.wire.merge_field(tag, wire_type, buf, ctx)
    }

    fn encoded_len(&self) -> usize {
        self.wire.encoded_len()
    }

    fn clear(&mut self) {
        self.wire.clear();
    }
}

impl MessageType for Counted {
    const TYPE_NAME: &'static str = super::Tick::TYPE_NAME;
    const SCHEMA_FINGERPRINT: u64 = super::Tick::SCHEMA_FINGERPRINT;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

struct Factory {
    role: &'static str,
    value: Arc<Counted>,
}

impl ComponentFactory for Factory {
    fn create(&self, config: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        let address = config
            .and_then(|config| config.get("events"))
            .and_then(toml::Value::as_str)
            .ok_or_else(|| std::io::Error::other("events address required"))?;
        Ok(Box::new(Endpoint {
            role: self.role,
            value: Arc::clone(&self.value),
            address: address.to_owned(),
            events: None,
            input: None,
            output: None,
        }))
    }
}

struct Endpoint {
    role: &'static str,
    value: Arc<Counted>,
    address: String,
    events: Option<TcpStream>,
    input: Option<Subscriber<Counted>>,
    output: Option<Publisher<Counted>>,
}

impl Endpoint {
    async fn event(&mut self, mut event: serde_json::Value) -> reiny::Result<()> {
        event["role"] = self.role.into();
        event["pid"] = std::process::id().into();
        event["encodes"] = self.value.encodes.load(Ordering::SeqCst).into();
        let stream = self
            .events
            .as_mut()
            .ok_or_else(|| std::io::Error::other("event stream not connected"))?;
        let mut bytes = serde_json::to_vec(&event)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        Ok(())
    }
}

impl Component for Endpoint {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            let (input, output) = match self.role {
                "sender" => ("acknowledgement", "data"),
                "receiver" => ("data", "acknowledgement"),
                _ => return Err(std::io::Error::other("unknown role").into()),
            };
            self.input = Some(ctx.input(input)?);
            self.output = Some(ctx.output(output)?);
            self.events = Some(TcpStream::connect(&self.address).await?);
            self.event(json!({"event": "prepared"})).await
        })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.event(json!({"event": "run"})).await?;
            let output = self
                .output
                .as_ref()
                .ok_or_else(|| std::io::Error::other("missing output"))?;
            if self.role == "sender" {
                output.send_shared(Arc::clone(&self.value)).await?;
            }
            let input = self
                .input
                .as_mut()
                .ok_or_else(|| std::io::Error::other("missing input"))?;
            let envelope = tokio::select! {
                () = ctx.shutdown() => return Ok(()),
                value = input.recv_shared_envelope() => value
                    .ok_or_else(|| std::io::Error::other("input closed before shared delivery"))?,
            };
            let native = Arc::ptr_eq(&envelope.value, &self.value);
            if self.role == "receiver" {
                output.send_shared(Arc::clone(&envelope.value)).await?;
            }
            self.event(json!({
                "event": "delivered", "native": native,
                "sequence": envelope.value.wire.sequence,
                "source": envelope.source, "source_port": envelope.source_port,
            }))
            .await?;
            ctx.shutdown().await;
            Ok(())
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.input = None;
            self.output = None;
            self.event(json!({"event": "stop"})).await
        })
    }
}

/// Serve both factories on one actual ProcessHost using owner-selected transport.
pub fn serve() -> reiny::Result<()> {
    // A positive control checks that the counter observes real protobuf encoding.
    let control = Counted::default();
    let _bytes = control.encode_to_vec();
    if control.encodes.load(Ordering::SeqCst) != 1 {
        return Err(std::io::Error::other("serialization instrumentation failed").into());
    }
    let value = Arc::new(Counted {
        wire: super::Tick { sequence: 42 },
        encodes: AtomicUsize::new(0),
    });
    let bootstrap = Bootstrap::read(&mut std::io::stdin().lock())?;
    let mut registry = ProcessRegistry::new();
    for role in ["sender", "receiver"] {
        registry.register(
            FactoryRegistration {
                factory: role.into(),
                revision: "1".into(),
            },
            Arc::new(Factory {
                role,
                value: Arc::clone(&value),
            }),
        )?;
    }
    match ProcessHost::run(bootstrap, registry, ProcessLimits::default())? {
        HostOutcome::Stopped {} => Ok(()),
        HostOutcome::Failed { .. } | HostOutcome::Unresponsive { .. } => std::process::exit(1),
    }
}
