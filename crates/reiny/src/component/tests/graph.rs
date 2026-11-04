use super::*;
use crate::bindings::{
    EndpointAddress, EndpointContract, InputBinding, OutputBinding, ReportErrorCode,
};
use crate::engine::{Key, READY_CHUNK};
use crate::{Envelope, MessageType, Publisher, Subscriber};

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Probe {
    #[prost(uint32, tag = "1")]
    value: u32,
}

impl MessageType for Probe {
    const TYPE_NAME: &'static str = "host.Probe";
    const SCHEMA_FINGERPRINT: u64 = 123;
    const DESCRIPTOR: Option<crate::Descriptor> = None;
}

#[derive(Clone, PartialEq, prost::Message)]
struct WrongSchema {
    #[prost(uint32, tag = "1")]
    value: u32,
}

impl MessageType for WrongSchema {
    const TYPE_NAME: &'static str = Probe::TYPE_NAME;
    const SCHEMA_FINGERPRINT: u64 = 456;
    const DESCRIPTOR: Option<crate::Descriptor> = None;
}

#[derive(Clone, Copy)]
enum Role {
    Source,
    Sink,
    WrongSchema,
    MissingOutput,
    LoseOutput,
}

struct PipeFactory {
    role: Role,
    payload: Arc<Probe>,
    deliveries: mpsc::Sender<Envelope<Arc<Probe>>>,
    records: mpsc::Sender<Record>,
    gate: watch::Receiver<bool>,
}

struct Pipe {
    factory: PipeFactory,
    output: Option<Publisher<Probe>>,
    unrelated: Option<Publisher<Probe>>,
    wrong: Option<Publisher<WrongSchema>>,
    input: Option<Subscriber<Probe>>,
    config: Option<toml::Table>,
}

impl ComponentFactory for PipeFactory {
    fn create(&self, config: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        Ok(Box::new(Pipe {
            factory: Self {
                role: self.role,
                payload: self.payload.clone(),
                deliveries: self.deliveries.clone(),
                records: self.records.clone(),
                gate: self.gate.clone(),
            },
            output: None,
            unrelated: None,
            wrong: None,
            input: None,
            config: config.cloned(),
        }))
    }
}

impl Component for Pipe {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            anyhow::ensure!(
                self.config.as_ref() == ctx.config_table(),
                "factory/context config mismatch"
            );
            match self.factory.role {
                Role::Source | Role::LoseOutput => {
                    self.output = Some(ctx.output("out")?);
                    self.unrelated = Some(ctx.output("unrelated")?);
                }
                Role::Sink => self.input = Some(ctx.input("in")?),
                Role::WrongSchema => {
                    self.wrong = Some(ctx.output("out")?);
                    self.unrelated = Some(ctx.output("unrelated")?);
                }
                Role::MissingOutput => {}
            }
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.factory
                .records
                .send(Record {
                    id: ctx.id().to_owned(),
                    stage: Stage::Run,
                })
                .await?;
            match self.factory.role {
                Role::Source => {
                    self.unrelated
                        .as_ref()
                        .expect("prepared unrelated")
                        .send_shared(Arc::new(Probe { value: 999 }))
                        .await?;
                    self.output
                        .as_ref()
                        .expect("prepared output")
                        .send_shared(self.factory.payload.clone())
                        .await?;
                }
                Role::Sink => {
                    let envelope = self
                        .input
                        .as_mut()
                        .expect("prepared input")
                        .recv_shared_envelope()
                        .await
                        .expect("live delivery");
                    self.factory.deliveries.send(envelope).await?;
                }
                Role::LoseOutput => {
                    self.factory.gate.wait_for(|open| *open).await?;
                    self.output.take();
                }
                Role::WrongSchema | Role::MissingOutput => {}
            }
            std::future::pending().await
        })
    }

    fn stop<'a>(
        &'a mut self,
        ctx: &'a ComponentContext,
        reason: StopReason,
    ) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.factory
                .records
                .send(Record {
                    id: ctx.id().to_owned(),
                    stage: Stage::Stop(reason),
                })
                .await?;
            Ok(())
        })
    }
}

struct Graph {
    registry: Arc<ComponentRegistry>,
    instances: Vec<ComponentInstanceSpec>,
    payload: Arc<Probe>,
    deliveries: mpsc::Receiver<Envelope<Arc<Probe>>>,
    records: mpsc::Receiver<Record>,
    gate: watch::Sender<bool>,
}

fn graph(source: Role) -> Graph {
    let payload = Arc::new(Probe { value: 42 });
    let (deliveries, delivery_rx) = mpsc::channel(4);
    let (records, record_rx) = mpsc::channel(16);
    let (gate, gate_rx) = watch::channel(false);
    let mut registry = ComponentRegistry::new();
    for (name, role) in [("source", source), ("sink", Role::Sink)] {
        registry
            .register(
                name.to_owned(),
                Arc::new(PipeFactory {
                    role,
                    payload: payload.clone(),
                    deliveries: deliveries.clone(),
                    records: records.clone(),
                    gate: gate_rx.clone(),
                }),
            )
            .expect("register pipe");
    }
    let contract = EndpointContract {
        type_name: Probe::TYPE_NAME.to_owned(),
        ..EndpointContract::default()
    };
    let mut source = instance("app/source", "source");
    for name in ["out", "unrelated"] {
        source.bindings.outputs.insert(
            name.to_owned(),
            OutputBinding {
                contract: contract.clone(),
            },
        );
    }
    let mut sink = instance("app/sink", "sink");
    sink.bindings.inputs.insert(
        "in".to_owned(),
        InputBinding {
            contract,
            sources: vec![EndpointAddress {
                namespace: "app/source".to_owned(),
                port: "out".to_owned(),
            }],
        },
    );
    source.config = Some(toml::Table::from_iter([(
        "value".to_owned(),
        toml::Value::Integer(42),
    )]));
    Graph {
        registry: Arc::new(registry),
        instances: vec![source, sink],
        payload,
        deliveries: delivery_rx,
        records: record_rx,
        gate,
    }
}

mod cases;
mod staged;
