use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use prost::Message;
use reiny::bindings::{
    EndpointAddress, EndpointContract, InputBinding, MODULE_BINDINGS_VERSION, ModuleBindings,
    OutputBinding, PortKind,
};
use reiny::{MessageType, ModuleContext, RuntimeOptions, Service};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, PartialEq, Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    value: u32,
}

impl MessageType for Value {
    const TYPE_NAME: &'static str = "named.Value";
    const SCHEMA_FINGERPRINT: u64 = 0x7e91_8bc2_319a_4021;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

#[derive(Clone, PartialEq, Message)]
struct Request {
    #[prost(uint32, tag = "1")]
    value: u32,
}

impl MessageType for Request {
    const TYPE_NAME: &'static str = "named.Request";
    const SCHEMA_FINGERPRINT: u64 = 0xf571_069c_a987_6534;
    const DESCRIPTOR: Option<reiny::Descriptor> = None;
}

impl Service for Request {
    type Response = Value;
}

fn local_bindings() -> ModuleBindings {
    let namespace = "named-ports/proof";
    let mut binding = ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: namespace.into(),
        endpoint_namespace: namespace.into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    };
    for (input, output, rpc) in [
        ("left", "first", false),
        ("right", "second", false),
        ("call_first", "rpc_first", true),
        ("call_second", "rpc_second", true),
    ] {
        let contract = EndpointContract {
            type_name: if rpc {
                Request::TYPE_NAME
            } else {
                Value::TYPE_NAME
            }
            .into(),
            kind: if rpc { PortKind::Rpc } else { PortKind::Stream },
            response: rpc.then(|| Value::TYPE_NAME.into()),
            ..EndpointContract::default()
        };
        binding.inputs.insert(
            input.into(),
            InputBinding {
                contract: contract.clone(),
                sources: vec![EndpointAddress {
                    namespace: namespace.into(),
                    port: output.into(),
                }],
            },
        );
        binding
            .outputs
            .insert(output.into(), OutputBinding { contract });
    }
    binding
}

async fn prove(module: &ModuleContext) -> Result<()> {
    let first = module.output::<Value>("first")?;
    let second = module.output::<Value>("second")?;
    let mut left = module.input::<Value>("left")?;
    let mut right = module.raw_input("right", Value::TYPE_NAME, Value::SCHEMA_FINGERPRINT)?;
    let mut server_first = module.rpc_server::<Request>("rpc_first")?;
    let mut server_second = module.rpc_server::<Request>("rpc_second")?;
    let call_first = module.rpc_client::<Request>("call_first")?;
    let call_second = module.rpc_client::<Request>("call_second")?;
    module.ready()?;

    // Both subscribers exist before either publication. Reversed send order exposes cross-talk.
    second.send(Value { value: 22 }).await?;
    first.send(Value { value: 11 }).await?;
    let a = timeout(DEADLINE, left.recv_envelope())
        .await?
        .context("first stream ended")?;
    let b = timeout(DEADLINE, right.recv())
        .await?
        .context("second stream ended")?;
    ensure!(
        a.value.value == 11 && a.source_port == "first",
        "first stream crossed ports"
    );
    ensure!(
        Value::decode(b.payload.as_slice())?.value == 22 && b.source_port == "second",
        "second stream crossed ports"
    );
    ensure!(
        a.source == b.source && !a.source.is_empty(),
        "source namespace was lost"
    );
    ensure!(
        left.stats().received == 1 && right.stats().received == 1,
        "stream duplication"
    );

    // Join polls both servers and clients together; no startup delay or timing-based assertion.
    let serve_first = async {
        let request = server_first.recv().await.context("first server ended")?;
        ensure!(request.value.value == 3, "first RPC crossed ports");
        request.reply(Value { value: 103 }).await
    };
    let serve_second = async {
        let request = server_second.recv().await.context("second server ended")?;
        ensure!(request.value.value == 7, "second RPC crossed ports");
        request.reply(Value { value: 207 }).await
    };
    let (a, b, served_a, served_b) = timeout(DEADLINE, async {
        tokio::join!(
            call_first.call(Request { value: 3 }),
            call_second.call(Request { value: 7 }),
            serve_first,
            serve_second,
        )
    })
    .await?;
    served_a?;
    served_b?;
    ensure!(
        a?.value == 103 && b?.value == 207,
        "RPC responses crossed ports"
    );
    println!("NAMED_STREAM_RPC_OK");
    if !module
        .extra_args()
        .iter()
        .any(|argument| argument == "--local")
    {
        module.shutdown().await;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let local = std::env::args().any(|argument| argument == "--local");
    let mut options = if local {
        let binding = local_bindings();
        let mut options = RuntimeOptions::new(&binding.namespace);
        options.engine = Some(Arc::new(reiny::engine::Local::new()));
        options.module_bindings = Some(binding);
        options.module_report_path = None;
        options.extra_args = vec!["--local".into()];
        options
    } else {
        RuntimeOptions::from_args("named-ports")
    };
    options.install_tracing = false;
    let module = ModuleContext::open(options).await?;
    prove(&module).await
}
