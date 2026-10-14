use super::*;
use crate::bindings::PortKind;
use crate::{CallError, Service};

#[derive(Clone, PartialEq, Message)]
pub(super) struct RequestProbe {
    #[prost(uint32, tag = "1")]
    value: u32,
}

impl MessageType for RequestProbe {
    const TYPE_NAME: &'static str = "test.RequestProbe";
    const SCHEMA_FINGERPRINT: u64 = 0xabcd;
    const DESCRIPTOR: Option<Descriptor> = Some(Descriptor {
        message: "test.RequestProbe",
        file_set: b"request-descriptor",
    });
}

impl Service for RequestProbe {
    type Response = Probe;
}

fn rpc_contract() -> EndpointContract {
    EndpointContract {
        kind: PortKind::Rpc,
        response: Some("test.Probe".into()),
        ..contract("test.RequestProbe")
    }
}

async fn client(bus: Arc<dyn Engine>) -> ModuleContext {
    let mut binding = bindings(RECEIVER);
    binding.inputs.insert(
        "request".into(),
        InputBinding {
            contract: rpc_contract(),
            sources: vec![SENDER.into()],
        },
    );
    ModuleContext::open(options(bus, binding))
        .await
        .expect("client")
}

async fn service(bus: Arc<dyn Engine>) -> ModuleContext {
    let mut binding = bindings(SENDER);
    binding.outputs.insert(
        "request".into(),
        OutputBinding {
            contract: rpc_contract(),
        },
    );
    ModuleContext::open(options(bus, binding))
        .await
        .expect("service")
}

async fn standalone_node(bus: Arc<dyn Engine>, namespace: &str) -> ModuleContext {
    // Internal transport regression fixture; application startup cannot construct this context.
    ModuleContext::new(
        bus,
        namespace.to_string(),
        DOMAIN.to_string(),
        crate::shutdown::Shutdown::new(),
        None,
        Vec::new(),
    )
    .await
    .expect("transport fixture")
}

#[tokio::test]
async fn named_rpc_uses_exact_target_and_reports_both_compiled_schemas() {
    let bus = Arc::new(Local::new());
    let service = service(bus.clone()).await;
    let client = client(bus.clone()).await;
    let mut server = service
        .rpc_server::<RequestProbe>("request")
        .expect("server");
    let caller = client
        .rpc_client::<RequestProbe>("request")
        .expect("caller");
    let others = Arc::new(AtomicU64::new(0));
    let count = others.clone();
    let other_key = Key::topic(DOMAIN, Some("deployment/other"), RequestProbe::TYPE_NAME);
    let reply_key = other_key.clone();
    let _other = bus
        .respond(
            &other_key,
            Box::new(move |query| {
                count.fetch_add(1, Ordering::SeqCst);
                query
                    .reply(
                        &reply_key,
                        Probe { value: 99 }.encode_to_vec(),
                        crate::pubsub::fingerprint(Some(Probe::SCHEMA_FINGERPRINT)),
                    )
                    .expect("other reply");
            }),
        )
        .expect("other server");
    service.ready().expect("server ready");
    client.ready().expect("caller ready");
    {
        let ports = client.module.ports.lock().expect("ports");
        let report = &ports.inputs["request"].contract;
        assert_eq!(
            (report.type_name.as_str(), report.schema),
            ("test.RequestProbe", 0xabcd)
        );
        let response = report.response.as_ref().expect("response contract");
        assert_eq!(
            (response.type_name.as_str(), response.schema),
            ("test.Probe", 0x1234)
        );
        assert_eq!(report.contract, rpc_contract());
    }
    let task = tokio::spawn(async move {
        let request = server.recv().await.expect("request");
        request.reply(Probe { value: 42 }).await.expect("reply");
    });
    let response = timeout(PATIENCE, caller.call(RequestProbe { value: 1 }))
        .await
        .expect("bounded call")
        .expect("reply");
    assert_eq!(response.value, 42);
    timeout(PATIENCE, task)
        .await
        .expect("server finished")
        .expect("server task");
    assert_eq!(others.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn server_rejects_unstamped_malformed_and_mismatching_requests() {
    for managed in [false, true] {
        let bus = Arc::new(Local::new());
        let service = if managed {
            service(bus.clone()).await
        } else {
            standalone_node(bus.clone(), SENDER).await
        };
        let mut server = if managed {
            service.rpc_server::<RequestProbe>("request")
        } else {
            service.serve::<RequestProbe>()
        }
        .expect("server");
        let task = tokio::spawn(async move {
            let request = server.recv().await.expect("verified request");
            assert_eq!(request.value.value, 2);
            request.reply(Probe { value: 2 }).await.expect("reply");
        });
        let key = Key::topic(DOMAIN, Some(SENDER), RequestProbe::TYPE_NAME);
        for attachment in [
            None,
            Some(vec![1]),
            crate::pubsub::fingerprint(Some(0xffff)),
        ] {
            let mut replies = bus
                .query(
                    &key,
                    QueryParams {
                        payload: Some(RequestProbe { value: 1 }.encode_to_vec()),
                        attachment,
                        timeout: PATIENCE,
                    },
                )
                .expect("raw request");
            let response = timeout(PATIENCE, replies.next())
                .await
                .expect("bounded rejection")
                .expect("reply");
            assert!(response.is_err());
        }
        let client = if managed {
            client(bus).await
        } else {
            standalone_node(bus, RECEIVER).await
        };
        let caller = if managed {
            client.rpc_client::<RequestProbe>("request")
        } else {
            client.caller::<RequestProbe>().to(SENDER).build()
        }
        .expect("caller");
        assert_eq!(
            timeout(PATIENCE, caller.call(RequestProbe { value: 2 }))
                .await
                .expect("call")
                .expect("reply")
                .value,
            2
        );
        timeout(PATIENCE, task)
            .await
            .expect("server finished")
            .expect("server task");
    }
}

#[tokio::test]
async fn caller_rejects_unstamped_malformed_and_mismatching_responses() {
    for managed in [false, true] {
        let bus = Arc::new(Local::new());
        let client = if managed {
            client(bus.clone()).await
        } else {
            standalone_node(bus.clone(), RECEIVER).await
        };
        let caller = if managed {
            client.rpc_client::<RequestProbe>("request")
        } else {
            client.caller::<RequestProbe>().to(SENDER).build()
        }
        .expect("caller");
        let count = AtomicU64::new(0);
        let key = Key::topic(DOMAIN, Some(SENDER), RequestProbe::TYPE_NAME);
        let reply_key = key.clone();
        let _server = bus
            .respond(
                &key,
                Box::new(move |query| {
                    let attachment = match count.fetch_add(1, Ordering::SeqCst) {
                        0 => None,
                        1 => Some(vec![1]),
                        2 => crate::pubsub::fingerprint(Some(0xffff)),
                        _ => crate::pubsub::fingerprint(Some(Probe::SCHEMA_FINGERPRINT)),
                    };
                    query
                        .reply(&reply_key, Probe { value: 3 }.encode_to_vec(), attachment)
                        .expect("reply");
                }),
            )
            .expect("raw server");
        for _ in 0..2 {
            assert!(matches!(
                timeout(PATIENCE, caller.call(RequestProbe { value: 1 }))
                    .await
                    .expect("call"),
                Err(CallError::MissingSchema)
            ));
        }
        assert!(matches!(
            timeout(PATIENCE, caller.call(RequestProbe { value: 1 }))
                .await
                .expect("call"),
            Err(CallError::Schema { .. })
        ));
        assert_eq!(
            timeout(PATIENCE, caller.call(RequestProbe { value: 1 }))
                .await
                .expect("call")
                .expect("verified reply")
                .value,
            3
        );
    }
}

#[tokio::test]
async fn operation_and_response_type_mismatches_cannot_register() {
    let bus = Arc::new(Local::new());
    let mut binding = bindings(RECEIVER);
    let mut wrong = rpc_contract();
    wrong.response = Some("other.Probe".into());
    binding.inputs.insert(
        "request".into(),
        InputBinding {
            contract: wrong.clone(),
            sources: vec![SENDER.into()],
        },
    );
    binding
        .outputs
        .insert("request".into(), OutputBinding { contract: wrong });
    let cloudy = ModuleContext::open(options(bus, binding))
        .await
        .expect("module");
    assert!(cloudy.rpc_client::<RequestProbe>("request").is_err());
    assert!(cloudy.rpc_server::<RequestProbe>("request").is_err());
    assert!(cloudy.input::<RequestProbe>("request").is_err());
    assert!(cloudy.output::<RequestProbe>("request").is_err());
    assert!(cloudy.ready().is_err());
}

#[tokio::test]
async fn rpc_multiple_targets_are_rejected_before_launch_presence() {
    let bus = Arc::new(Local::new());
    let mut binding = bindings(RECEIVER);
    binding.inputs.insert(
        "request".into(),
        InputBinding {
            contract: rpc_contract(),
            sources: vec![SENDER.into(), "deployment/other".into()],
        },
    );
    assert!(
        ModuleContext::open(options(bus.clone(), binding))
            .await
            .is_err()
    );
    assert_eq!(
        bus.alive(&Key::launch(DOMAIN, Some(RECEIVER)), PATIENCE)
            .await
            .expect("launches")
            .len(),
        0
    );
}
