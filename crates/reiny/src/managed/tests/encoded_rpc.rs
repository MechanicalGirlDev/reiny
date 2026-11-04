//! Encoded and native ports share real admission, transport and RPC outcomes.

use super::*;
use crate::bindings::{PortKind, ReportEntry, ReportErrorCode, validate_graph};
use crate::encoded::EncodedType;
use crate::managed::HostedOptions;
use crate::{CallError, Service};

impl Service for OtherProbe {
    type Response = Probe;
}

fn rpc_contract() -> EndpointContract {
    EndpointContract {
        kind: PortKind::Rpc,
        response: Some(Probe::TYPE_NAME.into()),
        ..contract(OtherProbe::TYPE_NAME)
    }
}

async fn pair(
    server_bus: Arc<dyn Engine>,
    client_bus: Arc<dyn Engine>,
) -> crate::Result<(ModuleContext, ModuleContext)> {
    let mut server = bindings(SENDER);
    let mut client = bindings(RECEIVER);
    for (port, contract) in [
        ("rpc", rpc_contract()),
        ("stream", contract(Probe::TYPE_NAME)),
        ("other", contract(Probe::TYPE_NAME)),
    ] {
        server.outputs.insert(
            port.into(),
            OutputBinding {
                contract: contract.clone(),
            },
        );
        client.inputs.insert(
            port.into(),
            InputBinding {
                contract,
                sources: vec![endpoint(SENDER, port)],
            },
        );
    }
    let open = |engine, bindings| {
        ModuleContext::open_hosted(HostedOptions {
            domain: DOMAIN.into(),
            bindings,
            config: None,
            engine,
        })
    };
    Ok((
        open(server_bus, server).await?,
        open(client_bus, client).await?,
    ))
}

fn admit(
    server: &ModuleContext,
    client: &ModuleContext,
) -> crate::Result<(
    crate::managed::ReadyAdmission,
    crate::managed::ReadyAdmission,
)> {
    let output = server.prepare_admission()?;
    let input = client.prepare_admission()?;
    let server_bindings = server
        .module
        .bindings
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("bindings"))?;
    let client_bindings = client
        .module
        .bindings
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("bindings"))?;
    validate_graph(&[
        ReportEntry {
            bindings: server_bindings,
            report: output.report(),
        },
        ReportEntry {
            bindings: client_bindings,
            report: input.report(),
        },
    ])?;
    assert_eq!(
        output.report().outputs["rpc"].schema,
        OtherProbe::SCHEMA_FINGERPRINT
    );
    assert_eq!(
        output.report().outputs["rpc"]
            .response
            .as_ref()
            .map(|response| response.schema),
        Some(Probe::SCHEMA_FINGERPRINT),
    );
    Ok((
        server.commit_admission(output)?,
        client.commit_admission(input)?,
    ))
}

#[tokio::test]
async fn encoded_rpc_permissions_and_types_precede_registration() -> crate::Result<()> {
    let bus = Arc::new(Local::new());
    let (server, client) = pair(bus.clone(), bus).await?;
    let request = EncodedType::of::<OtherProbe>()?;
    let response = EncodedType::of::<Probe>()?;
    assert!(
        server
            .raw_rpc_server("missing", &request, &response)
            .is_err()
    );
    assert!(
        server
            .raw_rpc_server("stream", &request, &response)
            .is_err()
    );
    assert!(server.raw_rpc_server("rpc", &response, &request).is_err());
    assert!(server.raw_rpc_server("rpc", &request, &request).is_err());
    assert!(client.raw_rpc_server("rpc", &request, &response).is_err());
    assert!(client.raw_rpc_client("rpc", &request, &request).is_err());
    // Failed opens must not reserve an endpoint or prevent a later valid declaration.
    let _server = server.raw_rpc_server("rpc", &request, &response)?;
    let _client = client.raw_rpc_client("rpc", &request, &response)?;
    assert!(server.rpc_server::<OtherProbe>("rpc").is_err());
    assert!(client.rpc_client::<OtherProbe>("rpc").is_err());
    Ok(())
}

async fn exercise(server_bus: Arc<dyn Engine>, client_bus: Arc<dyn Engine>) -> crate::Result<()> {
    let (seen, subscribed) = flume::unbounded();
    let _watch = server_bus.watch_alive(
        &Key::input(DOMAIN, RECEIVER, "other", Probe::TYPE_NAME)
            .with_chunk(crate::engine::SUB_CHUNK),
        Box::new(move |event| {
            let _ = seen.send(event);
        }),
    )?;
    let (server, client) = pair(server_bus, client_bus).await?;
    let request = EncodedType::of::<OtherProbe>()?;
    let response = EncodedType::of::<Probe>()?;
    let mut rpc = server.raw_rpc_server("rpc", &request, &response)?;
    let caller = client.rpc_client::<OtherProbe>("rpc")?;
    assert!(server.rpc_server::<OtherProbe>("rpc").is_err());
    assert!(client.raw_rpc_client("rpc", &request, &response).is_err());
    let outgoing = server.raw_output("stream", response.type_name(), response.fingerprint())?;
    let unrelated = server.output::<Probe>("other")?;
    let mut incoming = client.input::<Probe>("stream")?;
    let mut other = client.raw_input("other", response.type_name(), response.fingerprint())?;
    assert!(matches!(
        timeout(PATIENCE, subscribed.recv_async()).await??,
        Presence::Joined(_)
    ));
    let (_server_ready, _client_ready) = admit(&server, &client)?;
    let mut ready = client.watch_input_ready("stream")?;
    timeout(PATIENCE, ready.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("ready closed"))?;

    outgoing.send(Probe { value: 42 }.encode_to_vec())?;
    unrelated.send(Probe { value: 99 }).await?;
    let message = timeout(PATIENCE, incoming.recv_envelope())
        .await?
        .ok_or_else(|| anyhow::anyhow!("stream closed"))?;
    assert_eq!(
        (
            message.value.value,
            message.source.as_str(),
            message.source_port.as_str()
        ),
        (42, SENDER, "stream")
    );
    let message = timeout(PATIENCE, other.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("other closed"))?;
    assert_eq!(
        (
            Probe::decode(message.payload.as_slice())?.value,
            message.source_port.as_str()
        ),
        (99, "other")
    );

    let server_work = async {
        let first = rpc
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("request missing"))?;
        assert_eq!(OtherProbe::decode(first.payload())?.value, 7);
        first.reply(Probe { value: 8 }.encode_to_vec()).await?;
        let remote = rpc
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("request missing"))?;
        remote.reply_err("refused").await?;
        let unanswered = rpc
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("request missing"))?;
        drop(unanswered);
        crate::Result::Ok(())
    };
    let client_work = async {
        assert_eq!(caller.call(OtherProbe { value: 7 }).await?.value, 8);
        assert!(
            matches!(caller.call(OtherProbe { value: 1 }).await, Err(CallError::Remote(message)) if message == "refused")
        );
        assert!(matches!(
            caller.call(OtherProbe { value: 1 }).await,
            Err(CallError::NoReply)
        ));
        crate::Result::Ok(())
    };
    timeout(PATIENCE, async {
        tokio::try_join!(server_work, client_work)
    })
    .await??;
    drop(rpc);
    timeout(PATIENCE, server.shutdown()).await?;
    assert_eq!(
        *server.hosted_events().borrow(),
        Some(crate::managed::HostedEvent::ReadinessLost)
    );
    Ok(())
}

#[tokio::test]
async fn encoded_rpc_local_graph() -> crate::Result<()> {
    let bus = Arc::new(Local::new());
    exercise(bus.clone(), bus).await
}

#[tokio::test]
async fn encoded_client_typed_server_deadline_and_decode() -> crate::Result<()> {
    let bus = Arc::new(Local::new());
    let (server, client) = pair(bus.clone(), bus).await?;
    let request = EncodedType::of::<OtherProbe>()?;
    let response = EncodedType::of::<Probe>()?;
    let mut rpc = server.rpc_server::<OtherProbe>("rpc")?;
    let caller = client.raw_rpc_client("rpc", &request, &response)?;
    let _outputs = [
        server.output::<Probe>("stream")?,
        server.output::<Probe>("other")?,
    ];
    let _inputs = [
        client.input::<Probe>("stream")?,
        client.input::<Probe>("other")?,
    ];
    let (_server_ready, _client_ready) = admit(&server, &client)?;
    // Registered before dispatch: the request stays owned until the caller observes its deadline.
    let (finished, wait_finished) = tokio::sync::oneshot::channel();
    let server_work = async {
        let first = rpc
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("request missing"))?;
        let value = first.value.value;
        first.reply(Probe { value }).await?;
        let held = rpc
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("request missing"))?;
        wait_finished.await?;
        drop(held);
        // Typed decoding rejects malformed protobuf; the raw transport does not pretend to decode.
        assert!(rpc.recv().await.is_none());
        crate::Result::Ok(())
    };
    let client_work = async {
        let bytes = caller.call(OtherProbe { value: 5 }.encode_to_vec()).await?;
        assert_eq!(Probe::decode(bytes.as_slice())?.value, 5);
        assert!(matches!(
            caller
                .call_with_timeout(Vec::new(), Duration::from_millis(20))
                .await,
            Err(CallError::Timeout)
        ));
        finished
            .send(())
            .map_err(|()| anyhow::anyhow!("server gone"))?;
        assert!(matches!(
            caller.call(vec![0]).await,
            Err(CallError::Remote(_))
        ));
        server.shutdown_handle().trigger();
        crate::Result::Ok(())
    };
    timeout(PATIENCE, async {
        tokio::try_join!(server_work, client_work)
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn encoded_graph_rejects_independent_request_and_response_fingerprints() -> crate::Result<()>
{
    for response_mismatch in [false, true] {
        let bus = Arc::new(Local::new());
        let (server, client) = pair(bus.clone(), bus).await?;
        let request = EncodedType::of::<OtherProbe>()?;
        let response = EncodedType::of::<Probe>()?;
        let bad = if response_mismatch {
            &response
        } else {
            &request
        };
        let bad = EncodedType::new(
            bad.type_name(),
            bad.fingerprint_version(),
            bad.fingerprint() ^ 1,
        )?;
        let _rpc = server.raw_rpc_server("rpc", &request, &response)?;
        let _caller = client.raw_rpc_client(
            "rpc",
            if response_mismatch { &request } else { &bad },
            if response_mismatch { &bad } else { &response },
        )?;
        let _outputs = [
            server.output::<Probe>("stream")?,
            server.output::<Probe>("other")?,
        ];
        let _inputs = [
            client.input::<Probe>("stream")?,
            client.input::<Probe>("other")?,
        ];
        let failure = admit(&server, &client)
            .err()
            .ok_or_else(|| anyhow::anyhow!("unexpected admission"))?;
        assert_eq!(
            failure
                .downcast_ref::<crate::bindings::ReportError>()
                .map(|error| error.code),
            Some(ReportErrorCode::Schema)
        );
    }
    Ok(())
}

#[tokio::test]
async fn encoded_rpc_retention_queries_and_opaque_requests() -> crate::Result<()> {
    let bus = Arc::new(Local::new());
    let (server, client) = pair(bus.clone(), bus.clone()).await?;
    let request = EncodedType::of::<OtherProbe>()?;
    let response = EncodedType::of::<Probe>()?;
    let mut rpc = server.raw_rpc_server("rpc", &request, &response)?;
    let caller = client.raw_rpc_client("rpc", &request, &response)?;
    let _outputs = [
        server.output::<Probe>("stream")?,
        server.output::<Probe>("other")?,
    ];
    let _inputs = [
        client.input::<Probe>("stream")?,
        client.input::<Probe>("other")?,
    ];
    let (_server_ready, _client_ready) = admit(&server, &client)?;
    let key = Key::output(DOMAIN, SENDER, "rpc", OtherProbe::TYPE_NAME);
    let serve = async {
        let opaque = rpc
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("request missing"))?;
        assert_eq!(opaque.payload(), &[0]);
        opaque.reply(vec![0]).await?;
        let empty = rpc
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("request missing"))?;
        assert_eq!(empty.payload(), b"");
        empty.reply(Vec::new()).await?;
        drop(
            rpc.recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("request missing"))?,
        );
        crate::Result::Ok(())
    };
    let call = async {
        for (payload, attachment) in [
            (None, None),
            (Some(Vec::new()), None),
            (Some(Vec::new()), Some(vec![1])),
            (Some(Vec::new()), Some(42_u64.to_le_bytes().to_vec())),
        ] {
            let retained = payload.is_none();
            let mut replies = bus.query(
                &key,
                QueryParams {
                    payload,
                    attachment,
                    timeout: PATIENCE,
                },
            )?;
            let reply = replies.next().await;
            if retained {
                assert!(reply.is_none());
            } else {
                assert!(matches!(reply, Some(Err(_))));
            }
        }
        assert_eq!(caller.call(vec![0]).await?, vec![0]);
        assert_eq!(caller.call(Vec::new()).await?, Vec::<u8>::new());
        assert!(matches!(
            caller.call(Vec::new()).await,
            Err(CallError::NoReply)
        ));
        crate::Result::Ok(())
    };
    timeout(PATIENCE, async { tokio::try_join!(serve, call) }).await??;
    Ok(())
}

#[tokio::test]
async fn encoded_client_preserves_response_failures_and_never_retries() -> crate::Result<()> {
    let bus = Arc::new(Local::new());
    let (server, client) = pair(bus.clone(), bus.clone()).await?;
    let request = EncodedType::of::<OtherProbe>()?;
    let response = EncodedType::of::<Probe>()?;
    let _rpc = server.raw_rpc_server("rpc", &request, &response)?;
    let caller = client.raw_rpc_client("rpc", &request, &response)?;
    let _outputs = [
        server.output::<Probe>("stream")?,
        server.output::<Probe>("other")?,
    ];
    let _inputs = [
        client.input::<Probe>("stream")?,
        client.input::<Probe>("other")?,
    ];
    let (_server_ready, _client_ready) = admit(&server, &client)?;
    let key = Key::output(DOMAIN, SENDER, "rpc", OtherProbe::TYPE_NAME);
    for case in 0..4 {
        let calls = Arc::new(AtomicU64::new(0));
        let count = calls.clone();
        let mut source = key.clone();
        if case == 3 {
            source = Key::output(DOMAIN, "undeclared/server", "rpc", OtherProbe::TYPE_NAME);
        }
        let (sent, completed) = flume::bounded(1);
        let _bad = bus.respond(
            &key,
            Box::new(move |query| {
                count.fetch_add(1, Ordering::SeqCst);
                let attachment = match case {
                    0 => None,
                    1 => Some(vec![1]),
                    2 => Some(999_u64.to_le_bytes().to_vec()),
                    _ => Some(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec()),
                };
                let _ = sent.send(query.reply(&source, Vec::new(), attachment));
            }),
        )?;
        let failure = timeout(PATIENCE, caller.call(Vec::new())).await?;
        match case {
            0 | 1 => assert!(matches!(failure, Err(CallError::MissingSchema))),
            2 => assert!(matches!(
                failure,
                Err(CallError::Schema {
                    expected: Probe::SCHEMA_FINGERPRINT,
                    received: 999
                })
            )),
            _ => assert!(matches!(failure, Err(CallError::Engine(_)))),
        }
        timeout(PATIENCE, completed.recv_async()).await???;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    // An unpolled operation is definite pre-dispatch cancellation.
    drop(caller.call(Vec::new()));
    Ok(())
}

#[cfg(feature = "zenoh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoded_rpc_zenoh_graph() -> crate::Result<()> {
    let configure = |listen| {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .map_err(anyhow::Error::msg)?;
        config
            .insert_json5(
                if listen {
                    "listen/endpoints"
                } else {
                    "connect/endpoints"
                },
                "[\"tcp/127.0.0.1:37591\"]",
            )
            .map_err(anyhow::Error::msg)?;
        crate::Result::Ok(config)
    };
    let server = Arc::new(crate::engine::Zenoh::open(configure(true)?).await?);
    let client = Arc::new(crate::engine::Zenoh::open(configure(false)?).await?);
    exercise(server, client).await
}
