use super::*;
use crate::bindings::{CONTRACT_VERSION, InputBinding, OutputBinding, Replay, Retention};
use crate::engine::Local;
use crate::{Descriptor, RuntimeOptions, Service, Topic};
use tokio::time::timeout;

#[derive(Clone, PartialEq, prost::Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    value: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Command {
    #[prost(uint32, tag = "1")]
    value: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Add {
    #[prost(uint32, tag = "1")]
    value: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Sum {
    #[prost(uint32, tag = "1")]
    value: u32,
}

macro_rules! topic {
    ($ty:ty, $name:literal, $schema:literal) => {
        impl Topic for $ty {
            const TYPE: &'static str = $name;
            const SCHEMA: u64 = $schema;
            const DESCRIPTOR: Option<Descriptor> = Some(Descriptor {
                message: concat!("bridge.", $name),
                file_set: b"bridge-test-descriptor",
            });
        }
    };
}
topic!(Probe, "Probe", 0x77);
topic!(Command, "Command", 0x88);
topic!(Add, "Add", 0x99);
topic!(Sum, "Sum", 0xaa);
impl Service for Add {
    type Response = Sum;
}

const PATIENCE: Duration = Duration::from_secs(5);
const DOMAIN: &str = "bridge";

fn bindings(id: &str) -> ModuleBindings {
    ModuleBindings {
        version: CONTRACT_VERSION,
        namespace: id.into(),
        endpoint_namespace: id.into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    }
}

fn contract(ty: &str) -> EndpointContract {
    EndpointContract {
        type_name: format!("bridge.{ty}"),
        ..Default::default()
    }
}

fn rpc() -> EndpointContract {
    EndpointContract {
        kind: PortKind::Rpc,
        response: Some("bridge.Sum".into()),
        ..contract("Add")
    }
}

fn input(b: &mut ModuleBindings, name: &str, source: &str, contract: EndpointContract) {
    b.inputs.insert(
        name.into(),
        InputBinding {
            contract,
            sources: vec![source.into()],
        },
    );
}

fn output(b: &mut ModuleBindings, name: &str, contract: EndpointContract) {
    b.outputs.insert(name.into(), OutputBinding { contract });
}

async fn open(bus: Arc<Local>, bindings: ModuleBindings) -> Cloudy {
    let mut opts = RuntimeOptions::new(&bindings.namespace);
    opts.domain = DOMAIN.into();
    opts.engine = Some(bus);
    opts.module_report_path = None;
    opts.module_bindings = Some(bindings);
    Cloudy::open(opts).await.expect("bound context")
}

fn schemas() -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("bridge.Probe".into(), Probe::SCHEMA),
        ("bridge.Command".into(), Command::SCHEMA),
        ("bridge.Add".into(), Add::SCHEMA),
        ("bridge.Sum".into(), Sum::SCHEMA),
    ])
}

async fn bridge_contexts(left: &Arc<Local>, right: &Arc<Local>) -> (Cloudy, Cloudy, Bridge) {
    let mut a = bindings("bridge");
    input(&mut a, "state", "native", contract("Probe"));
    input(&mut a, "adder", "native", rpc());
    output(&mut a, "command", contract("Command"));
    let bridge_a = open(left.clone(), a).await;
    let mut b = bindings("bridge");
    input(&mut b, "command", "client", contract("Command"));
    output(
        &mut b,
        "state",
        EndpointContract {
            retention: Retention::Last,
            ..contract("Probe")
        },
    );
    output(&mut b, "adder", rpc());
    let bridge_b = open(right.clone(), b).await;
    let bridge = forward(&bridge_a, &bridge_b, &schemas()).expect("named bridge");
    bridge_a.ready().expect("left bridge ready");
    bridge_b.ready().expect("right bridge ready");
    (bridge_a, bridge_b, bridge)
}

#[tokio::test]
async fn named_streams_and_rpc_follow_sources_policies_and_readiness() {
    // Given two fabrics and explicit complementary bridge and application contracts.
    let left = Arc::new(Local::new());
    let right = Arc::new(Local::new());
    let mut source = bindings("native");
    output(&mut source, "state", contract("Probe"));
    output(&mut source, "adder", rpc());
    input(&mut source, "command", "bridge", contract("Command"));
    let native = open(left.clone(), source).await;
    let state = native.output::<Probe>("state").expect("state");
    let mut server = native.provides::<Add>("adder").expect("server");
    let mut commands = native.input::<Command>("command").expect("commands");
    native.ready().expect("native ready");
    let mut destination = bindings("client");
    input(&mut destination, "state", "bridge", contract("Probe"));
    input(&mut destination, "adder", "bridge", rpc());
    output(&mut destination, "command", contract("Command"));
    let client = open(right.clone(), destination).await;
    let mut incoming = client.input::<Probe>("state").expect("state input");
    let caller = client.uses::<Add>("adder").expect("caller");
    let command = client.output::<Command>("command").expect("command output");
    client.ready().expect("client ready");
    let (bridge_a, bridge_b, bridge) = bridge_contexts(&left, &right).await;
    let server_task = tokio::spawn(async move {
        let request = server.recv().await.expect("request");
        let value = request.value.value + 1;
        request.reply(Sum { value }).await.expect("reply");
    });
    let intruder = left
        .publisher(
            &Key::topic(DOMAIN, Some("undeclared"), Probe::TYPE),
            &crate::Qos::DEFAULT,
        )
        .expect("raw transport fixture");

    // When declared streams and RPC cross the bridge after an unauthorized source sample.
    intruder
        .put(vec![8, 99], Some(Probe::SCHEMA.to_le_bytes().to_vec()))
        .expect("foreign sample");
    state.send(Probe { value: 42 }).await.expect("state send");
    let received = timeout(PATIENCE, incoming.recv_envelope())
        .await
        .expect("state deadline")
        .expect("state");
    let mut late = bindings("late");
    input(
        &mut late,
        "state",
        "bridge",
        EndpointContract {
            replay: Replay::Last,
            ..contract("Probe")
        },
    );
    let late = open(right, late).await;
    let mut retained = late.input::<Probe>("state").expect("late input");
    late.ready().expect("late ready");
    let replayed = timeout(PATIENCE, retained.recv())
        .await
        .expect("retention deadline")
        .expect("retained sample");
    command
        .send(Command { value: 7 })
        .await
        .expect("command send");
    let received_command = timeout(PATIENCE, commands.recv_envelope())
        .await
        .expect("command deadline")
        .expect("command");
    let response = timeout(PATIENCE, caller.call(Add { value: 10 }))
        .await
        .expect("RPC deadline")
        .expect("RPC response");
    timeout(PATIENCE, server_task)
        .await
        .expect("server deadline")
        .expect("server task");

    // Then outputs have the declared bridge identity, retained state and validated RPC schemas.
    assert_eq!(
        (received.value.value, received.source.as_str()),
        (42, "bridge")
    );
    assert_eq!(replayed.value, 42);
    assert_eq!(
        (
            received_command.value.value,
            received_command.source.as_str()
        ),
        (7, "bridge")
    );
    assert_eq!(response.value, 11);
    assert_eq!(
        left.alive(&Key::topic(DOMAIN, Some("bridge"), Probe::TYPE), PATIENCE)
            .await
            .expect("left outputs"),
        Vec::<Key>::new()
    );
    drop(bridge);
    timeout(PATIENCE, bridge_a.shutdown())
        .await
        .expect("left readiness withdrawn");
    timeout(PATIENCE, bridge_b.shutdown())
        .await
        .expect("right readiness withdrawn");
}

#[tokio::test]
async fn missing_schema_and_unmatched_ports_cannot_create_routes() {
    // Given an input and an output with incompatible explicit contracts.
    let left = Arc::new(Local::new());
    let right = Arc::new(Local::new());
    let mut a = bindings("bridge");
    input(&mut a, "state", "native", contract("Probe"));
    let mut b = bindings("bridge");
    output(&mut b, "state", contract("Command"));
    let a = open(left.clone(), a).await;
    let b = open(right.clone(), b).await;

    // When the bridge tries to create routes without a compatible declared destination.
    assert!(forward(&a, &b, &schemas()).is_err());

    // Then no data endpoint or ready report can be declared.
    assert_eq!(
        left.alive(&Key::all(DOMAIN), PATIENCE)
            .await
            .expect("left data"),
        Vec::<Key>::new()
    );
    assert_eq!(
        right
            .alive(&Key::all(DOMAIN), PATIENCE)
            .await
            .expect("right data"),
        Vec::<Key>::new()
    );
    assert!(a.ready().is_err());
    assert!(b.ready().is_err());
    let mut matching = bindings("bridge");
    output(&mut matching, "state", contract("Probe"));
    let matching = open(right, matching).await;
    assert!(forward(&a, &matching, &BTreeMap::new()).is_err());
}

#[tokio::test]
async fn rpc_bridge_rejects_invalid_request_and_response_fingerprints() {
    // Given declared RPC routes and a real responder returning an incompatible stamp.
    let left = Arc::new(Local::new());
    let right = Arc::new(Local::new());
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = count.clone();
    let target = Key::topic(DOMAIN, Some("native"), Add::TYPE);
    let reply_key = target.clone();
    let _server = left
        .respond(
            &target,
            Box::new(move |query| {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                query
                    .reply(
                        &reply_key,
                        vec![8, 11],
                        Some(0xffff_u64.to_le_bytes().to_vec()),
                    )
                    .expect("invalid response fixture");
            }),
        )
        .expect("raw responder");
    let mut a = bindings("bridge");
    input(&mut a, "adder", "native", rpc());
    let a = open(left, a).await;
    let mut b = bindings("bridge");
    output(&mut b, "adder", rpc());
    let b = open(right.clone(), b).await;
    let _bridge = forward(&a, &b, &schemas()).expect("RPC bridge");
    a.ready().expect("caller ready");
    b.ready().expect("service ready");
    let endpoint = Key::topic(DOMAIN, Some("bridge"), Add::TYPE);

    // When malformed requests and then a valid request with a malformed reply cross the route.
    for stamp in [None, Some(vec![1]), Some(0xffff_u64.to_le_bytes().to_vec())] {
        let mut replies = right
            .query(
                &endpoint,
                QueryParams {
                    payload: Some(vec![8, 10]),
                    attachment: stamp,
                    timeout: PATIENCE,
                },
            )
            .expect("request");
        assert!(
            timeout(PATIENCE, replies.next())
                .await
                .expect("rejection deadline")
                .expect("rejection")
                .is_err()
        );
    }
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    let mut replies = right
        .query(
            &endpoint,
            QueryParams {
                payload: Some(vec![8, 10]),
                attachment: Some(Add::SCHEMA.to_le_bytes().to_vec()),
                timeout: PATIENCE,
            },
        )
        .expect("valid request");
    let rejected = timeout(PATIENCE, replies.next())
        .await
        .expect("response deadline")
        .expect("response rejection");

    // Then only the correctly stamped request is delivered and no incompatible reply is exposed.
    assert!(rejected.is_err());
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ambiguous_rpc_destinations_fail_before_creating_handles() {
    // Given two distinct targets of the same RPC type and one public service address.
    let left = Arc::new(Local::new());
    let right = Arc::new(Local::new());
    let mut a = bindings("bridge");
    input(&mut a, "first", "native", rpc());
    input(&mut a, "second", "other", rpc());
    let mut b = bindings("bridge");
    output(&mut b, "adder", rpc());
    let a = open(left.clone(), a).await;
    let b = open(right.clone(), b).await;

    // When routing cannot choose exactly one declared destination.
    let result = forward(&a, &b, &schemas());

    // Then neither a service token nor a ready report can hide the ambiguity.
    assert!(result.is_err());
    assert_eq!(
        left.alive(&Key::all(DOMAIN), PATIENCE)
            .await
            .expect("caller tokens"),
        Vec::<Key>::new()
    );
    assert_eq!(
        right
            .alive(&Key::all(DOMAIN).with_chunk(SERVICE_CHUNK), PATIENCE)
            .await
            .expect("service tokens"),
        Vec::<Key>::new()
    );
    assert!(a.ready().is_err());
    assert!(b.ready().is_err());
}
