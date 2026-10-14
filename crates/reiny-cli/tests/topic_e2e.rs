//! The end-to-end test of `reiny topic / node / service` — list / hz / echo / node / service call
//! against a real zenoh fabric, driven through the built `reiny` binary.
//!
//! The same style as `bag_e2e`: the test side holds one zenoh session "playing a launch" and acts out
//! the publisher / presence / `@schema` / service queryable parts in plain zenoh. The descriptors are
//! built by hand with `prost-types` (no protoc). A fixed loopback TCP port, multicast off.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto,
};
use reiny::zenoh::{self, Wait};

/// The loopback port for this test alone (the next one after e2e's 37447, `bag_e2e`'s 37448 and `rpc_e2e`'s 37449).
const ENDPOINT: &str = "tcp/127.0.0.1:37450";
const BIN: &str = env!("CARGO_BIN_EXE_reiny");

#[derive(Clone, PartialEq, Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    seq: u32,
    #[prost(string, tag = "2")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct Add {
    #[prost(int32, tag = "1")]
    a: i32,
    #[prost(int32, tag = "2")]
    b: i32,
}

#[derive(Clone, PartialEq, Message)]
struct Sum {
    #[prost(int32, tag = "1")]
    sum: i32,
}

fn field(name: &str, number: i32, ty: field_descriptor_proto::Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(field_descriptor_proto::Label::Optional as i32),
        r#type: Some(ty as i32),
        json_name: Some(name.into()),
        ..Default::default()
    }
}

/// `package e2e; message Probe {uint32 seq=1; string name=2;} message Add {int32 a=1; int32 b=2;}
/// message Sum {int32 sum=1;}` in one file.
fn file_set() -> Vec<u8> {
    use field_descriptor_proto::Type;
    let msg = |name: &str, fields: Vec<FieldDescriptorProto>| DescriptorProto {
        name: Some(name.into()),
        field: fields,
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("e2e.proto".into()),
            package: Some("e2e".into()),
            message_type: vec![
                msg(
                    "Probe",
                    vec![
                        field("seq", 1, Type::Uint32),
                        field("name", 2, Type::String),
                    ],
                ),
                msg(
                    "Add",
                    vec![field("a", 1, Type::Int32), field("b", 2, Type::Int32)],
                ),
                msg("Sum", vec![field("sum", 1, Type::Int32)]),
            ],
            syntax: Some("proto3".into()),
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn fabric() -> zenoh::Session {
    let mut config = zenoh::Config::default();
    for (k, v) in [
        ("listen/endpoints", format!("[\"{ENDPOINT}\"]")),
        ("scouting/multicast/enabled", "false".to_string()),
    ] {
        config.insert_json5(k, &v).expect("zenoh config");
    }
    zenoh::open(config).wait().expect("fabric session")
}

fn reiny(args: &[&str]) -> Output {
    reiny_with_policy(
        args,
        reiny::bindings::ReceiveBuffer::Fifo,
        reiny::bindings::Replay::Live,
    )
}

fn reiny_with_policy(
    args: &[&str],
    buffer: reiny::bindings::ReceiveBuffer,
    replay: reiny::bindings::Replay,
) -> Output {
    let bindings =
        std::env::temp_dir().join(format!("reiny-topic-bindings-{}.json", std::process::id()));
    let mut contract: reiny::bindings::ModuleBindings = serde_yaml::from_str(
        "version: 3\nnamespace: sim\nendpoint_namespace: sim\ninputs:\n  Probe:\n    contract: {type: e2e.Probe}\n    sources: [{namespace: ctrl, port: probe}]\n  Add:\n    contract: {type: e2e.Add, kind: rpc, response: e2e.Sum}\n    sources: [{namespace: ctrl, port: add}]\noutputs:\n  Cmd:\n    contract: {type: e2e.Sum, qos: sensor, retention: last}\n",
    ).unwrap();
    let probe = &mut contract.inputs.get_mut("Probe").unwrap().contract;
    probe.buffer = buffer;
    probe.replay = replay;
    let data = args.len() > 2
        && matches!(
            (args[0], args[1]),
            ("topic", "pub" | "echo" | "hz" | "bw") | ("service", "call")
        );
    if data {
        if args[1] == "pub" {
            contract.inputs.clear();
            contract.outputs.retain(|name, _| name == args[2]);
        } else {
            contract.outputs.clear();
            contract.inputs.retain(|name, _| name == args[2]);
        }
    }
    let report = bindings.with_extension("report.json");
    std::fs::write(&bindings, serde_json::to_vec(&contract).unwrap()).unwrap();
    let output = Command::new(BIN)
        .args(args)
        .env("REINY_MODULE_REPORT", &report)
        .args(["--name", "sim"])
        .arg("--module-bindings")
        .arg(&bindings)
        .args([
            "--domain",
            "lab",
            "--connect",
            ENDPOINT,
            "--zenoh-mode",
            "client",
        ])
        .output()
        .expect("spawn reiny");
    std::fs::remove_file(bindings).unwrap();
    if data && output.status.success() {
        let actual: reiny::bindings::ModuleReport =
            serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
        assert_eq!(actual.namespace, "sim");
        assert_eq!(actual.inputs.len(), contract.inputs.len());
        assert_eq!(actual.outputs.len(), contract.outputs.len());
        for port in actual.inputs.values().chain(actual.outputs.values()) {
            assert_eq!(
                Some(port.schema),
                reiny_build::message_fingerprint(&file_set(), &port.type_name).unwrap()
            );
        }
    }
    if report.exists() {
        std::fs::remove_file(report).unwrap();
    }
    output
}

fn stdout(o: &Output) -> String {
    assert!(
        o.status.success(),
        "reiny failed: {}\n{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    String::from_utf8_lossy(&o.stdout).into_owned()
}

/// The queryable that announces a descriptor set at `<key>/@schema/<fqn>`.
fn schema_queryable(fab: &zenoh::Session, key: &str, fqn: &str) -> zenoh::query::Queryable<()> {
    let reply_key = format!("{key}/@schema/{fqn}");
    let k = reply_key.clone();
    let set = file_set();
    fab.declare_queryable(reply_key)
        .callback(move |q| {
            let _ = q.reply(k.clone(), set.clone()).wait();
        })
        .wait()
        .expect("schema queryable")
}

#[test]
#[allow(clippy::too_many_lines)] // one pass end to end (so as not to add another port), hence long
fn topic_node_service_against_live_launch() {
    let fab = fabric();
    let fp = reiny_build::message_fingerprint(&file_set(), "e2e.Probe")
        .unwrap()
        .unwrap();

    // --- playing the launch ctrl: @launch, plus Probe's publisher + presence + @schema ---
    let _launch = fab
        .liveliness()
        .declare_token("reiny/v3/lab/ctrl/@launch")
        .wait()
        .expect("launch token");
    let probe_key = "reiny/v3/lab/ctrl/@out/probe/e2e.Probe";
    let probe_pub = Arc::new(fab.declare_publisher(probe_key).wait().expect("probe pub"));
    let _probe_token = fab
        .liveliness()
        .declare_token(probe_key)
        .wait()
        .expect("probe token");
    let _probe_schema = schema_queryable(&fab, probe_key, "e2e.Probe");

    // --- the service Add: a queryable + @service + @schema (two: the request and the response) ---
    let add_key = "reiny/v3/lab/ctrl/@out/add/e2e.Add";
    let _add_token = fab
        .liveliness()
        .declare_token(format!("{add_key}/@service"))
        .wait()
        .expect("service token");
    let _add_schema_req = schema_queryable(&fab, add_key, "e2e.Add");
    let _add_schema_resp = schema_queryable(&fab, add_key, "e2e.Sum");
    let add_fp = reiny_build::message_fingerprint(&file_set(), "e2e.Add")
        .unwrap()
        .unwrap();
    let sum_fp = reiny_build::message_fingerprint(&file_set(), "e2e.Sum")
        .unwrap()
        .unwrap();
    let _add_q = fab
        .declare_queryable(add_key)
        .callback(move |q| {
            let Some(payload) = q.payload() else { return };
            assert_eq!(
                q.attachment()
                    .map(|attachment| attachment.to_bytes().into_owned()),
                Some(add_fp.to_le_bytes().to_vec())
            );
            let add = Add::decode(payload.to_bytes().as_ref()).expect("decode Add");
            let sum = Sum { sum: add.a + add.b };
            let _ = q
                .reply(add_key, sum.encode_to_vec())
                .attachment(sum_fp.to_le_bytes().to_vec())
                .wait();
        })
        .wait()
        .expect("add queryable");

    // Publish only after the CLI announces its opened input, never after a timed settle.
    let live = Arc::new(AtomicBool::new(true));
    let _feed = {
        let live = Arc::clone(&live);
        let publisher = Arc::clone(&probe_pub);
        fab.liveliness()
            .declare_subscriber("reiny/v3/lab/sim/@ready")
            .callback(move |sample| {
                if sample.kind() != zenoh::sample::SampleKind::Put || !live.load(Ordering::SeqCst) {
                    return;
                }
                let invalid = Probe {
                    seq: 99,
                    name: "invalid".into(),
                }
                .encode_to_vec();
                publisher.put(invalid.clone()).wait().unwrap();
                publisher
                    .put(invalid)
                    .attachment((fp ^ 1).to_le_bytes().to_vec())
                    .wait()
                    .unwrap();
                for seq in 1..=3 {
                    publisher
                        .put(
                            Probe {
                                seq,
                                name: "probe".into(),
                            }
                            .encode_to_vec(),
                        )
                        .attachment(fp.to_le_bytes().to_vec())
                        .wait()
                        .unwrap();
                }
            })
            .wait()
            .unwrap()
    };

    // --- playing the launch gui: it only subscribes to Cmd (@sub + @schema; there is no publisher) ---
    let cmd_key = "reiny/v3/lab/gui/@in/command/e2e.Sum";
    let _cmd_sub_token = fab
        .liveliness()
        .declare_token(format!("{cmd_key}/@sub"))
        .wait()
        .expect("sub token");
    let _cmd_schema = schema_queryable(&fab, cmd_key, "e2e.Sum");
    let (cmd_seen, cmd_received) = std::sync::mpsc::channel();
    let _cmd_sub = {
        fab.declare_subscriber("reiny/v3/lab/sim/@out/Cmd/e2e.Sum")
            .callback(move |s| {
                if Sum::decode(s.payload().to_bytes().as_ref()).is_ok_and(|v| v.sum == 7) {
                    let _ = cmd_seen.send(());
                }
            })
            .wait()
            .expect("cmd subscriber")
    };

    // --- node list / info ---
    let out = stdout(&reiny(&["node", "list"]));
    assert!(out.lines().any(|l| l.trim() == "ctrl"), "node list: {out}");
    let out = stdout(&reiny(&["node", "info", "ctrl"]));
    assert!(
        out.contains("pub : probe (e2e.Probe)") && out.contains("srv : add (e2e.Add)"),
        "node info: {out}"
    );
    // A launch that only listens is still a launch, and `sub :` is the line that says so.
    let out = stdout(&reiny(&["node", "info", "gui"]));
    assert!(
        out.contains("sub : command (e2e.Sum)") && out.contains("pub : -"),
        "node info gui: {out}"
    );

    // --- topic list: Probe is [pub ctrl], Add is [srv ctrl], Cmd is [sub gui] ---
    let out = stdout(&reiny(&["topic", "list"]));
    let row = |ty: &str| {
        out.lines()
            .find(|l| l.starts_with(ty))
            .unwrap_or_else(|| panic!("{ty} row in: {out}"))
            .split_whitespace()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    };
    // TYPE_NAME  PUB  SUB  SRV
    assert_eq!(
        row("e2e.Probe"),
        ["e2e.Probe", "ctrl:probe", "-", "-"],
        "{out}"
    );
    assert_eq!(row("e2e.Add"), ["e2e.Add", "-", "-", "ctrl:add"], "{out}");
    assert_eq!(
        row("e2e.Sum"),
        ["e2e.Sum", "-", "gui:command", "-"],
        "{out}"
    );

    // --- topic hz: a Hz appears on ctrl's row ---
    let out = stdout(&reiny(&["topic", "hz", "Probe", "--duration", "1.5"]));
    assert!(
        out.contains("ctrl") && out.contains("Hz"),
        "topic hz: {out}"
    );

    // --- topic echo: the JSON decoded through @schema, with its source ---
    let out = stdout(&reiny(&[
        "topic",
        "echo",
        "Probe",
        "--count",
        "3",
        "--from",
        "ctrl",
        "--duration",
        "5",
    ]));
    let lines: Vec<&str> = out.lines().filter(|l| l.starts_with("ctrl")).collect();
    assert_eq!(lines.len(), 3, "topic echo: {out}");
    assert!(
        lines
            .iter()
            .all(|l| l.contains(r#""name":"probe""#) && l.contains(r#""seq":"#)),
        "topic echo JSON: {out}"
    );
    // --raw is hex.
    let out = stdout(&reiny(&["topic", "echo", "Probe", "--count", "1", "--raw"]));
    assert!(
        out.contains("120570726f6265"),
        "raw hex of \"probe\": {out}"
    );

    // The SDK, not a CLI-specific queue, implements latest buffering and retained replay.
    live.store(false, Ordering::SeqCst);
    let _retained = fab
        .declare_queryable(probe_key)
        .callback(move |q| {
            q.reply(
                probe_key,
                Probe {
                    seq: 42,
                    name: "retained".into(),
                }
                .encode_to_vec(),
            )
            .attachment(fp.to_le_bytes().to_vec())
            .wait()
            .unwrap();
        })
        .wait()
        .unwrap();
    let out = stdout(&reiny_with_policy(
        &["topic", "echo", "Probe", "--count", "1", "--duration", "5"],
        reiny::bindings::ReceiveBuffer::Latest(1),
        reiny::bindings::Replay::Last,
    ));
    let retained = out.trim().strip_prefix("ctrl:probe  ").unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(retained).unwrap(),
        serde_json::json!({"seq": 42, "name": "retained"})
    );

    // A cooperative stop ends both receive commands even without count/duration limits.
    for command in ["echo", "hz"] {
        let (ready, seen) = std::sync::mpsc::channel();
        let _ready = fab
            .liveliness()
            .declare_subscriber("reiny/v3/lab/sim/@ready")
            .callback(move |sample| {
                if sample.kind() == zenoh::sample::SampleKind::Put {
                    ready.send(()).unwrap();
                }
            })
            .wait()
            .unwrap();
        let (finished, result) = std::sync::mpsc::channel();
        let cli = thread::spawn(move || {
            finished.send(reiny(&["topic", command, "Probe"])).unwrap();
        });
        seen.recv_timeout(Duration::from_secs(10))
            .expect("CLI readiness");
        let replies = fab.get("reiny/v3/lab/sim/@stop").wait().unwrap();
        let reply = replies
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            reply.result().unwrap().payload().to_bytes().as_ref(),
            b"ack"
        );
        assert!(
            result
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .status
                .success()
        );
        cli.join().unwrap();
    }

    // --- service list / call ---
    let out = stdout(&reiny(&["service", "list"]));
    assert!(
        out.contains("Add") && out.contains("e2e.Sum") && out.contains("ctrl"),
        "service list: {out}"
    );
    let out = stdout(&reiny(&[
        "service",
        "call",
        "Add",
        r#"{"a": 2, "b": 3}"#,
        "--to",
        "ctrl",
    ]));
    assert_eq!(out.trim(), r#"{"sum":5}"#, "service call: {out}");
    let ghost = reiny(&["service", "call", "Add", "{}", "--to", "ghost"]);
    assert!(
        !ghost.status.success(),
        "call to an absent server must fail"
    );

    // --- topic pub: a listen-only launch can be poked, because it announces `@schema` too ---
    let out = stdout(&reiny(&[
        "topic",
        "pub",
        "Cmd",
        r#"{"sum": 7}"#,
        "--as",
        "sim",
    ]));
    assert!(
        out.contains("reiny/v3/lab/sim/@out/Cmd/e2e.Sum: sent 1") && out.contains("e2e.Sum"),
        "topic pub: {out}"
    );
    cmd_received
        .recv_timeout(Duration::from_secs(5))
        .expect("the subscriber must have received the published Cmd");
    // A source id that would corrupt the key is refused before anything is declared.
    assert!(
        !reiny(&["topic", "pub", "Cmd", "{}", "--as", "a/b"])
            .status
            .success(),
        "--as must be a single key segment"
    );
    // Nothing describes this type, so there is no way to encode for it.
    assert!(
        !reiny(&["topic", "pub", "Nope", "{}"]).status.success(),
        "an undescribed type must fail rather than guess"
    );
}
