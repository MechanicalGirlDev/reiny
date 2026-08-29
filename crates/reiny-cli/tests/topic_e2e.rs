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
    Command::new(BIN)
        .args(args)
        .args([
            "--domain",
            "lab",
            "--connect",
            ENDPOINT,
            "--zenoh-mode",
            "client",
        ])
        .output()
        .expect("spawn reiny")
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
        .declare_token("reiny/lab/ctrl/@launch")
        .wait()
        .expect("launch token");
    let probe_key = "reiny/lab/ctrl/Probe";
    let probe_pub = Arc::new(fab.declare_publisher(probe_key).wait().expect("probe pub"));
    let _probe_token = fab
        .liveliness()
        .declare_token(probe_key)
        .wait()
        .expect("probe token");
    let _probe_schema = schema_queryable(&fab, probe_key, "e2e.Probe");

    // --- the service Add: a queryable + @service + @schema (two: the request and the response) ---
    let add_key = "reiny/lab/ctrl/Add";
    let _add_token = fab
        .liveliness()
        .declare_token(format!("{add_key}/@service"))
        .wait()
        .expect("service token");
    let _add_schema_req = schema_queryable(&fab, add_key, "e2e.Add");
    let _add_schema_resp = schema_queryable(&fab, add_key, "e2e.Sum");
    let _add_q = fab
        .declare_queryable(add_key)
        .callback(move |q| {
            let Some(payload) = q.payload() else { return };
            let add = Add::decode(payload.to_bytes().as_ref()).expect("decode Add");
            let sum = Sum { sum: add.a + add.b };
            let _ = q.reply(add_key, sum.encode_to_vec()).wait();
        })
        .wait()
        .expect("add queryable");

    // --- keep Probe flowing at 50 Hz (for echo / hz to pick up) ---
    let stop = Arc::new(AtomicBool::new(false));
    let feeder = {
        let stop = Arc::clone(&stop);
        let publisher = Arc::clone(&probe_pub);
        thread::spawn(move || {
            let mut seq = 0u32;
            while !stop.load(Ordering::SeqCst) {
                seq += 1;
                let bytes = Probe {
                    seq,
                    name: "probe".into(),
                }
                .encode_to_vec();
                let _ = publisher
                    .put(bytes)
                    .attachment(fp.to_le_bytes().to_vec())
                    .wait();
                thread::sleep(Duration::from_millis(20));
            }
        })
    };

    // --- playing the launch gui: it only subscribes to Cmd (@sub + @schema; there is no publisher) ---
    let cmd_key = "reiny/lab/gui/Cmd";
    let _cmd_sub_token = fab
        .liveliness()
        .declare_token(format!("{cmd_key}/@sub"))
        .wait()
        .expect("sub token");
    let _cmd_schema = schema_queryable(&fab, cmd_key, "e2e.Sum");
    let cmd_seen = Arc::new(AtomicBool::new(false));
    let _cmd_sub = {
        let seen = Arc::clone(&cmd_seen);
        fab.declare_subscriber("reiny/lab/*/Cmd")
            .callback(move |s| {
                if Sum::decode(s.payload().to_bytes().as_ref()).is_ok_and(|v| v.sum == 7) {
                    seen.store(true, Ordering::SeqCst);
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
        out.contains("pub : Probe") && out.contains("srv : Add"),
        "node info: {out}"
    );
    // A launch that only listens is still a launch, and `sub :` is the line that says so.
    let out = stdout(&reiny(&["node", "info", "gui"]));
    assert!(
        out.contains("sub : Cmd") && out.contains("pub : -"),
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
    // TYPE  PUB  SUB  SRV
    assert_eq!(row("Probe"), ["Probe", "ctrl", "-", "-"], "{out}");
    assert_eq!(row("Add"), ["Add", "-", "-", "ctrl"], "{out}");
    assert_eq!(row("Cmd"), ["Cmd", "-", "gui", "-"], "{out}");

    // --- topic hz: a Hz appears on ctrl's row ---
    let out = stdout(&reiny(&["topic", "hz", "Probe", "--duration", "1.5"]));
    assert!(
        out.contains("ctrl") && out.contains("Hz"),
        "topic hz: {out}"
    );

    // --- topic echo: the JSON decoded through @schema, with its source ---
    let out = stdout(&reiny(&["topic", "echo", "Probe", "--count", "3"]));
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
        out.contains("reiny/lab/sim/Cmd: sent 1") && out.contains("e2e.Sum"),
        "topic pub: {out}"
    );
    for _ in 0..50 {
        if cmd_seen.load(Ordering::SeqCst) {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        cmd_seen.load(Ordering::SeqCst),
        "the subscriber must have received the published Cmd"
    );
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

    stop.store(true, Ordering::SeqCst);
    feeder.join().expect("feeder");
}
