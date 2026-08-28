//! `reiny topic / node / service` の通し試験 —— 本物の zenoh fabric に対して、ビルド済み
//! `reiny` バイナリで list / hz / echo / node / service call を回す。
//!
//! `bag_e2e` と同じ流儀: テスト側が「grain 役」の zenoh セッションを 1 本張り、publisher /
//! presence / `@schema` / service の queryable を素の zenoh で演じる。descriptor は
//! `prost-types` で手組みする(protoc 無し)。ループバック TCP 固定ポート・マルチキャスト off。

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

/// この試験専用のループバックポート(e2e 37447 / `bag_e2e` 37448 / `rpc_e2e` 37449 の次)。
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
/// message Sum {int32 sum=1;}` を 1 ファイルに持つ descriptor set。
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

/// `<key>/@schema/<fqn>` で descriptor set を名乗る queryable。
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
#[allow(clippy::too_many_lines)] // 1 本で通す(ポートを増やさない)ので長い。
fn topic_node_service_against_live_grain() {
    let fab = fabric();
    let fp = reiny_build::message_fingerprint(&file_set(), "e2e.Probe")
        .unwrap()
        .unwrap();

    // --- grain 役 ctrl: @grain、Probe の publisher + presence + @schema ---
    let _grain = fab
        .liveliness()
        .declare_token("reiny/lab/ctrl/@grain")
        .wait()
        .expect("grain token");
    let probe_key = "reiny/lab/ctrl/Probe";
    let probe_pub = Arc::new(fab.declare_publisher(probe_key).wait().expect("probe pub"));
    let _probe_token = fab
        .liveliness()
        .declare_token(probe_key)
        .wait()
        .expect("probe token");
    let _probe_schema = schema_queryable(&fab, probe_key, "e2e.Probe");

    // --- service Add: queryable + @service + @schema(request / response の 2 本) ---
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

    // --- Probe を 50 Hz で流し続ける(echo / hz が拾う) ---
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

    // --- node list / info ---
    let out = stdout(&reiny(&["node", "list"]));
    assert!(out.lines().any(|l| l.trim() == "ctrl"), "node list: {out}");
    let out = stdout(&reiny(&["node", "info", "ctrl"]));
    assert!(
        out.contains("pub : Probe") && out.contains("srv : Add"),
        "node info: {out}"
    );

    // --- topic list: Probe は [pub ctrl]、Add は [srv ctrl] ---
    let out = stdout(&reiny(&["topic", "list"]));
    let probe_row = out
        .lines()
        .find(|l| l.starts_with("Probe"))
        .expect("Probe row");
    assert!(probe_row.contains("ctrl"), "topic list: {out}");
    let add_row = out.lines().find(|l| l.starts_with("Add")).expect("Add row");
    assert!(
        add_row.contains('-') && add_row.ends_with("ctrl"),
        "Add should be srv only: {add_row}"
    );

    // --- topic hz: ctrl の行に Hz が出る ---
    let out = stdout(&reiny(&["topic", "hz", "Probe", "--duration", "1.5"]));
    assert!(
        out.contains("ctrl") && out.contains("Hz"),
        "topic hz: {out}"
    );

    // --- topic echo: @schema で decode した JSON が source 付きで出る ---
    let out = stdout(&reiny(&["topic", "echo", "Probe", "--count", "3"]));
    let lines: Vec<&str> = out.lines().filter(|l| l.starts_with("ctrl")).collect();
    assert_eq!(lines.len(), 3, "topic echo: {out}");
    assert!(
        lines
            .iter()
            .all(|l| l.contains(r#""name":"probe""#) && l.contains(r#""seq":"#)),
        "topic echo JSON: {out}"
    );
    // --raw は hex。
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

    stop.store(true, Ordering::SeqCst);
    feeder.join().expect("feeder");
}
