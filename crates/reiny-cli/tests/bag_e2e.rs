//! The end-to-end test of `reiny bag` — record → info → play against a real zenoh fabric, driven
//! through the built `reiny` binary.
//!
//! reiny-cli is a bin-only crate (it has no lib target), so `bagcmd` cannot be called directly.
//! Instead the real `CARGO_BIN_EXE_reiny` is started as a subprocess while the test side holds one
//! zenoh session "playing a launch", acting out the publisher / latched / presence parts — wired
//! deterministically over a fixed loopback TCP port with multicast off, as reiny's own e2e is.
//!
//! The payloads are opaque bytes as far as a bag is concerned, so neither prost nor a proto is needed.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

use reiny::zenoh::{self, Wait};

/// The loopback port for this test alone (kept clear of the 37447 reiny's e2e uses).
const ENDPOINT: &str = "tcp/127.0.0.1:37448";
const BIN: &str = env!("CARGO_BIN_EXE_reiny");

/// The fingerprint of the recorded type (a real attachment, recreated by hand).
const PROBE_FP: u64 = 0xA1B2_C3D4_E5F6_0718;

/// Playing a launch: this one session listens on ENDPOINT, and the others (the record / play subprocesses) connect as clients.
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

/// The fabric arguments every `reiny bag` subprocess shares (a client connecting to the listener).
fn bus_args(domain: &str) -> Vec<String> {
    vec![
        "--domain".into(),
        domain.into(),
        "--connect".into(),
        ENDPOINT.into(),
        "--zenoh-mode".into(),
        "client".into(),
    ]
}

#[test]
fn record_info_play_round_trip_and_live_guard() {
    let bag = std::env::temp_dir().join(format!("reiny-bag-e2e-{}.mcap", std::process::id()));
    let _ = std::fs::remove_file(&bag);

    let fab = fabric();

    // --- set up what the launch offers ---
    // 1) The live type ctrl/Probe: a publisher plus a liveliness token (for presence and the safety catch).
    let probe_key = "reiny/lab/ctrl/Probe";
    let probe_pub = fab.declare_publisher(probe_key).wait().expect("probe pub");
    let _probe_token = fab
        .liveliness()
        .declare_token(probe_key)
        .wait()
        .expect("probe token");
    // 2) The latched cfg/Config: a queryable answering with the most recent value (record's snapshot picks it up).
    let cfg_key = "reiny/lab/cfg/Config";
    let _cfg_q = fab
        .declare_queryable(cfg_key)
        .callback(move |query| {
            let _ = query.reply(cfg_key, b"snapshot-value".to_vec()).wait();
        })
        .wait()
        .expect("cfg queryable");

    // --- start record as a subprocess (1.5 s) ---
    let mut record = Command::new(BIN)
        .arg("bag")
        .arg("record")
        .args(bus_args("lab"))
        .args(["--duration", "1.5"])
        .arg("--out")
        .arg(&bag)
        .spawn()
        .expect("spawn record");

    // Wait until record has connected, subscribed and taken its snapshot before publishing anything live.
    sleep(Duration::from_millis(500));
    for seq in 0u32..5 {
        fab.put(probe_key, seq.to_le_bytes().to_vec())
            .attachment(PROBE_FP.to_le_bytes().to_vec())
            .wait()
            .expect("put probe");
        sleep(Duration::from_millis(60));
    }

    let status = record.wait().expect("record wait");
    assert!(status.success(), "record exited with {status:?}");
    assert!(bag.is_file(), "bag file not written");

    // --- info: the snapshotted cfg and the live ctrl are both visible ---
    let info = Command::new(BIN)
        .args(["bag", "info"])
        .arg(&bag)
        .output()
        .expect("info");
    let info_out = String::from_utf8_lossy(&info.stdout);
    assert!(info.status.success(), "info failed: {info_out}");
    assert!(info_out.contains("ctrl"), "info lacks ctrl: {info_out}");
    assert!(info_out.contains("Probe"), "info lacks Probe: {info_out}");
    assert!(
        info_out.contains("cfg") && info_out.contains("latched"),
        "info lacks the latched cfg row: {info_out}"
    );
    assert!(
        info_out.contains(&format!("{PROBE_FP:016x}")),
        "info lacks the Probe fingerprint: {info_out}"
    );

    // --- play: into another domain, with the source rewritten to bag, and picked up on the test side ---
    let collected = fab
        .declare_subscriber("reiny/replay/*/*")
        .wait()
        .expect("replay sub");
    let play = Command::new(BIN)
        .args(["bag", "play"])
        .arg(&bag)
        .args(bus_args("replay"))
        .args(["--as", "bag", "--rate", "8"])
        .output()
        .expect("play");
    assert!(
        play.status.success(),
        "play failed: {}",
        String::from_utf8_lossy(&play.stderr)
    );

    // Collect: Probe comes back from the source bag with its fingerprint restored. cfg (latched) may be empty.
    let mut probe_values = Vec::new();
    let mut saw_fp = false;
    while let Ok(Some(sample)) = collected.recv_timeout(Duration::from_millis(500)) {
        let key = sample.key_expr().as_str().to_string();
        if key == "reiny/replay/bag/Probe" {
            let raw: [u8; 4] = sample.payload().to_bytes().as_ref().try_into().unwrap();
            probe_values.push(u32::from_le_bytes(raw));
            if let Some(att) = sample.attachment()
                && let Ok(raw) = <[u8; 8]>::try_from(att.to_bytes().as_ref())
                && u64::from_le_bytes(raw) == PROBE_FP
            {
                saw_fp = true;
            }
        }
    }
    probe_values.sort_unstable();
    assert_eq!(probe_values, [0, 1, 2, 3, 4], "replayed Probe values");
    assert!(saw_fp, "fingerprint attachment was not restored on replay");

    // --- the safety catch: replaying into domain lab, where a live ctrl/Probe is, without --force → refused ---
    let guarded = Command::new(BIN)
        .args(["bag", "play"])
        .arg(&bag)
        .args(bus_args("lab"))
        .output()
        .expect("guarded play");
    assert!(
        !guarded.status.success(),
        "replay into a live domain must be refused without --force"
    );
    let stderr = String::from_utf8_lossy(&guarded.stderr);
    assert!(
        stderr.contains("live publisher"),
        "guard message missing: {stderr}"
    );

    // With --force it goes through.
    let forced = Command::new(BIN)
        .args(["bag", "play"])
        .arg(&bag)
        .args(bus_args("lab"))
        .args(["--force", "--rate", "8"])
        .output()
        .expect("forced play");
    assert!(
        forced.status.success(),
        "forced replay failed: {}",
        String::from_utf8_lossy(&forced.stderr)
    );

    drop(probe_pub);
    let _ = std::fs::remove_file(&bag);
}
