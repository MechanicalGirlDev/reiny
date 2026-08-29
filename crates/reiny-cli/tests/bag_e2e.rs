//! `reiny bag` の通し試験 —— 本物の zenoh fabric に対して、ビルド済み `reiny` バイナリで
//! record → info → play を回す。
//!
//! reiny-cli は bin 専用クレート(lib ターゲットが無い)なので、`bagcmd` を直接呼べない。
//! そこで `CARGO_BIN_EXE_reiny` の実体をサブプロセスで起動し、テスト側は「launch 役」の
//! zenoh セッションを 1 本張って publisher / latched / presence を演じる —— reiny の e2e と
//! 同じく、ループバック TCP 固定ポート・マルチキャスト off で決定的に繋ぐ。
//!
//! ペイロードは bag にとって不透明なバイト列なので、prost も proto も要らない。

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

use reiny::zenoh::{self, Wait};

/// この試験専用のループバックポート(reiny の e2e が使う 37447 とずらす)。
const ENDPOINT: &str = "tcp/127.0.0.1:37448";
const BIN: &str = env!("CARGO_BIN_EXE_reiny");

/// 記録対象の型の指紋(実機の attachment を手で再現)。
const PROBE_FP: u64 = 0xA1B2_C3D4_E5F6_0718;

/// launch 役: この 1 本が ENDPOINT を listen し、他(record / play のサブプロセス)は client で繋ぐ。
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

/// サブプロセスの `reiny bag` に共通の fabric 引数(client で listener へ繋ぐ)。
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

    // --- launch 役の口を用意する ---
    // 1) ライブの型 ctrl/Probe: publisher + liveliness トークン(presence と guard 用)。
    let probe_key = "reiny/lab/ctrl/Probe";
    let probe_pub = fab.declare_publisher(probe_key).wait().expect("probe pub");
    let _probe_token = fab
        .liveliness()
        .declare_token(probe_key)
        .wait()
        .expect("probe token");
    // 2) latched の cfg/Config: 直近値を返す queryable(record の snapshot が拾う)。
    let cfg_key = "reiny/lab/cfg/Config";
    let _cfg_q = fab
        .declare_queryable(cfg_key)
        .callback(move |query| {
            let _ = query.reply(cfg_key, b"snapshot-value".to_vec()).wait();
        })
        .wait()
        .expect("cfg queryable");

    // --- record をサブプロセスで起動(1.5s) ---
    let mut record = Command::new(BIN)
        .arg("bag")
        .arg("record")
        .args(bus_args("lab"))
        .args(["--duration", "1.5"])
        .arg("--out")
        .arg(&bag)
        .spawn()
        .expect("spawn record");

    // record が接続・購読・snapshot を済ませるまで待ってから、ライブを流す。
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

    // --- info: snapshot の cfg と、ライブの ctrl が両方見える ---
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

    // --- play: 別 domain へ、送信元を bag に書き換えて流し、テスト側で拾う ---
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

    // 収集: Probe が送信元 bag で戻り、指紋も復元されている。cfg(latched)は空でも可。
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

    // --- 安全弁: ライブの ctrl/Probe が居る domain lab へ --force 無しで再生 → 拒否 ---
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

    // --force を付ければ通る。
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
