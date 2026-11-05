//! Real C ABI acceptance on Linux, with a bounded subprocess and private outputs.
#![cfg(target_os = "linux")]
#![cfg(not(all(miri, feature = "component-ffi-skip-miri")))]

use std::process::Command;

#[test]
fn c_service_callbacks_use_real_named_ports_and_survive_cancelled_waiters() -> anyhow::Result<()> {
    let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let executable = std::env::current_exe()?;
    let library_dir = executable
        .parent()
        .ok_or_else(|| anyhow::anyhow!("test executable has no parent directory"))?;
    let temporary = tempfile::tempdir()?;
    let client = temporary.path().join("component-client");
    let compiler = Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pthread"])
        .arg("-I")
        .arg(crate_dir.join("include"))
        .arg(crate_dir.join("tests/component_client.c"))
        // Link the exact feature-matched cdylib, not a stale target/debug copy.
        .arg(library_dir.join("libreiny_ffi.so"))
        .arg("-o")
        .arg(&client)
        .output()?;
    anyhow::ensure!(
        compiler.status.success(),
        "C client compilation failed:\n{}",
        String::from_utf8_lossy(&compiler.stderr)
    );
    let output = Command::new("timeout").arg("30").arg(client).output()?;
    anyhow::ensure!(
        output.status.success(),
        "C client failed ({}):\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
