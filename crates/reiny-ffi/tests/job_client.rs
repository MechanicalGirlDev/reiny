//! Real standalone foreign jobs against the core-backed C ABI.
#![cfg(target_os = "linux")]
#![cfg(not(all(miri, feature = "component-ffi-skip-miri")))]
use std::process::Command;

#[test]
fn c_owned_jobs_preserve_capacity_cancellation_and_native_teardown() -> anyhow::Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let exe = std::env::current_exe()?;
    let library = std::env::var_os("REINY_FFI_TEST_LIBRARY")
        .map_or_else(
            || exe.parent().map(|p| p.join("libreiny_ffi.so")),
            |p| Some(p.into()),
        )
        .ok_or_else(|| anyhow::anyhow!("test executable has no parent"))?;
    let temporary = tempfile::tempdir()?;
    let client = temporary.path().join("job-client");
    let compile = Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pthread"])
        .arg("-I")
        .arg(root.join("include"))
        .arg(root.join("tests/job_client.c"))
        .arg(library)
        .arg("-o")
        .arg(&client)
        .output()?;
    anyhow::ensure!(
        compile.status.success(),
        "C job compile failed: {}",
        String::from_utf8_lossy(&compile.stderr)
    );
    let output = Command::new("timeout").arg("60").arg(client).output()?;
    anyhow::ensure!(
        output.status.success(),
        "C jobs failed ({}):\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
