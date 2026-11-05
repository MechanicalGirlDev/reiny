//! Native C tests of the additive managed-step ABI.
#![cfg(target_os = "linux")]
#![cfg(not(all(miri, feature = "component-ffi-skip-miri")))]

use std::process::Command;

#[test]
fn c_managed_steps_preserve_values_clocks_and_callback_quiescence() -> anyhow::Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let exe = std::env::current_exe()?;
    let library = std::env::var_os("REINY_FFI_TEST_LIBRARY")
        .map_or_else(
            || exe.parent().map(|p| p.join("libreiny_ffi.so")),
            |p| Some(p.into()),
        )
        .ok_or_else(|| anyhow::anyhow!("test executable has no parent"))?;
    let temporary = tempfile::tempdir()?;
    let client = temporary.path().join("component-step");
    let compile = Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pthread"])
        .arg("-I")
        .arg(root.join("include"))
        .arg(root.join("tests/component_step.c"))
        .arg(library)
        .arg("-o")
        .arg(&client)
        .output()?;
    anyhow::ensure!(
        compile.status.success(),
        "C compile failed: {}",
        String::from_utf8_lossy(&compile.stderr)
    );
    let output = Command::new("timeout").arg("60").arg(client).output()?;
    anyhow::ensure!(
        output.status.success(),
        "C steps failed ({}):\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
