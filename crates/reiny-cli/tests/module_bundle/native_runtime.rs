//! Native library search paths must survive source-independent bundling.
use super::write;
use anyhow::{Context, Result, ensure};
use reiny_launch::{DeploymentPlan, PreparedDeployment};
use std::path::{Path, PathBuf};
use std::process::Command;

const BUILD_SCRIPT: &str = r#"
use std::{env, fs, path::PathBuf, process::Command};
fn main() {
    let package = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let lib = package.join("native/lib");
    let bin = package.join("native/bin");
    fs::create_dir_all(&lib).unwrap();
    fs::create_dir_all(&bin).unwrap();
    let status = Command::new(env::var_os("RUSTC").unwrap())
        .args(["--crate-type", "cdylib", "--crate-name", "fixture_runtime", "--edition", "2024"])
        .arg(package.join("native.rs"))
        .arg("-C").arg(format!("link-arg=/IMPLIB:{}", lib.join("fixture_runtime.lib").display()))
        .arg("-o").arg(bin.join("fixture_runtime.dll"))
        .status().unwrap();
    assert!(status.success(), "native DLL compilation failed");
    let search = if env::var("FIXTURE_RELATIVE_LINK").as_deref() == Ok("1") {
        "native=native/lib".to_owned()
    } else {
        lib.display().to_string()
    };
    println!("cargo:rustc-link-search={search}");
    println!("cargo:rustc-link-lib=dylib=fixture_runtime");
    println!("cargo:rerun-if-changed=native.rs");
}
"#;

fn fixture(source: &Path, relative: bool) -> Result<PathBuf> {
    let package = source.join("package");
    write(
        &package.join("Cargo.toml"),
        "[workspace]\n[package]\nname = 'native-runtime-fixture'\nversion = '0.0.0'\nedition = '2024'\n[[bin]]\nname = 'worker'\npath = 'src/main.rs'\n",
    )?;
    write(
        &package.join("Cargo.lock"),
        "version = 4\n[[package]]\nname = 'native-runtime-fixture'\nversion = '0.0.0'\n",
    )?;
    write(&package.join("build.rs"), BUILD_SCRIPT)?;
    write(
        &package.join("native.rs"),
        "#[unsafe(no_mangle)] pub extern \"C\" fn fixture_value() -> u32 { 73 }\n",
    )?;
    write(
        &package.join("src/main.rs"),
        "unsafe extern \"C\" { fn fixture_value() -> u32; }\nfn main() { println!(\"native value {}\", unsafe { fixture_value() }); }\n",
    )?;
    let launch = source.join("launch");
    std::fs::create_dir_all(source.join("invocation"))?;
    write(
        &launch.join("main.yaml"),
        &format!(
            "version: 3\ndeployment: native-library\nproviders: {{process: {{type: process}}}}\nrun: {{provider: process, bin: worker}}\nbuild:\n  type: cargo\n  manifest: ../package/Cargo.toml\n  cwd: ../invocation\n  profile: dev\n  env: {{FIXTURE_RELATIVE_LINK: '{}'}}\n",
            u8::from(relative),
        ),
    )?;
    Ok(launch)
}

#[test]
fn cargo_linked_native_runtime_survives_source_and_cache_removal() -> Result<()> {
    for relative in [false, true] {
        let workspace = tempfile::tempdir()?;
        let source = workspace.path().join("source");
        let launch = fixture(&source, relative)?;
        let cache = workspace.path().join("compiler-cache");
        let output = workspace.path().join("bundle");
        let bundled = Command::new(env!("CARGO_BIN_EXE_reiny"))
            .arg("bundle")
            .arg(&launch)
            .arg("--out")
            .arg(&output)
            .env("CARGO_TARGET_DIR", &cache)
            .output()?;
        ensure!(
            bundled.status.success(),
            "native bundle failed: {}",
            String::from_utf8_lossy(&bundled.stderr)
        );
        let relocated = workspace.path().join("relocated");
        std::fs::rename(output, &relocated)?;
        std::fs::remove_dir_all(&source)?;
        std::fs::remove_dir_all(&cache)?;
        let plan = DeploymentPlan::load(&relocated, false)?;
        let node = plan.nodes.first().context("bundled native app")?;
        assert!(node.build.is_none());
        let library = node.provider.bin_dir.join("fixture_runtime.dll");
        let inventory: serde_json::Value = serde_json::from_slice(&std::fs::read(
            node.provider.bin_dir.join(".reiny-bundle.json"),
        )?)?;
        assert!(
            inventory["files"]
                .as_array()
                .context("runtime inventory")?
                .iter()
                .any(|file| file == "fixture_runtime.dll")
        );
        let bytes = std::fs::read(&library)?;
        let prepared_path = workspace.path().join("prepared.json");
        let cli = relocated.join("reiny.exe");
        let prepared = Command::new(&cli)
            .arg("prepare")
            .arg(&relocated)
            .arg("--out")
            .arg(&prepared_path)
            .env("PATH", "")
            .env("CARGO", workspace.path().join("cargo-unavailable"))
            .current_dir(workspace.path())
            .output()?;
        ensure!(
            prepared.status.success(),
            "{}",
            String::from_utf8_lossy(&prepared.stderr)
        );
        let prepared = PreparedDeployment::load_file(&prepared_path)?;
        prepared.verify()?;
        let app = prepared.nodes.first().context("prepared native app")?;
        assert_eq!(
            std::fs::read(app.bundle_dir.join("fixture_runtime.dll"))?,
            bytes
        );
        let result = Command::new(&app.executable)
            .env("PATH", "")
            .current_dir(workspace.path())
            .output()?;
        ensure!(
            result.status.success(),
            "native DLL app failed: {}",
            result.status
        );
        assert_eq!(String::from_utf8(result.stdout)?.trim(), "native value 73");
        std::fs::remove_file(library)?;
        let missing = workspace.path().join("missing.json");
        let failure = Command::new(&cli)
            .arg("prepare")
            .arg(&relocated)
            .arg("--out")
            .arg(&missing)
            .env("PATH", "")
            .env("CARGO", workspace.path().join("cargo-unavailable"))
            .current_dir(workspace.path())
            .output()?;
        assert_eq!(failure.status.code(), Some(1));
        assert!(!missing.exists());
        prepared.verify()?;
        println!(
            "native DLL relocation: relative={relative}, value=73, source/cache removed, missing inventory file rejected"
        );
    }
    Ok(())
}
