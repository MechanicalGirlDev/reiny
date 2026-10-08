//! Parsed deployment contracts and executable relocation regressions.

#[path = "../src/compress.rs"]
mod compress;

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, ensure};
use reiny_launch::{DeploymentPlan, ModuleManifest, ModuleSource, PortSources, ProviderKind};

fn write(path: &Path, text: &str) -> Result<()> {
    std::fs::create_dir_all(path.parent().context("fixture path has no parent")?)?;
    std::fs::write(path, text)?;
    Ok(())
}

fn manifest(dir: &Path) -> Result<ModuleManifest> {
    Ok(serde_yaml::from_slice(&std::fs::read(
        dir.join("main.yaml"),
    )?)?)
}

fn fixture(root: &Path) -> Result<()> {
    write(
        &root.join("main.yaml"),
        r"
version: 2
deployment: robot
domain: lab
providers:
  host: {type: process, bin_dir: installed, connect: [tcp/127.0.0.1:9999], zenoh_config: transport.json}
  files: {type: artifact}
modules:
  transmitter:
    source: definitions/publisher
    providers: {process: host}
    out: {tick: {type: demo.Tick}}
  arm:
    source: definitions/composite
    providers: {process: host}
    in: {tick: {type: demo.Tick, from: transmitter.tick}}
    out: {echo: {type: demo.Echo}}
out:
  echo: {type: demo.Echo, from: arm.echo}
resources:
  calibration: {type: artifact.file, provider: files, source: assets/calibration.bin}
schema: {types: {unused: test.proto}}
",
    )?;
    write(
        &root.join("definitions/publisher/main.yaml"),
        r"
version: 2
run: {provider: process, bin: worker}
out: {tick: {type: demo.Tick}}
",
    )?;
    write(
        &root.join("definitions/composite/main.yaml"),
        r"
version: 2
in: {tick: {type: demo.Tick}}
modules:
  receiver:
    source: ../subscriber
    in: {tick: {type: demo.Tick, from: in.tick}}
    out: {echo: {type: demo.Echo}}
out: {echo: {type: demo.Echo, from: receiver.echo}}
",
    )?;
    write(
        &root.join("definitions/subscriber/main.yaml"),
        r"
version: 2
run: {provider: process, bin: worker, config: ../../configs/receiver.json, config_assets: [../assets/calibration.bin, main.yaml]}
in: {tick: {type: demo.Tick}}
out: {echo: {type: demo.Echo}}
",
    )?;
    write(&root.join("transport.json"), "{}")?;
    write(
        &root.join("configs/receiver.json"),
        r#"{"asset":"../assets/calibration.bin","ordinary":"not/a/path"}"#,
    )?;
    write(&root.join("assets/calibration.bin"), "calibration")?;
    write(&root.join("configs/main.yaml"), "value: included-config")?;
    write(&root.join(".git/ignored"), "not shipped")?;
    write(&root.join("target/ignored"), "not shipped")?;
    // A real executable can prove relocation without needing a Cargo build.
    std::fs::create_dir_all(root.join("installed"))?;
    std::fs::copy(
        std::env::current_exe()?,
        root.join("installed")
            .join(format!("worker{}", std::env::consts::EXE_SUFFIX)),
    )?;
    write(
        &root.join("installed/fixture.dll"),
        "runtime library fixture",
    )?;
    Ok(())
}

#[test]
fn bundle_preserves_invocations_and_bindings_when_sources_are_relocated() -> Result<()> {
    // Given a composite forwarding a sibling output into its executable child.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    fixture(&root)?;
    let expected = DeploymentPlan::load(&root, false)?;
    let output = workspace.path().join("bundle");

    // When definitions are bundled by invocation namespace instead of source name.
    compress::compress(&root, &output, Some("robot-launcher"), false)?;

    // Then the actual loader sees the same namespace and endpoint contract.
    let actual = DeploymentPlan::load(&output, false)?;
    assert_eq!(actual.domain, "lab");
    assert_eq!(
        actual
            .nodes
            .iter()
            .map(|node| &node.namespace)
            .collect::<Vec<_>>(),
        vec!["robot/arm/receiver", "robot/transmitter"]
    );
    assert_eq!(
        serde_json::to_value(
            expected
                .nodes
                .iter()
                .map(|node| &node.bindings)
                .collect::<Vec<_>>()
        )?,
        serde_json::to_value(
            actual
                .nodes
                .iter()
                .map(|node| &node.bindings)
                .collect::<Vec<_>>()
        )?
    );
    let root_manifest = manifest(&output)?;
    assert_eq!(
        root_manifest.outputs["echo"].from.as_ref(),
        Some(&PortSources::One("arm.echo".into()))
    );
    let ModuleSource::Local(arm_source) = &root_manifest.modules["arm"].source else {
        anyhow::bail!("bundle retained Git source")
    };
    assert_eq!(arm_source, &PathBuf::from("modules/arm"));
    assert_eq!(
        manifest(&output.join(arm_source))?.modules["receiver"].inputs["tick"]
            .from
            .as_ref(),
        Some(&PortSources::One("in.tick".into()))
    );
    assert!(root_manifest.build.is_none() && root_manifest.schema.is_none());
    assert!(
        actual
            .nodes
            .iter()
            .all(|node| node.build.is_none() && node.provider.kind == ProviderKind::Process)
    );
    assert_ne!(
        actual.nodes[0].provider.bin_dir,
        actual.nodes[1].provider.bin_dir
    );
    assert_eq!(actual.nodes[0].provider.connect, vec!["tcp/127.0.0.1:9999"]);
    assert!(!output.join("sources/0/.git").exists());
    assert!(!output.join("sources/0/target").exists());
    Ok(())
}

#[test]
fn bundle_runs_and_retains_config_relative_assets_when_original_tree_is_removed() -> Result<()> {
    // Given real binaries plus an application config with a relative asset reference.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    fixture(&root)?;
    let output = workspace.path().join("bundle");

    // When the bundle is materialized and the entire original checkout disappears.
    compress::compress(&root, &output, None, false)?;
    std::fs::remove_dir_all(&root)?;

    // Then preparation and executable startup use only relocated files.
    let plan = DeploymentPlan::load(&output, false)?;
    let prepared = plan.prepare()?;
    let receiver = prepared
        .nodes
        .iter()
        .find(|node| node.namespace == "robot/arm/receiver")
        .context("receiver missing")?;
    let config = receiver.run.config.as_ref().context("config missing")?;
    ensure!(config.canonicalize()?.starts_with(output.canonicalize()?));
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(config)?)?;
    let asset = config
        .parent()
        .context("config parent missing")?
        .join(value["asset"].as_str().context("asset reference missing")?);
    assert_eq!(std::fs::read_to_string(asset)?, "calibration");
    assert_eq!(value["ordinary"], "not/a/path");
    assert!(
        config
            .parent()
            .context("config parent missing")?
            .join("main.yaml")
            .is_file()
    );
    assert_eq!(
        std::fs::read_to_string(&prepared.resources["robot/calibration"])?,
        "calibration"
    );
    assert_eq!(
        std::fs::read_to_string(
            receiver
                .executable
                .parent()
                .context("executable parent missing")?
                .join("fixture.dll")
        )?,
        "runtime library fixture"
    );
    let status = Command::new(&receiver.executable)
        .args([
            "--exact",
            "compress::tests::system_libraries_stay_out_of_the_bundle",
        ])
        .current_dir(&receiver.module_dir)
        .status()?;
    assert!(status.success());
    Ok(())
}

#[test]
fn bundle_freezes_explicit_external_config_without_a_dangling_path() -> Result<()> {
    // Given an explicitly selected config outside the app source tree.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    fixture(&root)?;
    let external = workspace.path().join("outside.json");
    write(&external, "{}")?;
    let mut leaf = manifest(&root.join("definitions/subscriber"))?;
    let run = leaf.run.as_mut().context("run missing")?;
    run.config = Some(external.clone());
    run.config_assets.clear();
    write(
        &root.join("definitions/subscriber/main.yaml"),
        &serde_yaml::to_string(&leaf)?,
    )?;

    // When the snapshot is bundled and both original sources disappear.
    let output = workspace.path().join("bundle");
    compress::compress(&root, &output, None, false)?;
    std::fs::remove_dir_all(root)?;
    std::fs::remove_file(external)?;

    // Then the selected config can be prepared entirely from distribution files.
    let prepared = DeploymentPlan::load(&output, false)?.prepare()?;
    let config = prepared.nodes[0]
        .run
        .config
        .as_ref()
        .context("config missing")?;
    assert_eq!(std::fs::read_to_string(config)?, "{}");
    Ok(())
}

#[test]
fn bundle_does_not_overwrite_existing_output() -> Result<()> {
    // Given a nonempty output directory.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    fixture(&root)?;
    let output = workspace.path().join("bundle");
    write(&output.join("keep"), "unchanged")?;

    // When bundling targets that directory.
    let result = compress::compress(&root, &output, None, false);

    // Then unrelated content remains intact.
    assert!(result.is_err());
    assert_eq!(std::fs::read_to_string(output.join("keep"))?, "unchanged");
    Ok(())
}

#[test]
fn bundle_excludes_its_output_when_destination_is_inside_the_source_tree() -> Result<()> {
    // Given a destination nested under the deployment source.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    fixture(&root)?;
    let output = root.join("distribution/bundle");

    // When the source tree is copied into the destination.
    compress::compress(&root, &output, None, false)?;

    // Then no recursive bundle copy is present and the root contract still loads.
    assert!(!output.join("sources/0/distribution/bundle").exists());
    assert_eq!(DeploymentPlan::load(&output, false)?.nodes.len(), 2);
    Ok(())
}

#[test]
fn bundle_manifest_contracts_are_deterministic_when_materialized_twice() -> Result<()> {
    // Given one unchanged deployment with compiler-independent executable fixtures.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    fixture(&root)?;
    let first = workspace.path().join("first");
    let second = workspace.path().join("second");

    // When identical inputs are materialized into different output directories.
    compress::compress(&root, &first, None, false)?;
    compress::compress(&root, &second, None, false)?;

    // Then all machine-consumed manifest bytes match.
    for instance in [
        "",
        "modules/arm",
        "modules/arm/receiver",
        "modules/transmitter",
    ] {
        assert_eq!(
            std::fs::read(first.join(instance).join("main.yaml"))?,
            std::fs::read(second.join(instance).join("main.yaml"))?
        );
    }
    Ok(())
}

#[test]
fn bundle_keeps_distinct_builds_when_leaves_use_the_same_binary_name() -> Result<()> {
    // Given independent Cargo packages exporting the same executable name.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    write(
        &root.join("main.yaml"),
        r"
version: 2
deployment: twins
providers: {process: {type: process}}
modules:
  first: {source: definitions/first}
  second: {source: definitions/second}
",
    )?;
    for name in ["first", "second"] {
        let dir = root.join("definitions").join(name);
        write(
            &dir.join("main.yaml"),
            r"
version: 2
run: {provider: process, bin: worker}
build: {type: cargo, profile: dev, locked: false}
schema: {types: {unused: test.proto}}
",
        )?;
        write(
            &dir.join("Cargo.toml"),
            &format!(
                "[workspace]\n[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[[bin]]\nname = \"worker\"\npath = \"src/main.rs\"\n"
            ),
        )?;
        write(
            &dir.join("src/main.rs"),
            &format!("fn main() {{ println!(\"{name}\"); }}\n"),
        )?;
    }
    let output = workspace.path().join("bundle");

    // When compiler artifacts are bundled and their original projects disappear.
    compress::compress(&root, &output, None, false)?;
    std::fs::remove_dir_all(root)?;

    // Then both runtime manifests select their own binary without Cargo sources.
    let plan = DeploymentPlan::load(&output, false)?;
    assert!(plan.nodes.iter().all(|node| node.build.is_none()));
    for node in plan.prepare()?.nodes {
        let expected = node.namespace.strip_prefix("twins/").context("namespace")?;
        let result = Command::new(&node.executable).output()?;
        assert!(result.status.success());
        assert_eq!(String::from_utf8(result.stdout)?.trim(), expected);
        let parsed = manifest(&node.module_dir)?;
        assert!(parsed.build.is_none() && parsed.schema.is_none());
    }
    Ok(())
}

#[test]
fn bundle_relocates_acquired_checkout_assets_when_cache_tree_is_removed() -> Result<()> {
    // Given a resolved module definition under the Git acquisition cache layout.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    fixture(&root)?;
    let checkout = root.join(".reiny/cache/git/checkouts/revision");
    let mut root_manifest = manifest(&root)?;
    root_manifest
        .modules
        .get_mut("transmitter")
        .context("transmitter")?
        .source = ModuleSource::Local(PathBuf::from(".reiny/cache/git/checkouts/revision/module"));
    write(
        &root.join("main.yaml"),
        &serde_yaml::to_string(&root_manifest)?,
    )?;
    write(
        &checkout.join("module/main.yaml"),
        r"
version: 2
run: {provider: process, bin: worker, config: ../configs/app.json, config_assets: [../assets/data]}
out: {tick: {type: demo.Tick}}
",
    )?;
    write(
        &checkout.join("configs/app.json"),
        r#"{"asset":"../assets/data"}"#,
    )?;
    write(&checkout.join("assets/data"), "checkout asset")?;
    write(&checkout.join(".git/ignored"), "repository state")?;
    let output = workspace.path().join("bundle");

    // When the acquired source is bundled and the acquisition cache disappears.
    compress::compress(&root, &output, None, false)?;
    std::fs::remove_dir_all(root)?;

    // Then the relocated config retains its checkout-relative asset tree.
    let plan = DeploymentPlan::load(&output, false)?;
    let transmitter = plan
        .nodes
        .iter()
        .find(|node| node.namespace == "robot/transmitter")
        .context("transmitter missing")?;
    let config = transmitter.run.config.as_ref().context("config missing")?;
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(config)?)?;
    let asset = config
        .parent()
        .context("config parent")?
        .join(value["asset"].as_str().context("asset reference")?);
    assert_eq!(std::fs::read_to_string(asset)?, "checkout asset");
    assert!(
        !config
            .parent()
            .context("config parent")?
            .join("../.git")
            .exists()
    );
    Ok(())
}

#[cfg(windows)]
#[test]
fn bundle_loads_native_runtime_libraries_when_original_binary_directory_is_removed() -> Result<()> {
    // Given a real executable dynamically linked to the compiler's native std DLL.
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    let project = root.join("project");
    write(
        &project.join("Cargo.toml"),
        "[workspace]\n[package]\nname = \"worker\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
    )?;
    write(
        &project.join("src/main.rs"),
        "fn main() { println!(\"native runtime loaded\"); }\n",
    )?;
    let status = Command::new("cargo")
        .current_dir(&project)
        .args(["build", "--quiet"])
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env("RUSTFLAGS", "-C prefer-dynamic")
        .status()?;
    ensure!(
        status.success(),
        "building dynamically linked fixture failed"
    );
    let libraries = Command::new("rustc")
        .current_dir(&project)
        .args(["--print", "target-libdir"])
        .output()?;
    ensure!(
        libraries.status.success(),
        "resolving native standard library directory failed"
    );
    let lib_dir = PathBuf::from(String::from_utf8(libraries.stdout)?.trim());
    let installed = root.join("installed");
    std::fs::create_dir_all(&installed)?;
    std::fs::copy(
        project.join("target/debug/worker.exe"),
        installed.join("worker.exe"),
    )?;
    let mut runtime_names = Vec::new();
    for entry in std::fs::read_dir(lib_dir)? {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "dll")
        {
            std::fs::copy(entry.path(), installed.join(entry.file_name()))?;
            runtime_names.push(entry.file_name());
        }
    }
    ensure!(
        !runtime_names.is_empty(),
        "compiler shipped no native runtime DLL"
    );
    write(
        &root.join("main.yaml"),
        "version: 2\ndeployment: native\nproviders: {process: {type: process, bin_dir: installed}}\nrun: {provider: process, bin: worker}\n",
    )?;
    let output = workspace.path().join("bundle");

    // When the executable and DLLs are bundled and their original directory disappears.
    compress::compress(&root, &output, None, false)?;
    std::fs::remove_dir_all(root)?;

    // Then Windows loads the DLL beside the immutable staged executable, without PATH.
    let prepared = DeploymentPlan::load(&output, false)?.prepare()?;
    let node = prepared
        .nodes
        .first()
        .context("native executable missing")?;
    for name in runtime_names {
        assert!(
            node.executable
                .parent()
                .context("executable parent")?
                .join(name)
                .is_file()
        );
    }
    let result = Command::new(&node.executable).env("PATH", "").output()?;
    assert!(result.status.success());
    assert_eq!(
        String::from_utf8(result.stdout)?.trim(),
        "native runtime loaded"
    );
    Ok(())
}
