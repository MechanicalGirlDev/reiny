use super::*;

#[test]
fn app_config_bundle_uses_prepared_tree_outside_the_cache() -> Result<()> {
    let workspace = tempfile::tempdir()?;
    let root = workspace.path().join("original");
    std::fs::create_dir_all(root.join("configs"))?;
    std::fs::create_dir_all(root.join("assets"))?;
    std::fs::create_dir_all(root.join("installed"))?;
    let manifest = "version: 3\ndeployment: robot\nproviders:\n  process: {type: process, bin_dir: installed}\nrun:\n  provider: process\n  bin: worker\n  config: configs/app.json\n  config_assets: [../assets/value.txt, include.json]\n  args: [instance-setting]\n";
    std::fs::write(root.join("main.yaml"), manifest)?;
    std::fs::write(
        root.join("configs/app.json"),
        r#"{"include":"include.json","asset":"../assets/value.txt"}"#,
    )?;
    std::fs::write(root.join("configs/include.json"), "{}")?;
    std::fs::write(root.join("assets/value.txt"), "frozen asset")?;
    std::fs::copy(
        std::env::current_exe()?,
        root.join("installed")
            .join(format!("worker{}", std::env::consts::EXE_SUFFIX)),
    )?;
    let plan = DeploymentPlan::load(&root, false)?;
    let mut prepared = plan.prepare()?;
    let node = &mut prepared.nodes[0];
    let source = node
        .config_bundle_dir
        .as_ref()
        .context("config bundle missing")?;
    let entry = node
        .run
        .config
        .as_ref()
        .context("config missing")?
        .strip_prefix(source)?
        .to_owned();
    // Keep the frozen address, but move the tree away from the cache layout.
    let relocated = workspace
        .path()
        .join("frozen")
        .join(source.file_name().context("config address missing")?);
    std::fs::create_dir_all(relocated.parent().context("frozen parent missing")?)?;
    std::fs::rename(source, &relocated)?;
    node.run.config = Some(relocated.join(entry));
    node.config_bundle_dir = Some(relocated);

    let output = workspace.path().join("bundle");
    std::fs::create_dir_all(&output)?;
    let origins = source_origins(&plan)?;
    let mut manifests =
        BTreeMap::from([(plan.deployment.clone(), serde_yaml::from_str(manifest)?)]);
    let providers = bundle_nodes(&plan, &prepared, &output, &origins, &mut manifests, false)?;
    write_manifests(&plan, &output, &origins, &mut manifests, &providers)?;
    finalize_bundle(&plan, &output, "reiny", false)?;
    std::fs::remove_dir_all(root)?;
    std::fs::remove_dir_all(workspace.path().join("frozen"))?;

    let bundled = DeploymentPlan::load(&output, false)?.prepare()?;
    assert_eq!(bundled.nodes[0].bindings, prepared.nodes[0].bindings);
    assert_eq!(bundled.nodes[0].run.args, vec!["instance-setting"]);
    let config = bundled.nodes[0]
        .run
        .config
        .as_ref()
        .context("bundled config missing")?;
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(config)?)?;
    let parent = config.parent().context("bundled config parent missing")?;
    assert_eq!(
        std::fs::read_to_string(
            parent.join(value["include"].as_str().context("include missing")?)
        )?,
        "{}"
    );
    assert_eq!(
        std::fs::read_to_string(parent.join(value["asset"].as_str().context("asset missing")?))?,
        "frozen asset"
    );
    Ok(())
}

#[test]
fn system_libraries_stay_out_of_the_bundle() {
    for system in [
        "/lib/x86_64-linux-gnu/libc.so.6",
        "/lib64/ld-linux-x86-64.so.2",
        "/usr/lib/x86_64-linux-gnu/libstdc++.so.6",
        "/usr/lib64/libm.so.6",
        "linux-vdso.so.1",
    ] {
        assert!(is_system_lib(system), "{system} should be a system library");
    }
    for ours in [
        "/home/nop/dev/robot/target/release/libmylib.so",
        "/opt/robot/lib/libdriver.so",
        "./libplugin.so",
    ] {
        assert!(!is_system_lib(ours), "{ours} has to be bundled");
    }
    assert!(!is_system_lib("/home/nop/usr/lib/libmine.so"));
}
