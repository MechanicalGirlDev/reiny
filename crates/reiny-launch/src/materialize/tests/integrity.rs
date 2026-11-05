use super::{Context, PreparedDeployment, Result, fixture, fs, stage, write};

#[test]
fn hosted_bundles_reject_changed_missing_and_extra_files() -> Result<()> {
    for change in ["change", "remove", "extra"] {
        let (root, plan) = fixture()?;
        let prepared = stage(&plan)?;
        let path = root.path().join("prepared.json");
        prepared.write_file(&path)?;
        let host = &prepared.component_hosts["control"];
        let component = &prepared.components[0];
        let files = [
            host.executable.clone(),
            host.bundle_dir.join("runtime.dll"),
            host.provider.zenoh_config.clone().context("transport")?,
            component.component.config.clone().context("config")?,
            component
                .config_dir
                .as_ref()
                .context("origin")?
                .join("../assets/table.bin"),
        ];
        for file in files {
            let original = fs::read(&file)?;
            let extra = file.parent().context("parent")?.join("extra.bin");
            match change {
                "change" => fs::write(&file, "changed")?,
                "remove" => fs::remove_file(&file)?,
                "extra" => fs::write(&extra, "extra")?,
                _ => unreachable!(),
            }

            assert!(
                PreparedDeployment::load_file(&path).is_err(),
                "{change}: {}",
                file.display()
            );

            match change {
                "change" | "remove" => fs::write(file, original)?,
                "extra" => fs::remove_file(extra)?,
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}

#[test]
fn factory_revision_assets_placement_transport_and_host_bytes_change_fingerprints() -> Result<()> {
    let (root, plan) = fixture()?;
    let original = stage(&plan)?;
    let mut changed = plan.clone();
    changed.components[0].component.factory_revision = "controller-v3".into();
    let revised = stage(&changed)?;
    assert_ne!(
        original.components[0].fingerprint,
        revised.components[0].fingerprint
    );
    write(root.path(), "configs/assets/table.bin", "changed table")?;
    let assets = stage(&plan)?;
    assert_ne!(
        original.components[0].fingerprint,
        assets.components[0].fingerprint
    );
    changed = plan.clone();
    changed.components[0].host = "sensors".into();
    changed.components[1].host = "control".into();
    let placed = stage(&changed)?;
    assert_ne!(
        original.components[0].fingerprint,
        placed.components[0].fingerprint
    );
    assert_ne!(
        original.component_hosts["control"].fingerprint,
        placed.component_hosts["control"].fingerprint
    );
    write(root.path(), "zenoh.json5", "{mode:'peer'}")?;
    let transport = stage(&plan)?;
    assert_ne!(
        assets.component_hosts["control"].fingerprint,
        transport.component_hosts["control"].fingerprint
    );
    write(root.path(), "bin/runtime.dll", "new native dependency")?;
    let runtime = stage(&plan)?;
    assert_ne!(
        transport.component_hosts["control"].fingerprint,
        runtime.component_hosts["control"].fingerprint
    );
    assert_ne!(
        transport.components[0].fingerprint,
        runtime.components[0].fingerprint
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn prepared_component_asset_symlink_cannot_escape_its_bundle() -> Result<()> {
    let (root, plan) = fixture()?;
    let prepared = stage(&plan)?;
    let asset = prepared.components[0]
        .config_dir
        .as_ref()
        .context("origin")?
        .join("../assets/table.bin");
    let external = root.path().join("outside.bin");
    fs::write(&external, fs::read(&asset)?)?;
    fs::remove_file(&asset)?;
    std::os::unix::fs::symlink(external, asset)?;

    let error = prepared
        .verify()
        .err()
        .context("symlink escape must fail")?;

    assert!(error.to_string().contains("escapes bundle"));
    Ok(())
}
