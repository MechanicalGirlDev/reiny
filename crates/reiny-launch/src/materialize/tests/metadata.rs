use super::{Context, PreparedDeployment, Result, fixture, fs, stage};
use std::sync::atomic::{AtomicBool, Ordering};

struct NoProcesses;

impl crate::ModuleObserver for NoProcesses {
    fn ready(&self, _namespace: &str) -> Result<bool> {
        anyhow::bail!("hosted deployment reached legacy observation")
    }
    fn request_stop(&self, _namespace: &str) -> Result<()> {
        anyhow::bail!("hosted deployment reached legacy stop")
    }
}

#[test]
fn hosted_owner_rejects_unrepresentable_transport_before_listening() -> Result<()> {
    let (root, plan) = fixture()?;
    let prepared = stage(&plan)?;
    let listened = AtomicBool::new(false);

    let error = crate::serve(prepared, &NoProcesses, &AtomicBool::new(false), &|| {
        listened.store(true, Ordering::Release);
        anyhow::bail!("unexpected owner admission")
    })
    .err()
    .context("owner must reject transport settings it cannot preserve")?;

    assert!(error.to_string().contains("hosted zenoh_config"));
    assert!(!listened.load(Ordering::Acquire));
    assert!(!root.path().join(".reiny/control.json").exists());
    Ok(())
}

#[test]
fn prepared_host_sets_and_placement_are_exact() -> Result<()> {
    let (_root, plan) = fixture()?;
    let prepared = stage(&plan)?;
    let mut cases = Vec::new();
    let mut duplicate = prepared.clone();
    duplicate.components.push(duplicate.components[0].clone());
    cases.push((duplicate, "duplicate component namespace"));
    let mut absent = prepared.clone();
    absent.component_hosts.remove("control");
    cases.push((absent, "unknown component host"));
    let mut unused = prepared.clone();
    unused
        .component_hosts
        .insert("unused".into(), unused.component_hosts["control"].clone());
    cases.push((unused, "unused component host"));
    let mut duplicate_placement = prepared.clone();
    duplicate_placement
        .component_hosts
        .get_mut("control")
        .context("host")?
        .instances
        .push("robot/controller".into());
    cases.push((duplicate_placement, "host instance set"));
    let mut empty = prepared.clone();
    empty.components.clear();
    cases.push((empty, "incomplete hosted deployment"));
    for factory in ["in", "invalid.factory", ""] {
        let mut invalid = prepared.clone();
        invalid.components[0].component.factory = factory.into();
        cases.push((invalid, "invalid hosted declaration name"));
    }
    for (changed, expected) in cases {
        let error = changed
            .verify()
            .err()
            .context("inconsistent hosted graph must fail")?;
        assert!(error.to_string().contains(expected), "{error:#}");
    }
    Ok(())
}

#[test]
fn prepared_envelope_rejects_forged_revision_and_hosted_version_downgrade() -> Result<()> {
    let (root, plan) = fixture()?;
    let prepared = stage(&plan)?;
    let path = root.path().join("prepared.json");
    prepared.write_file(&path)?;
    let envelope: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
    assert_eq!(envelope["version"], 2);
    for version in [0, 1, 3] {
        let mut changed = envelope.clone();
        changed["version"] = serde_json::json!(version);
        fs::write(&path, serde_json::to_vec(&changed)?)?;
        assert!(PreparedDeployment::load_file(&path).is_err());
    }
    let mut changed = prepared.clone();
    changed.components[0].component.factory_revision = "forged".into();
    let mut forged = envelope.clone();
    forged["deployment"] = serde_json::to_value(&changed)?;
    // Even someone recomputing the outer checksum cannot bypass inner fingerprints.
    forged["payload_sha256"] = crate::prepared::digest(&serde_json::to_vec(&changed)?).into();
    fs::write(&path, serde_json::to_vec(&forged)?)?;

    let error = PreparedDeployment::load_file(&path)
        .err()
        .context("revision must be fingerprinted")?;

    assert!(error.to_string().contains("fingerprint mismatch"));
    let host = serde_json::to_string(&prepared.component_hosts["control"])?;
    let duplicate = serde_json::to_string(&envelope)?.replace(
        "\"component_hosts\":{",
        &format!("\"component_hosts\":{{\"control\":{host},"),
    );
    fs::write(&path, duplicate)?;
    let error = PreparedDeployment::load_file(&path)
        .err()
        .context("duplicate host key")?;
    assert!(format!("{error:#}").contains("duplicate prepared host"));
    Ok(())
}

#[test]
fn duplicate_or_mixed_plan_is_rejected_before_staging() -> Result<()> {
    let (root, mut plan) = fixture()?;
    plan.components.push(plan.components[0].clone());
    let error = stage(&plan).err().context("duplicate placement")?;
    assert!(error.to_string().contains("duplicate component placement"));
    assert!(!root.path().join(".reiny/build").exists());
    plan.components.pop();
    super::write(
        root.path(),
        "legacy/main.yaml",
        "version: 3\nrun: {provider: local, bin: host}\n",
    )?;
    super::write(
        root.path(),
        "legacy-root/main.yaml",
        "version: 3\ndeployment: legacy\nproviders:\n  local: {type: process, bin_dir: ../bin}\nmodules:\n  app: {source: ../legacy}\n",
    )?;
    plan.nodes = crate::DeploymentPlan::load(&root.path().join("legacy-root"), false)?.nodes;

    let error = stage(&plan).err().context("mixed deployment")?;

    assert!(
        error
            .to_string()
            .contains("mixed hosted and legacy deployment")
    );
    assert!(!root.path().join(".reiny/build").exists());
    Ok(())
}
