use super::super::{DeploymentPlan, ROOT, fixture, write};
use reiny_launch::ComponentTransport;

#[test]
fn local_transport_rejects_every_cross_host_edge_before_build() -> anyhow::Result<()> {
    for host in ["sensors", "control"] {
        let root = fixture()?;
        let mut manifest: serde_json::Value = serde_yaml::from_str(ROOT)?;
        manifest["providers"]["local"]["connect"] = serde_json::json!([]);
        manifest["component_hosts"][host]["transport"] = "local".into();
        write(root.path(), "main.yaml", &serde_yaml::to_string(&manifest)?)?;
        let error = DeploymentPlan::load(root.path(), false)
            .err()
            .ok_or_else(|| anyhow::anyhow!("cross-host Local accepted"))?;
        assert!(error.to_string().contains("local-only host boundary"));
        assert!(!root.path().join(".reiny/build").exists());
    }
    Ok(())
}

#[test]
fn explicit_local_transport_rejects_network_settings() -> anyhow::Result<()> {
    let root = fixture()?;
    write(
        root.path(),
        "main.yaml",
        &ROOT.replace("bin: sensor-host", "bin: sensor-host, transport: local"),
    )?;
    let error = DeploymentPlan::load(root.path(), false)
        .err()
        .ok_or_else(|| anyhow::anyhow!("Local ignored network configuration"))?;
    assert!(error.to_string().contains("forbids Zenoh"));
    Ok(())
}

#[test]
fn default_transport_keeps_frozen_shape_and_mutated_plans_cannot_bypass_isolation()
-> anyhow::Result<()> {
    let root = fixture()?;
    for name in ["sensor-host", "control-host"] {
        write(
            root.path(),
            &format!("absent-binaries/{name}{}", std::env::consts::EXE_SUFFIX),
            name,
        )?;
    }
    let mut plan = DeploymentPlan::load(root.path(), false)?;
    let host = plan
        .component_hosts
        .get_mut("control")
        .ok_or_else(|| anyhow::anyhow!("control host"))?;
    assert_eq!(host.spec.transport, ComponentTransport::RoutedZenoh);
    assert!(serde_json::to_value(&host.spec)?.get("transport").is_none());
    host.spec.transport = ComponentTransport::Local;
    host.provider.connect.clear();
    let error = plan
        .prepare()
        .err()
        .ok_or_else(|| anyhow::anyhow!("public plan bypassed Local boundary validation"))?;
    assert!(format!("{error:#}").contains("local-only host boundary"));
    Ok(())
}

#[test]
fn local_host_accepts_only_its_complete_internal_graph() -> anyhow::Result<()> {
    let root = fixture()?;
    let mut manifest: serde_json::Value = serde_yaml::from_str(ROOT)?;
    manifest["providers"]["local"]["connect"] = serde_json::json!([]);
    manifest["component_hosts"]
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("hosts"))?
        .remove("control");
    manifest["component_hosts"]["sensors"]["transport"] = "local".into();
    manifest["placement"]["robot/controller"] = "sensors".into();
    write(root.path(), "main.yaml", &serde_yaml::to_string(&manifest)?)?;
    let plan = DeploymentPlan::load(root.path(), false)?;
    assert_eq!(plan.components.len(), 3);
    assert_eq!(plan.component_hosts.len(), 1);
    assert_eq!(
        plan.component_hosts["sensors"].spec.transport,
        ComponentTransport::Local
    );
    Ok(())
}
