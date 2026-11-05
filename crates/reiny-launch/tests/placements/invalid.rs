use super::super::{CONTROLLER, DeploymentPlan, ROOT, SENSOR, fixture, write};

#[test]
fn placement_requires_exact_canonical_instances_and_nonempty_known_hosts() -> anyhow::Result<()> {
    for (before, after) in [
        ("  robot/controller: control\n", ""),
        ("  robot/controller: control", "  robot/unknown: control"),
        ("  robot/controller: control", "  controller: control"),
        (
            "  robot/controller: control",
            "  robot//controller: control",
        ),
        ("  robot/controller: control", "  robot/branch: control"),
        ("  robot/controller: control", "  robot/controller: unknown"),
        ("  robot/controller: control", "  robot/controller: ''"),
        ("  robot/controller: control", "  robot/controller: sensors"),
        (
            "  robot/controller: control",
            "  robot/controller: sensors\n  robot/controller: control",
        ),
        (
            "  control: {provider: local, bin: control-host}",
            "  control: {provider: local, bin: control-host}\n  control: {provider: local, bin: other-host}",
        ),
        (
            "  control: {provider: local, bin: control-host}",
            "  control: {provider: missing, bin: control-host}",
        ),
        (
            "  control: {provider: local, bin: control-host}",
            "  control: {provider: local, bin: ../escape}",
        ),
        ("type: process", "type: artifact"),
    ] {
        let root = fixture()?;
        write(root.path(), "main.yaml", &ROOT.replace(before, after))?;

        assert!(DeploymentPlan::load(root.path(), false).is_err(), "{after}");
    }
    Ok(())
}

#[test]
fn component_schema_rejects_legacy_authority_and_unsupported_lifecycle() -> anyhow::Result<()> {
    for invalid in [
        SENSOR.replace("version: 4", "version: 3"),
        SENSOR.replace("factory: sensor", "factory: ''"),
        SENSOR.replace("factory_revision: '1'", "factory_revision: ''"),
        SENSOR.replace("factory_revision: '1'", "factory_revision: ' '"),
        SENSOR.replace("factory_revision: '1'", "factory_revision: '1', args: []"),
        SENSOR.replace(
            "factory_revision: '1'",
            "factory_revision: '1', companions: [helper]",
        ),
        SENSOR.replace(
            "factory_revision: '1'",
            "factory_revision: '1', config_assets: [missing]",
        ),
        format!("{SENSOR}run: {{provider: local, bin: sensor}}\n"),
        format!("{SENSOR}modules: {{nested: {{source: nested}}}}\n"),
        format!("{SENSOR}build: {{type: cargo}}\n"),
        format!("{SENSOR}owned_children: {{child: {{outputs: [state]}}}}\n"),
        format!("{SENSOR}placement: {{robot/branch/left: sensors}}\n"),
    ] {
        let root = fixture()?;
        write(root.path(), "branch/sensor/main.yaml", &invalid)?;

        assert!(
            DeploymentPlan::load(root.path(), false).is_err(),
            "{invalid}"
        );
    }
    for extra in [
        ", restart: on_failure",
        ", restart: always",
        ", rolling: true",
        ", type: in_process",
    ] {
        let root = fixture()?;
        write(
            root.path(),
            "main.yaml",
            &ROOT.replace("bin: control-host}", &format!("bin: control-host{extra}}}")),
        )?;

        assert!(DeploymentPlan::load(root.path(), false).is_err(), "{extra}");
    }
    Ok(())
}

#[test]
fn mixing_legacy_processes_with_hosted_components_is_rejected() -> anyhow::Result<()> {
    let root = fixture()?;
    write(
        root.path(),
        "controller/main.yaml",
        &CONTROLLER.replace(
            "component: {factory: controller, factory_revision: '2'}",
            "run: {provider: local, bin: legacy-controller}",
        ),
    )?;
    write(
        root.path(),
        "main.yaml",
        &ROOT
            .replace("  robot/controller: control\n", "")
            .replace("  control: {provider: local, bin: control-host}\n", ""),
    )?;

    assert!(DeploymentPlan::load(root.path(), false).is_err());
    Ok(())
}

#[test]
fn component_call_args_and_v3_root_hosted_fields_are_rejected() -> anyhow::Result<()> {
    for text in [
        ROOT.replace("version: 4", "version: 3"),
        ROOT.replace(
            "    source: controller",
            "    source: controller\n    args: []",
        ),
    ] {
        let root = fixture()?;
        write(root.path(), "main.yaml", &text)?;

        assert!(DeploymentPlan::load(root.path(), false).is_err());
    }
    Ok(())
}
