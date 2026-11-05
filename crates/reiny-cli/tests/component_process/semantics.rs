//! Semantic admission through real CLI ownership and actual SDK host processes.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use reiny::bindings::{ModuleReport, ReportEntry, ReportErrorCode, validate_graph};
use reiny_launch::{DeploymentPlan, PreparedDeployment};
use serde_json::Value;

use super::fixture::{Events, Fixture, Hosts, success};
use super::{assert_reaped, check_ready};

#[derive(Clone, Copy)]
enum Case {
    Legacy,
    Matching,
    DifferentUnits,
    MissingAssertions,
}

fn assertions() -> BTreeMap<String, String> {
    [
        ("units", "example.si.v1"),
        ("identity", "example.target-a.v1"),
        ("ordering", "example.xy.v1"),
        ("time", "example.monotonic-observation.v1"),
        ("update", "example.every-step.v1"),
    ]
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .into()
}

async fn prepare(fixture: &Fixture, case: Case) -> Result<PreparedDeployment> {
    if !matches!(case, Case::Legacy) {
        for (role, direction) in [("sender", "out"), ("receiver", "in")] {
            let path = fixture.path().join(role).join("main.yaml");
            let mut leaf: Value = serde_yaml::from_str(&std::fs::read_to_string(&path)?)?;
            leaf[direction]["data"]["semantics"] = serde_json::to_value(assertions())?;
            std::fs::write(path, serde_yaml::to_string(&leaf)?)?;
        }
    }
    if matches!(case, Case::DifferentUnits | Case::MissingAssertions) {
        // Public non-manifest callers can supply resolved plans. Keep each
        // binding valid but make this exact edge incompatible, so the real
        // process owner's compiled graph admission, not YAML parsing, rejects it.
        let mut plan = DeploymentPlan::load(fixture.path(), false)?;
        let receiver = plan
            .components
            .iter_mut()
            .find(|component| component.namespace == "placement/receiver")
            .context("receiver component")?;
        let semantics = &mut receiver
            .bindings
            .inputs
            .get_mut("data")
            .context("data input")?
            .contract
            .semantics;
        match case {
            Case::DifferentUnits => {
                semantics.insert("units".into(), "example.other-units.v1".into());
            }
            Case::MissingAssertions => semantics.clear(),
            Case::Legacy | Case::Matching => unreachable!("rejection fixture"),
        }
        plan.prepare()?
            .write_file(&fixture.path().join("prepared.json"))?;
    } else {
        fixture.prepare().await?;
    }
    PreparedDeployment::load_file(&fixture.path().join("prepared.json"))
}

fn check_reports(prepared: &PreparedDeployment, status: &Value, accepted: bool) -> Result<()> {
    let reports = prepared
        .components
        .iter()
        .map(|component| {
            serde_json::from_value::<ModuleReport>(
                status["components"][&component.namespace]["report"].clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let entries = prepared
        .components
        .iter()
        .zip(&reports)
        .map(|(component, report)| ReportEntry {
            bindings: &component.bindings,
            report,
        })
        .collect::<Vec<_>>();
    let result = validate_graph(&entries);
    if accepted {
        result?;
    } else {
        let error = result
            .err()
            .context("incompatible compiled graph admitted")?;
        assert_eq!(error.code, ReportErrorCode::Semantics);
        let source = error.source.context("exact mismatching source")?;
        assert_eq!(source.namespace, "placement/sender");
        assert_eq!(source.port, "data");
    }
    Ok(())
}

pub(super) async fn admission(hosts: &Hosts, endpoint: &str) -> Result<()> {
    for case in [
        Case::Legacy,
        Case::Matching,
        Case::DifferentUnits,
        Case::MissingAssertions,
    ] {
        let mut events = Events::listen().await?;
        let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
        let prepared = prepare(&fixture, case).await?;
        let sender = prepared
            .components
            .iter()
            .find(|component| component.namespace == "placement/sender")
            .context("sender component")?;
        assert_eq!(
            sender.bindings.outputs["data"].contract.semantics,
            if matches!(case, Case::Legacy) {
                BTreeMap::new()
            } else {
                assertions()
            }
        );
        let output = fixture.apply().await?;
        for role in ["sender", "receiver"] {
            let _prepared = events.wait(role, "prepared").await?;
        }
        let accepted = matches!(case, Case::Legacy | Case::Matching);
        assert_eq!(output.status.success(), accepted);
        let status = fixture.status().await?;
        check_reports(&prepared, &status, accepted)?;
        if accepted {
            success(&output, "semantic admission")?;
            let _pids = check_ready(&status)?;
            for role in ["sender", "receiver"] {
                let delivered = events.wait(role, "delivered").await?;
                assert_eq!(delivered["sequence"], 42);
            }
            let stopped = fixture.stop().await?;
            assert_reaped(&stopped)?;
            for role in ["sender", "receiver"] {
                let _stopped = events.wait(role, "stop").await?;
            }
        } else {
            assert_reaped(&status)?;
            for role in ["sender", "receiver"] {
                ensure!(
                    !fixture.path().join(format!("{role}.ran")).exists(),
                    "semantic rejection entered {role} run"
                );
                let _stopped = events.wait(role, "stop").await?;
            }
            events.assert_no_run()?;
        }
    }
    Ok(())
}
