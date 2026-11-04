use super::*;

#[test]
fn compiled_report_must_preserve_exact_ports_sources_and_rpc_type_names() -> Result<()> {
    for mismatch in 0..5 {
        let mut harness = Harness::new()?;
        let mut prepared = connected_fixture(&harness, 1, true)?;
        let node = &mut prepared.nodes[1];
        let namespace = node.namespace.clone();
        let mut report: ModuleReport = serde_json::from_str(&node.run.args[5])?;
        match mismatch {
            0 => {
                report.inputs.get_mut("in").context("input")?.endpoint = PortEndpoint::Input {
                    sources: vec![EndpointAddress {
                        namespace: prepared.nodes[0].namespace.clone(),
                        port: "other".into(),
                    }],
                }
            }
            1 => report.inputs.get_mut("in").context("input")?.type_name = "test.Other".into(),
            2 => {
                report
                    .inputs
                    .get_mut("in")
                    .context("input")?
                    .response
                    .as_mut()
                    .context("response")?
                    .type_name = "test.Other".into();
            }
            3 => report.inputs.clear(),
            4 => {
                let port = report.inputs["in"].clone();
                report.inputs.insert("extra".into(), port);
            }
            _ => unreachable!(),
        }
        prepared.nodes[1].run.args[5] = serde_json::to_string(&report)?;

        let client = harness.start(prepared)?;

        let status = wait_phase(&client, DeploymentPhase::Degraded)?;
        assert!(client.wait_ready(Duration::ZERO).is_err());
        assert_eq!(status.modules[&namespace].phase, DeploymentPhase::Failed);
        assert_eq!(status.modules[&namespace].report, None);
        assert_eq!(status.modules[&namespace].pid, None);
    }
    Ok(())
}
