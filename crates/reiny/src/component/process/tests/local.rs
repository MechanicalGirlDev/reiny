//! The same real `ProcessHost` lifecycle runs with and without the Zenoh feature.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use super::component::Behavior;
use super::fixture::{Event, Fixture, bindings, registration};
use super::protocol::*;

fn request() -> Prepare {
    Prepare {
        assignments: vec![ComponentAssignment {
            implementation: registration(),
            bindings: bindings(),
            config_origin: None,
        }],
        transport: TransportSettings::Local {
            domain: "local-process".into(),
            placement: BTreeMap::from([("test/service".into(), "fixture".into())]),
        },
        remaining_budget: Milliseconds(5000),
    }
}

#[tokio::test]
async fn local_process_host_runs_and_cleans_up_without_zenoh() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.authenticate().await?;
    fixture.send(OwnerMessage::Prepare(request())).await?;
    assert!(matches!(fixture.receive().await?, HostMessage::Report(_)));
    assert_eq!(fixture.receive().await?, HostMessage::PreparedComplete {});
    fixture.send(OwnerMessage::Commit {}).await?;
    assert_eq!(fixture.receive().await?, HostMessage::Committed {});
    fixture.send(OwnerMessage::Run {}).await?;
    assert_eq!(fixture.receive().await?, HostMessage::Running {});
    assert!(matches!(fixture.event().await?, Event::Prepare(_, _)));
    assert_eq!(fixture.event().await?, Event::Run);
    fixture
        .send(OwnerMessage::Stop {
            reason: StopReason::Requested,
            remaining_budget: Milliseconds(5000),
        })
        .await?;
    assert_eq!(fixture.outcome().await?, HostOutcome::Stopped {});
    assert_eq!(fixture.join()?, HostOutcome::Stopped {});
    assert_eq!(fixture.event().await?, Event::Stop);
    Ok(())
}

#[tokio::test]
async fn local_assignments_reject_invalid_maps_and_foreign_sources_before_construction()
-> crate::Result<()> {
    for invalid in 0..5 {
        let mut request = request();
        let TransportSettings::Local { placement, .. } = &mut request.transport else {
            anyhow::bail!("local fixture");
        };
        match invalid {
            0 => {
                placement.insert("test/service".into(), "other".into());
            }
            1 => {
                placement.insert("test/extra".into(), "fixture".into());
            }
            2 => {
                placement.insert("test/remote".into(), "bad/host".into());
            }
            3 => {
                for index in 0..8 {
                    placement.insert(format!("test/remote{index}"), "other".into());
                }
            }
            4 => {
                placement.insert("test/remote".into(), "other".into());
                request.assignments[0].bindings.inputs.insert(
                    "data".into(),
                    crate::bindings::InputBinding {
                        contract: crate::bindings::EndpointContract {
                            type_name: "Probe".into(),
                            ..Default::default()
                        },
                        sources: vec![crate::bindings::EndpointAddress {
                            namespace: "test/remote".into(),
                            port: "data".into(),
                        }],
                    },
                );
            }
            _ => anyhow::bail!("unexpected case"),
        }
        let mut fixture = Fixture::start(Behavior::Idle).await?;
        fixture.authenticate().await?;
        fixture.send(OwnerMessage::Prepare(request)).await?;
        assert!(matches!(fixture.outcome().await?,
            HostOutcome::Failed { primary, .. } if primary.code == FailureCode::Assignment));
        fixture.join()?;
        assert_eq!(fixture.constructed.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
