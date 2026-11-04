use super::super::protocol::*;
use super::component::Behavior;
use super::fixture::{Event, Fixture, bindings};
use crate::bindings::{ReportEntry, validate_graph};

#[tokio::test]
async fn owner_validates_reports_then_commits_runs_and_stops() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.authenticate().await?;
    fixture.prepare(vec![None]).await?;
    let HostMessage::Report(report) = fixture.receive().await? else {
        anyhow::bail!("missing compiled report");
    };
    validate_graph(&[ReportEntry {
        bindings: &bindings(),
        report: &report,
    }])?;
    assert_eq!(fixture.receive().await?, HostMessage::PreparedComplete {});
    fixture.send(OwnerMessage::Commit {}).await?;
    assert_eq!(fixture.receive().await?, HostMessage::Committed {});
    fixture.send(OwnerMessage::Run {}).await?;
    assert_eq!(fixture.receive().await?, HostMessage::Running {});
    assert_eq!(
        fixture.event().await?,
        Event::Prepare("test/service".into(), None)
    );
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
async fn run_before_commit_is_rejected_and_never_calls_run() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.authenticate().await?;
    fixture.prepare(vec![None]).await?;
    assert!(matches!(fixture.receive().await?, HostMessage::Report(_)));
    assert_eq!(fixture.receive().await?, HostMessage::PreparedComplete {});
    fixture.send(OwnerMessage::Run {}).await?;
    assert!(matches!(
        fixture.outcome().await?,
        HostOutcome::Failed {
            primary: HostFailure {
                code: FailureCode::Transition,
                ..
            },
            ..
        }
    ));
    fixture.join()?;
    assert_eq!(
        fixture.event().await?,
        Event::Prepare("test/service".into(), None)
    );
    assert_eq!(fixture.event().await?, Event::Stop);
    assert!(fixture.events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn stale_sequence_aborts_prepared_tickets() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.authenticate().await?;
    fixture.prepare(vec![None]).await?;
    assert!(matches!(fixture.receive().await?, HostMessage::Report(_)));
    assert_eq!(fixture.receive().await?, HostMessage::PreparedComplete {});
    fixture.sequence = 1;
    fixture.send(OwnerMessage::Commit {}).await?;
    assert!(matches!(
        fixture.outcome().await?,
        HostOutcome::Failed {
            primary: HostFailure {
                code: FailureCode::Sequence,
                ..
            },
            ..
        }
    ));
    fixture.join()?;
    assert_eq!(
        fixture.event().await?,
        Event::Prepare("test/service".into(), None)
    );
    assert_eq!(fixture.event().await?, Event::Stop);
    assert!(fixture.events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn eof_during_pending_prepare_cancels_and_joins_without_run() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::WaitPrepare).await?;
    fixture.authenticate().await?;
    fixture.prepare(vec![None]).await?;
    assert_eq!(
        fixture.event().await?,
        Event::Prepare("test/service".into(), None)
    );
    fixture.disconnect()?;
    assert!(matches!(
        fixture.join()?,
        HostOutcome::Failed {
            primary: HostFailure {
                code: FailureCode::Disconnected,
                ..
            },
            ..
        }
    ));
    assert_eq!(fixture.event().await?, Event::Stop);
    assert!(fixture.events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn owner_rejects_wrong_nonce_before_any_component_is_constructed() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.nonce = Nonce::new([24; 32]);
    assert!(fixture.authenticate().await.is_err());
    fixture.disconnect()?;
    assert!(matches!(
        fixture.join()?,
        HostOutcome::Failed {
            primary: HostFailure {
                code: FailureCode::Disconnected,
                ..
            },
            ..
        }
    ));
    assert!(fixture.events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn each_instance_captures_its_own_config_directory() -> crate::Result<()> {
    let first = std::fs::canonicalize(std::env::temp_dir())?;
    let second = std::fs::canonicalize(std::env::current_dir()?)?;
    anyhow::ensure!(first != second, "fixture directories must differ");
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.authenticate().await?;
    fixture
        .prepare(vec![
            Some(ConfigOrigin {
                directory: first.to_string_lossy().into_owned(),
                config_file: None,
            }),
            Some(ConfigOrigin {
                directory: second.to_string_lossy().into_owned(),
                config_file: None,
            }),
        ])
        .await?;
    for _ in 0..2 {
        assert!(matches!(fixture.receive().await?, HostMessage::Report(_)));
    }
    assert_eq!(fixture.receive().await?, HostMessage::PreparedComplete {});
    fixture
        .send(OwnerMessage::Stop {
            reason: StopReason::Requested,
            remaining_budget: Milliseconds(5000),
        })
        .await?;
    assert_eq!(fixture.outcome().await?, HostOutcome::Stopped {});
    fixture.join()?;
    let mut origins = std::collections::BTreeMap::new();
    while let Ok(event) = fixture.events.try_recv() {
        match event {
            Event::Prepare(id, directory) => {
                origins.insert(id, directory);
            }
            Event::Stop => {}
            Event::Run => anyhow::bail!("run before owner commit"),
        }
    }
    assert_eq!(
        origins,
        [
            ("test/service".to_owned(), Some(first)),
            ("test/service1".to_owned(), Some(second)),
        ]
        .into()
    );
    Ok(())
}
