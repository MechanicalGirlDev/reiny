use super::super::protocol::*;
use super::component::Behavior;
use super::fixture::{Event, Fixture, registration};

#[tokio::test]
async fn incompatible_release_and_revision_are_rejected_before_prepare() -> crate::Result<()> {
    for (compatibility, required_factories, expected) in [
        (
            Compatibility {
                report_version: 0,
                ..Compatibility::current(3)
            },
            vec![registration()],
            FailureCode::Compatibility,
        ),
        (
            Compatibility::current(3),
            vec![FactoryRegistration {
                revision: "other".into(),
                ..registration()
            }],
            FailureCode::Factory,
        ),
    ] {
        let mut fixture = Fixture::start(Behavior::Idle).await?;
        assert!(matches!(
            fixture.receive().await?,
            HostMessage::Authenticate(_)
        ));
        fixture
            .send(OwnerMessage::Authenticated {
                compatibility,
                required_factories,
            })
            .await?;
        let outcome = fixture.outcome().await?;
        assert!(matches!(outcome, HostOutcome::Failed { primary, .. } if primary.code == expected));
        fixture.join()?;
        assert!(fixture.events.try_recv().is_err());
    }
    Ok(())
}

#[tokio::test]
async fn stale_owner_identity_aborts_before_constructing_components() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.authenticate().await?;
    fixture.identity.owner_generation = OwnerGeneration(999);
    fixture.prepare(vec![None]).await?;
    // The rejection still echoes the original trusted bootstrap identity.
    fixture.identity.owner_generation = OwnerGeneration(1);
    assert!(matches!(
        fixture.outcome().await?,
        HostOutcome::Failed {
            primary: HostFailure {
                code: FailureCode::Identity,
                ..
            },
            ..
        }
    ));
    fixture.join()?;
    assert!(fixture.events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn component_failure_interrupts_pending_startup_without_an_owner_command() -> crate::Result<()>
{
    let mut fixture = Fixture::start(Behavior::FailPrepare).await?;
    fixture.authenticate().await?;
    fixture.prepare(vec![None]).await?;
    assert!(matches!(
        fixture.outcome().await?,
        HostOutcome::Failed {
            primary: HostFailure {
                code: FailureCode::Prepare,
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
async fn owner_stop_interrupts_prepare_without_releasing_run() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::WaitPrepare).await?;
    fixture.authenticate().await?;
    fixture.prepare(vec![None]).await?;
    assert_eq!(
        fixture.event().await?,
        Event::Prepare("test/service".into(), None)
    );
    fixture
        .send(OwnerMessage::Stop {
            reason: StopReason::Requested,
            remaining_budget: Milliseconds(5000),
        })
        .await?;
    assert_eq!(fixture.outcome().await?, HostOutcome::Stopped {});
    assert_eq!(fixture.join()?, HostOutcome::Stopped {});
    assert_eq!(fixture.event().await?, Event::Stop);
    assert!(fixture.events.try_recv().is_err());
    Ok(())
}
