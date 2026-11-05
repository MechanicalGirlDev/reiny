use super::{ProcessStepClient, ProcessStepStartError};
use crate::{
    component::step::{CancelStatus, StepRunError},
    step::{StepFailureKind, StepRequest},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::timeout;

mod fixture;
use fixture::{BUDGET, Fixture, Mode};

fn request() -> StepRequest {
    StepRequest {
        dt: Duration::from_millis(25),
        inputs: BTreeMap::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_admission_veto_disposes_the_actual_prepared_object_without_run()
-> crate::Result<()> {
    Fixture::reject_validator().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_object_and_dropped_waiter_retain_exact_request() -> crate::Result<()> {
    let mut fixture = Fixture::start(Mode::Wait).await?;
    assert_eq!(
        fixture
            .constructions
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    let run = fixture.client.start(7, request(), BUDGET)?;
    timeout(BUDGET, fixture.entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("not entered"))?;
    let id = run.id();
    drop(run);
    fixture.release.send_replace(true);
    let retained = fixture
        .client
        .observe(7)?
        .ok_or_else(|| anyhow::anyhow!("lost request"))?;
    let receipt = timeout(BUDGET, retained.wait()).await??;
    assert_eq!(receipt.tick.id.index, 0);
    assert_eq!(
        receipt
            .outputs
            .values()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no output"))?
            .decode::<fixture::Number>()?
            .value,
        18
    );
    assert_eq!(fixture.client.start(7, request(), BUDGET)?.id(), id);
    assert!(matches!(
        fixture.client.start(7, request(), Duration::from_secs(8)),
        Err(ProcessStepStartError::RequestConflict)
    ));
    fixture.client.release(7)?;
    assert!(matches!(
        fixture.client.start(7, request(), BUDGET),
        Err(ProcessStepStartError::Released)
    ));
    fixture.finish().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_callback_causes_are_not_flattened() -> crate::Result<()> {
    for (mode, kind) in [
        (Mode::Error, StepFailureKind::NodeError),
        (Mode::Panic, StepFailureKind::NodePanicked),
        (Mode::Invalid, StepFailureKind::InvalidOutputs),
    ] {
        let fixture = Fixture::start(mode).await?;
        let run = fixture.client.try_step(request(), BUDGET)?;
        let result = timeout(BUDGET, run.wait()).await?;
        let Err(StepRunError::Failed(failure)) = result else {
            anyhow::bail!("missing retained fault");
        };
        assert_eq!(failure.kind, kind);
        assert_eq!(failure.attempted, ["node"]);
        assert_eq!(failure.completed, Vec::<String>::new());
        assert_eq!(fixture.client.clock().time, Duration::ZERO);
        fixture.finish().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_after_remote_dispatch_cannot_commit_or_replay() -> crate::Result<()> {
    let mut fixture = Fixture::start(Mode::Wait).await?;
    let run = fixture.client.try_step(request(), BUDGET)?;
    timeout(BUDGET, fixture.entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("not entered"))?;
    assert_eq!(run.cancel(), CancelStatus::Requested);
    let result = timeout(BUDGET, run.wait()).await?;
    let Err(StepRunError::Failed(failure)) = &result else {
        anyhow::bail!("missing fault");
    };
    assert_eq!(failure.kind, StepFailureKind::Cancelled);
    assert_eq!(failure.attempted, ["node"]);
    fixture.release.send_replace(true);
    assert_eq!(run.wait().await, result);
    assert_eq!(fixture.client.clock().next_index, 0);
    assert!(matches!(
        fixture.client.try_step(request(), BUDGET),
        Err(ProcessStepStartError::Admission(
            crate::component::step::StepStartError::Faulted(_)
        ))
    ));
    fixture.finish().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_capacity_and_release_never_reexecute_consumed_ids() -> crate::Result<()> {
    let fixture = Fixture::start(Mode::Sum).await?;
    for id in 0..64 {
        timeout(BUDGET, fixture.client.start(id, request(), BUDGET)?.wait()).await??;
    }
    assert!(matches!(
        fixture.client.start(64, request(), BUDGET),
        Err(ProcessStepStartError::RetentionCapacity)
    ));
    fixture.client.release(0)?;
    assert!(matches!(
        fixture.client.start(64, request(), BUDGET),
        Err(ProcessStepStartError::Released)
    ));
    timeout(BUDGET, fixture.client.start(65, request(), BUDGET)?.wait()).await??;
    assert_eq!(fixture.client.clock().next_index, 65);
    fixture.client.release(1)?;
    timeout(
        BUDGET,
        fixture.client.start(u64::MAX, request(), BUDGET)?.wait(),
    )
    .await??;
    assert!(matches!(
        fixture.client.try_step(request(), BUDGET),
        Err(ProcessStepStartError::SequenceExhausted)
    ));
    fixture.finish().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_deadline_faults_without_an_observer_and_channel_loss_is_unknown()
-> crate::Result<()> {
    for disconnect in [false, true] {
        let mut fixture = Fixture::start(Mode::Wait).await?;
        let run = fixture.client.start(0, request(), BUDGET)?;
        timeout(BUDGET, fixture.entered.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("not entered"))?;
        drop(run);
        if disconnect {
            fixture.disconnect()?;
        }
        let run = fixture
            .client
            .observe(0)?
            .ok_or_else(|| anyhow::anyhow!("lost accepted request"))?;
        let result = timeout(BUDGET + BUDGET, run.wait()).await?;
        let Err(StepRunError::Failed(failure)) = result else {
            anyhow::bail!("missing retained fault");
        };
        assert_eq!(
            failure.kind,
            if disconnect {
                StepFailureKind::WorkerLost
            } else {
                StepFailureKind::TimedOut
            }
        );
        assert_eq!(fixture.client.clock().next_index, 0);
        fixture.finish().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_client_value_rejects_before_dispatch_and_consumes_its_id() -> crate::Result<()> {
    let mut fixture = Fixture::start(Mode::Sum).await?;
    let oversized = crate::step::StepValue::from_encoded(
        crate::step::StepType::of::<fixture::Number>(),
        vec![0; super::protocol::step::MAX_VALUE_BYTES + 1],
    );
    let invalid = StepRequest {
        dt: Duration::from_millis(1),
        inputs: BTreeMap::from([(
            crate::step::StepInput {
                instance: "node".into(),
                port: "previous".into(),
            },
            oversized,
        )]),
    };
    assert!(matches!(
        fixture.client.start(0, invalid, BUDGET),
        Err(ProcessStepStartError::Bounds)
    ));
    assert!(matches!(
        fixture.entered.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        fixture.client.start(0, request(), BUDGET),
        Err(ProcessStepStartError::Released)
    ));
    timeout(BUDGET, fixture.client.start(1, request(), BUDGET)?.wait()).await??;
    assert_eq!(fixture.client.clock().next_index, 1);
    fixture.finish().await?;
    Ok(())
}
