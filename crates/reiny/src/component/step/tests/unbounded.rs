use super::*;
use std::future::{Future, poll_fn};

#[path = "unbounded/native.rs"]
mod native;

fn local_options() -> crate::component::LocalHostOptions {
    crate::component::LocalHostOptions {
        domain: "unbounded_steps".into(),
        engine: Arc::new(crate::engine::Local::new()),
        startup_timeout: None,
        stop_timeout: None,
    }
}

#[tokio::test]
async fn unbounded_step_stays_admitted_and_commits_after_release() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::Wait)?;
    let (host, steps) = ComponentHost::launch_local_with_steps(
        fixture.registry,
        local_options(),
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step_with_timeout(request(), None)?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    tokio::time::pause();
    tokio::time::advance(Duration::from_hours(24)).await;
    let mut wait = Box::pin(run.wait());
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(wait);
    assert!(matches!(
        steps.try_step_with_timeout(request(), None),
        Err(StepStartError::Busy { in_flight }) if in_flight == run.id()
    ));
    fixture.release.send_replace(true);
    let receipt = tokio::time::timeout(BUDGET, run.wait()).await??;
    assert_eq!(
        receipt.outputs[&output("physics")]
            .decode::<Number>()?
            .value,
        14
    );
    assert_eq!(steps.clock().next_index, 1);
    assert_eq!(run.cancel(), CancelStatus::Finished);
    assert!(matches!(host.stop().await, HostOutcome::Stopped));
    assert!(Arc::ptr_eq(&receipt, &run.wait().await?));
    Ok(())
}

#[tokio::test]
async fn unbounded_cancellation_retains_failure_while_cleanup_waits() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::StopWait)?;
    let (host, steps) = ComponentHost::launch_local_with_steps(
        fixture.registry,
        local_options(),
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step_with_timeout(request(), None)?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    assert_eq!(run.cancel(), CancelStatus::Requested);
    failed(run.wait().await, StepFailureKind::Cancelled)?;
    tokio::time::timeout(BUDGET, async {
        loop {
            let observed = fixture
                .events
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("cleanup event stream closed"))?;
            if observed == Event::Stop("controller".into()) {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;
    tokio::time::pause();
    tokio::time::advance(Duration::from_hours(24)).await;
    let mut terminal = Box::pin(host.wait_terminal());
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(terminal.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert_eq!(steps.clock().next_index, 0);
    fixture.release.send_replace(true);
    assert!(matches!(
        tokio::time::timeout(BUDGET, terminal).await?,
        HostOutcome::Failed { .. }
    ));
    failed(run.wait().await, StepFailureKind::Cancelled)?;
    Ok(())
}

#[tokio::test]
async fn dropping_unbounded_run_does_not_cancel_accepted_execution() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::Wait)?;
    let (host, steps) = ComponentHost::launch_local_with_steps(
        fixture.registry,
        local_options(),
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step_with_timeout(request(), None)?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    let mut clock = steps.subscribe_clock();
    drop(run);
    fixture.release.send_replace(true);
    tokio::time::timeout(BUDGET, clock.wait_for(|clock| clock.next_index == 1)).await??;
    assert_eq!(steps.clock().phase, ClockPhase::Ready);
    assert!(matches!(host.stop().await, HostOutcome::Stopped));
    Ok(())
}
