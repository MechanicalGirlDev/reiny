use super::*;

#[path = "unbounded.rs"]
mod unbounded;

#[tokio::test]
async fn cancellation_after_dispatch_faults_and_cleanup_is_separate() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::StopWait)?;
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step(request(), BUDGET)?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    assert_eq!(run.cancel(), CancelStatus::Requested);
    let failure = failed(run.wait().await, StepFailureKind::Cancelled)?;
    assert_eq!(failure.attempted, ["controller"]);
    assert_eq!(failure.completed, Vec::<String>::new());
    assert_eq!(steps.clock().next_index, 0);
    assert!(matches!(
        steps.try_step(request(), BUDGET),
        Err(StepStartError::Faulted(_))
    ));
    let mut status = host.subscribe();
    tokio::time::timeout(
        BUDGET,
        status.wait_for(|status| status.phase == HostPhase::Stopping),
    )
    .await??;
    assert!(host.terminal.borrow().is_none());
    fixture.release.send_replace(true);
    assert!(
        matches!(tokio::time::timeout(BUDGET, host.wait_terminal()).await?, HostOutcome::Failed { primary, .. } if primary.code == crate::component::HostErrorCode::Step)
    );
    Ok(())
}

#[tokio::test]
async fn stop_during_dispatch_keeps_clock_and_known_progress() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::Wait)?;
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step(request(), BUDGET)?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    let (outcome, result) = tokio::join!(host.stop(), run.wait());
    let failure = failed(result, StepFailureKind::HostStopped)?;
    assert_eq!(failure.attempted, ["controller"]);
    assert_eq!(failure.completed, Vec::<String>::new());
    assert_eq!(steps.clock().time, Duration::ZERO);
    assert!(matches!(
        outcome,
        HostOutcome::Stopped | HostOutcome::Failed { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn timeout_before_dispatch_is_retryable_and_after_dispatch_is_terminal() -> crate::Result<()>
{
    let mut fixture = fixture(Behavior::Wait)?;
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    // Pause only after Local's external dispatcher has finished admission.
    tokio::time::pause();
    let first = steps.try_step(request(), Duration::from_secs(1))?;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        first.wait().await,
        Err(StepRunError::TimedOutBeforeDispatch { tick: first.tick() })
    );
    assert_eq!(steps.clock().phase, ClockPhase::Ready);
    let second = steps.try_step(request(), Duration::from_secs(1))?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    tokio::time::advance(Duration::from_secs(1)).await;
    let failure = failed(second.wait().await, StepFailureKind::TimedOut)?;
    assert_eq!(failure.attempted, ["controller"]);
    assert_eq!(failure.completed, Vec::<String>::new());
    assert!(matches!(
        host.wait_terminal().await,
        HostOutcome::Failed { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn node_error_panic_and_invalid_outputs_preserve_fault_and_cleanup_rules() -> crate::Result<()>
{
    for (behavior, kind) in [
        (Behavior::Error, StepFailureKind::NodeError),
        (Behavior::Panic, StepFailureKind::NodePanicked),
        (Behavior::Invalid, StepFailureKind::InvalidOutputs),
    ] {
        let mut fixture = fixture(behavior)?;
        let (host, steps) = ComponentHost::launch_with_steps(
            fixture.registry,
            fixture.options,
            fixture.instances,
            graph(),
            Duration::ZERO,
        )?;
        host.wait_running().await?;
        let run = steps.try_step(request(), BUDGET)?;
        let failure = failed(tokio::time::timeout(BUDGET, run.wait()).await?, kind)?;
        assert_eq!(failure.attempted, ["controller"]);
        assert_eq!(failure.completed, Vec::<String>::new());
        assert!(matches!(
            host.wait_terminal().await,
            HostOutcome::Failed { .. }
        ));
        let mut records = Vec::new();
        while let Ok(record) = fixture.events.try_recv() {
            records.push(record);
        }
        assert!(records.contains(&Event::Drop("controller".into())));
        assert_eq!(
            records.contains(&Event::Stop("controller".into())),
            !matches!(behavior, Behavior::Panic)
        );
        assert_eq!(steps.clock().next_index, 0);
    }
    Ok(())
}

#[tokio::test]
async fn readiness_loss_cancels_step_and_withdraws_group() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::LoseReady)?;
    fixture
        .instances
        .retain(|instance| instance.bindings.namespace == "controller");
    let mut graph = graph();
    graph.nodes.remove("physics");
    graph.inputs.remove(&input("physics", "command"));
    graph.inputs.insert(
        input("controller", "state"),
        StepSource::PreviousTick {
            source: output("controller"),
            initial: value(10),
        },
    );
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph,
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step(request(), BUDGET)?;
    let failure = failed(
        tokio::time::timeout(BUDGET, run.wait()).await?,
        StepFailureKind::HostStopped,
    )?;
    assert_eq!(failure.attempted, ["controller"]);
    assert!(
        matches!(host.wait_terminal().await, HostOutcome::Failed { primary, .. } if primary.code == crate::component::HostErrorCode::ReadinessLost)
    );
    Ok(())
}

#[tokio::test]
async fn unresponsive_cleanup_does_not_replace_step_failure() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::StopWait)?;
    fixture.options.stop_timeout = Duration::from_secs(2);
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    tokio::time::pause();
    let run = steps.try_step(request(), BUDGET)?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    assert_eq!(run.cancel(), CancelStatus::Requested);
    failed(run.wait().await, StepFailureKind::Cancelled)?;
    tokio::time::timeout(BUDGET, async {
        loop {
            if fixture.events.recv().await == Some(Event::Stop("controller".into())) {
                break;
            }
        }
    })
    .await?;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(
        matches!(host.wait_terminal().await, HostOutcome::Unresponsive { instances, .. } if instances == ["controller"])
    );
    failed(run.wait().await, StepFailureKind::Cancelled)?;
    Ok(())
}

#[tokio::test]
async fn dropping_run_and_cancelled_waiters_do_not_cancel_execution() -> crate::Result<()> {
    let mut fixture = fixture(Behavior::Wait)?;
    let (host, steps) = ComponentHost::launch_with_steps(
        fixture.registry,
        fixture.options,
        fixture.instances,
        graph(),
        Duration::ZERO,
    )?;
    host.wait_running().await?;
    let run = steps.try_step(request(), BUDGET)?;
    event(&mut fixture.events, Event::Enter("controller".into())).await?;
    let mut wait = Box::pin(run.wait());
    assert!(matches!(
        std::future::poll_fn(|cx| { std::task::Poll::Ready(wait.as_mut().poll(cx)) }).await,
        std::task::Poll::Pending
    ));
    drop(wait);
    let mut clock = steps.subscribe_clock();
    drop(run);
    fixture.release.send_replace(true);
    tokio::time::timeout(BUDGET, clock.wait_for(|clock| clock.next_index == 1)).await??;
    assert_eq!(steps.clock().phase, ClockPhase::Ready);
    assert!(matches!(host.stop().await, HostOutcome::Stopped));
    Ok(())
}
