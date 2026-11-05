use super::*;
use std::future::{Future, poll_fn};

fn local_options() -> LocalHostOptions {
    LocalHostOptions {
        domain: "unbounded".into(),
        engine: Arc::new(Local::new()),
        startup_timeout: None,
        stop_timeout: None,
    }
}

#[tokio::test]
async fn unbounded_startup_retains_preparation_until_release() {
    let (registry, mut records, gate) = fixture(Behavior::PrepareWait);
    let host = ComponentHost::launch_local(
        registry,
        local_options(),
        vec![instance("app/wait", "test")],
    )
    .expect("launch");
    stage(&mut records, Stage::Prepare).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_hours(24)).await;
    let mut running = Box::pin(host.wait_running());
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(running.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(running);
    assert_eq!(host.status().phase, HostPhase::Starting);
    tokio::time::resume();
    gate.send_replace(true);
    timeout(PATIENCE, host.wait_running())
        .await
        .expect("startup watchdog")
        .expect("running");
    assert!(matches!(host.stop().await, HostOutcome::Stopped));
}

#[tokio::test]
async fn unbounded_stop_joins_gated_hook_even_when_waiter_is_dropped() {
    let (registry, mut records, gate) = fixture(Behavior::StopWait);
    let host = ComponentHost::launch_local(
        registry,
        local_options(),
        vec![instance("app/wait", "test")],
    )
    .expect("launch");
    host.wait_running().await.expect("running");
    let mut stopping = Box::pin(host.stop());
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(stopping.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    stage(&mut records, Stage::Stop(StopReason::Requested)).await;
    drop(stopping);
    tokio::time::pause();
    tokio::time::advance(Duration::from_hours(24)).await;
    let mut terminal = Box::pin(host.wait_terminal());
    assert!(
        poll_fn(|cx| std::task::Poll::Ready(terminal.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert_eq!(host.status().phase, HostPhase::Stopping);
    gate.send_replace(true);
    assert!(matches!(
        timeout(PATIENCE, terminal).await.expect("join watchdog"),
        HostOutcome::Stopped
    ));
    stage(&mut records, Stage::Dropped).await;
}

#[test]
fn optional_budgets_still_reject_zero_and_unrepresentable_values() {
    for invalid in [Duration::ZERO, Duration::MAX] {
        for startup in [true, false] {
            let (registry, _records, _gate) = fixture(Behavior::Idle);
            let mut options = local_options();
            if startup {
                options.startup_timeout = Some(invalid);
            } else {
                options.stop_timeout = Some(invalid);
            }
            assert!(matches!(
                ComponentHost::launch_local(registry, options, vec![instance("app", "test")]),
                Err(HostError {
                    code: HostErrorCode::InvalidOptions,
                    ..
                })
            ));
        }
    }
}
