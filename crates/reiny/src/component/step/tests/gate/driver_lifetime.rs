//! Driver loss must terminate accepted observers without committing.

use super::*;

#[tokio::test]
async fn aborted_driver_reports_unknown_effects_and_requests_host_shutdown() -> crate::Result<()> {
    use crate::component::step::{StepHandle, driver::Driver, worker::Executor};
    let scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
    let gate = Gate::new(scheduler.clock());
    gate.activate();
    let (requests, received) = tokio::sync::mpsc::unbounded_channel();
    let steps = StepHandle {
        gate: gate.clone(),
        requests,
        plan: scheduler.plan.clone(),
    };
    let (calls, mut called) = tokio::sync::mpsc::unbounded_channel();
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let (control, _controlled) = tokio::sync::mpsc::channel(1);
    let driver = tokio::spawn(
        Driver {
            scheduler,
            gate,
            requests: received,
            executor: Box::new(Executor {
                calls: std::collections::BTreeMap::from([("controller".into(), calls)]),
                failure: None,
            }),
        }
        .run(stop, control),
    );
    let run = steps.try_step(request(), Duration::from_secs(5))?;
    let _dispatched = tokio::time::timeout(Duration::from_secs(5), called.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("worker was not dispatched"))?;
    driver.abort();
    assert!(driver.await.is_err());
    assert_eq!(
        run.wait().await,
        Err(StepRunError::SupervisorGone { tick: run.tick() })
    );
    tokio::time::timeout(Duration::from_secs(5), stopped.wait_for(|stop| *stop)).await??;
    assert_eq!(steps.clock().phase, ClockPhase::Stopped);
    assert!(matches!(
        steps.try_step(request(), Duration::from_secs(1)),
        Err(StepStartError::SupervisorGone)
    ));
    Ok(())
}

#[tokio::test]
async fn dropping_unpolled_driver_also_terminates_accepted_observers() -> crate::Result<()> {
    use crate::component::step::{StepHandle, driver::Driver, worker::Executor};
    let scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
    let gate = Gate::new(scheduler.clock());
    gate.activate();
    let (requests, received) = tokio::sync::mpsc::unbounded_channel();
    let steps = StepHandle {
        gate: gate.clone(),
        requests,
        plan: scheduler.plan.clone(),
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let (control, _controlled) = tokio::sync::mpsc::channel(1);
    let driver = Driver {
        scheduler,
        gate,
        requests: received,
        executor: Box::new(Executor {
            calls: std::collections::BTreeMap::new(),
            failure: None,
        }),
    }
    .run(stop, control);
    let run = steps.try_step(request(), Duration::from_secs(5))?;
    drop(driver);
    assert_eq!(
        run.wait().await,
        Err(StepRunError::SupervisorGone { tick: run.tick() })
    );
    assert!(*stopped.borrow());
    assert_eq!(steps.clock().phase, ClockPhase::Stopped);
    Ok(())
}
