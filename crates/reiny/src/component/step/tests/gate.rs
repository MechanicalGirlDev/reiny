use super::*;
use crate::component::step::tests::{graph, request};
use crate::step::{StepExecutor, StepFuture, StepInputs, StepScheduler, StepTick};

fn fixture() -> crate::Result<(StepScheduler, Arc<Gate>, Arc<Run>)> {
    let scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
    let gate = Gate::new(scheduler.clock());
    gate.activate();
    let run = gate.start(&request(), Duration::from_secs(5), &scheduler.plan)?;
    Ok((scheduler, gate, run))
}

fn prepared(run: &Run) -> (Arc<StepReceipt>, Arc<ClockSnapshot>) {
    let tick = StepTick {
        id: run.tick,
        start: Duration::ZERO,
        dt: Duration::from_secs(1),
        end: Duration::from_secs(1),
    };
    (
        Arc::new(StepReceipt {
            tick,
            outputs: std::collections::BTreeMap::new(),
        }),
        Arc::new(ClockSnapshot {
            epoch: run.tick.epoch,
            next_index: 1,
            time: tick.end,
            phase: ClockPhase::Ready,
        }),
    )
}

#[test]
fn stop_and_cancel_before_commit_never_apply_and_commit_before_cancel_retains_receipt()
-> crate::Result<()> {
    for stop in [true, false] {
        let (_, gate, run) = fixture()?;
        let (receipt, clock) = prepared(&run);
        let (entered, received) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let worker_gate = gate.clone();
        let worker_run = run.clone();
        let worker = std::thread::spawn(move || {
            let _ = entered.send(());
            released
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| error.to_string())?;
            let result = ControlledCommit {
                gate: &worker_gate,
                run: &worker_run,
            }
            .commit(&receipt, &clock, || 42);
            Ok::<_, String>(result)
        });
        received.recv_timeout(Duration::from_secs(5))?;
        if stop {
            gate.stop();
        } else {
            assert_eq!(gate.cancel(&run), CancelStatus::Requested);
        }
        release.send(())?;
        let result = worker
            .join()
            .map_err(|_| anyhow::anyhow!("commit thread panicked"))?
            .map_err(anyhow::Error::msg)?;
        assert_eq!(
            result,
            Err(if stop {
                StepFailureKind::HostStopped
            } else {
                StepFailureKind::Cancelled
            })
        );
        assert!(run.result.get().is_none());
        assert_eq!(gate.clock().next_index, 0);
    }
    let (_, gate, run) = fixture()?;
    let (receipt, clock) = prepared(&run);
    ControlledCommit {
        gate: &gate,
        run: &run,
    }
    .commit(&receipt, &clock, || ())
    .map_err(|kind| anyhow::anyhow!("{kind:?}"))?;
    assert_eq!(gate.cancel(&run), CancelStatus::Finished);
    gate.stop();
    assert_eq!(run.result.get(), Some(&Ok(receipt)));
    assert_eq!(gate.clock().next_index, 1);
    gate.disappear();
    assert_eq!(gate.cancel(&run), CancelStatus::Finished);
    Ok(())
}

#[tokio::test]
async fn deadline_is_rechecked_at_commit_even_after_successful_work() -> crate::Result<()> {
    let (_, gate, run) = fixture()?;
    let (receipt, clock) = prepared(&run);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    let mut applied = false;
    let result = ControlledCommit {
        gate: &gate,
        run: &run,
    }
    .commit(&receipt, &clock, || {
        applied = true;
    });
    assert_eq!(result, Err(StepFailureKind::TimedOut));
    assert!(!applied);
    assert_eq!(gate.clock().time, Duration::ZERO);
    Ok(())
}

#[test]
fn poisoned_gate_is_terminal_unknown_never_ready() -> crate::Result<()> {
    let (_, gate, run) = fixture()?;
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Ok(_state) = gate.state.lock() {
            std::panic::resume_unwind(Box::new("injected incomplete transaction"));
        }
    }));
    assert!(panicked.is_err());
    assert_eq!(gate.clock().phase, ClockPhase::Stopped);
    assert_eq!(gate.cancel(&run), CancelStatus::SupervisorGone);
    assert!(matches!(
        gate.start(
            &request(),
            Duration::from_secs(1),
            &StepScheduler::new(graph(), Duration::ZERO)?.plan
        ),
        Err(StepStartError::SupervisorGone)
    ));
    assert!(run.result.get().is_none());
    Ok(())
}

struct Sum;
impl StepExecutor for Sum {
    fn invoke<'a>(&'a mut self, _: &'a str, _: StepTick, inputs: StepInputs) -> StepFuture<'a> {
        Box::pin(async move {
            let value = inputs
                .into_values()
                .next()
                .ok_or_else(|| crate::step::NodeError {
                    message: "input absent".into(),
                })?;
            Ok(std::collections::BTreeMap::from([("out".into(), value)]))
        })
    }
}

struct CancelAtCommit {
    gate: Arc<Gate>,
    run: Arc<Run>,
}
impl Commit for CancelAtCommit {
    fn commit<T>(
        &self,
        receipt: &Arc<StepReceipt>,
        clock: &Arc<ClockSnapshot>,
        apply: impl FnOnce() -> T,
    ) -> Result<T, StepFailureKind> {
        assert_eq!(self.gate.cancel(&self.run), CancelStatus::Requested);
        ControlledCommit {
            gate: &self.gate,
            run: &self.run,
        }
        .commit(receipt, clock, apply)
    }
}

#[tokio::test]
async fn rejection_after_all_nodes_preserves_attempted_and_completed_without_commit()
-> crate::Result<()> {
    let (mut scheduler, gate, run) = fixture()?;
    let result = scheduler
        .advance_controlled(&mut Sum, request(), &CancelAtCommit { gate, run })
        .await;
    let Err(crate::step::SchedulerError::Failed(failure)) = result else {
        anyhow::bail!("expected failed commit");
    };
    assert_eq!(failure.kind, StepFailureKind::Cancelled);
    assert_eq!(failure.attempted, ["controller", "physics"]);
    assert_eq!(failure.completed, ["controller", "physics"]);
    assert_eq!(scheduler.clock().time, Duration::ZERO);
    assert_eq!(scheduler.clock().next_index, 0);
    Ok(())
}

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
            executor: Executor {
                calls: std::collections::BTreeMap::from([("controller".into(), calls)]),
                failure: None,
            },
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
        executor: Executor {
            calls: std::collections::BTreeMap::new(),
            failure: None,
        },
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

struct ReentrantObserver {
    gate: Arc<Gate>,
    fired: AtomicBool,
}

impl std::task::Wake for ReentrantObserver {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.fired.swap(true, Ordering::SeqCst) {
            self.gate.stop();
        }
    }
}

#[tokio::test]
async fn clock_wakers_can_reenter_stop_without_a_lock_or_stale_ready_publication()
-> crate::Result<()> {
    use std::future::Future;
    let (_, gate, run) = fixture()?;
    let mut clock_rx = gate.subscribe();
    let observer = Arc::new(ReentrantObserver {
        gate: gate.clone(),
        fired: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(observer.clone());
    let mut context = std::task::Context::from_waker(&waker);
    let mut changed = Box::pin(clock_rx.changed());
    assert!(changed.as_mut().poll(&mut context).is_pending());
    let (receipt, clock) = prepared(&run);
    ControlledCommit {
        gate: &gate,
        run: &run,
    }
    .commit(&receipt, &clock, || ())
    .map_err(|kind| anyhow::anyhow!("{kind:?}"))?;
    assert!(observer.fired.load(Ordering::SeqCst));
    assert_eq!(gate.clock().phase, ClockPhase::Stopped);
    assert_eq!(gate.subscribe().borrow().phase, ClockPhase::Stopped);
    assert_eq!(run.result.get(), Some(&Ok(receipt)));
    Ok(())
}

#[test]
fn hosted_stop_and_readiness_loss_mark_commit_gate_before_worker_observation() -> crate::Result<()>
{
    for readiness_loss in [false, true] {
        let (_, gate, run) = fixture()?;
        let shutdown = crate::shutdown::Shutdown::hosted();
        shutdown.attach_step_gate(gate.clone());
        if readiness_loss {
            shutdown.hosted_event(crate::managed::HostedEvent::ReadinessLost);
        } else {
            shutdown.trigger();
        }
        // No worker or async observer has been polled.
        let (receipt, clock) = prepared(&run);
        let mut applied = false;
        let result = ControlledCommit {
            gate: &gate,
            run: &run,
        }
        .commit(&receipt, &clock, || {
            applied = true;
        });
        assert_eq!(result, Err(StepFailureKind::HostStopped));
        assert!(!applied);
    }
    let (_, gate, run) = fixture()?;
    let shutdown = crate::shutdown::Shutdown::hosted();
    shutdown.trigger();
    shutdown.attach_step_gate(gate.clone());
    assert_eq!(gate.interruption(&run), Some(StepFailureKind::HostStopped));
    Ok(())
}
