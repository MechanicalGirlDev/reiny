use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

use super::{
    fault_executor::{Fault, Faulting},
    *,
};
use crate::step::{ClockPhase, SchedulerError, StepFailureKind, StepScheduler};

#[tokio::test]
async fn failed_nodes_preserve_committed_clock_and_permanently_fault_epoch() -> anyhow::Result<()> {
    for (fault, kind, port) in [
        (Fault::Error, StepFailureKind::NodeError, None),
        (Fault::InvokePanic, StepFailureKind::NodePanicked, None),
        (Fault::PollPanic, StepFailureKind::NodePanicked, None),
        (Fault::DropPanic, StepFailureKind::NodePanicked, None),
        (
            Fault::MissingOutput,
            StepFailureKind::InvalidOutputs,
            Some("out"),
        ),
        (
            Fault::ExtraOutput,
            StepFailureKind::InvalidOutputs,
            Some("extra"),
        ),
        (
            Fault::WrongName,
            StepFailureKind::InvalidOutputs,
            Some("out"),
        ),
        (
            Fault::WrongFingerprint,
            StepFailureKind::InvalidOutputs,
            Some("out"),
        ),
    ] {
        let mut scheduler = StepScheduler::new(graph(), Duration::from_secs(10))?;
        scheduler
            .advance(&mut Arithmetic::default(), request(2))
            .await?;
        let before = scheduler.clock();
        let mut executor = Faulting::new(fault);

        let result = scheduler.advance(&mut executor, request(3)).await;

        let Err(SchedulerError::Failed(failure)) = result else {
            anyhow::bail!("expected a new epoch fault, got {result:?}");
        };
        assert_eq!(failure.kind, kind, "{fault:?}");
        assert_eq!(failure.tick.index, 1);
        assert_eq!(failure.tick.epoch, before.epoch);
        assert_eq!(failure.instance.as_deref(), Some("physics"));
        assert_eq!(failure.port.as_deref(), port);
        assert_eq!(failure.attempted, ["controller", "physics"]);
        assert_eq!(failure.completed, ["controller"]);
        assert_eq!(executor.effects, 1);
        assert_eq!(scheduler.clock().time, before.time);
        assert_eq!(scheduler.clock().next_index, before.next_index);
        assert_eq!(
            scheduler.clock().phase,
            ClockPhase::Faulted(*failure.clone())
        );
        assert_eq!(
            scheduler.advance(&mut executor, request(3)).await,
            Err(SchedulerError::Faulted(failure))
        );
        assert_eq!(executor.effects, 1);
    }
    Ok(())
}

#[test]
fn dropping_after_dispatch_faults_even_when_the_node_never_completes() -> anyhow::Result<()> {
    let mut scheduler = StepScheduler::new(graph(), Duration::from_secs(7))?;
    let before = scheduler.clock();
    let mut executor = Faulting::new(Fault::Pending);
    let mut future = Box::pin(scheduler.advance(&mut executor, request(2)));

    assert_eq!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    );
    drop(future);

    let ClockPhase::Faulted(failure) = scheduler.clock().phase else {
        anyhow::bail!("a dispatched advancement must fault when abandoned");
    };
    assert_eq!(failure.kind, StepFailureKind::AdvanceDropped);
    assert_eq!(failure.instance.as_deref(), Some("physics"));
    assert_eq!(failure.attempted, ["controller", "physics"]);
    assert_eq!(failure.completed, ["controller"]);
    assert_eq!(executor.effects, 1);
    assert_eq!(scheduler.clock().time, before.time);
    assert_eq!(scheduler.clock().next_index, before.next_index);
    Ok(())
}

#[tokio::test]
async fn dropping_an_unpolled_advance_leaves_the_epoch_usable() -> anyhow::Result<()> {
    let mut scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
    let before = scheduler.clock();
    let mut executor = Arithmetic::default();

    drop(scheduler.advance(&mut executor, request(99)));

    assert_eq!(scheduler.clock(), before);
    assert_eq!(executor.visits, Vec::new());
    let receipt = scheduler.advance(&mut executor, request(2)).await?;
    assert_eq!(receipt.tick.id.index, 0);
    assert_eq!(
        receipt.outputs[&output("physics")]
            .decode::<Number>()?
            .value,
        14
    );
    Ok(())
}

#[test]
fn dropping_a_rejected_polled_request_leaves_the_epoch_usable() -> anyhow::Result<()> {
    let mut scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
    let before = scheduler.clock();
    let mut executor = Arithmetic::default();
    let mut invalid = request(1);
    invalid.dt = Duration::ZERO;
    let mut future = Box::pin(scheduler.advance(&mut executor, invalid));

    assert!(matches!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(SchedulerError::Request(_)))
    ));
    drop(future);

    assert_eq!(scheduler.clock(), before);
    assert_eq!(executor.visits, Vec::new());
    Ok(())
}

#[test]
fn first_dispatch_is_recorded_before_invoke_panics() -> anyhow::Result<()> {
    struct Panics;
    impl StepExecutor for Panics {
        fn invoke<'a>(&'a mut self, _: &'a str, _: StepTick, _: StepInputs) -> StepFuture<'a> {
            std::panic::resume_unwind(Box::new("before returning a future"))
        }
    }
    let mut scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
    let mut executor = Panics;
    let mut future = Box::pin(scheduler.advance(&mut executor, request(2)));

    let result = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));

    let Poll::Ready(Err(SchedulerError::Failed(failure))) = result else {
        anyhow::bail!("synchronous panic must return a node failure");
    };
    assert_eq!(failure.kind, StepFailureKind::NodePanicked);
    assert_eq!(failure.attempted, ["controller"]);
    assert_eq!(failure.completed, Vec::<String>::new());
    Ok(())
}
