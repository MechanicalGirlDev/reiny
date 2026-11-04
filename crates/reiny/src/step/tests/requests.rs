use super::*;
use crate::step::{ClockPhase, SchedulerError, StepRequestError, StepScheduler};

#[tokio::test]
async fn malformed_requests_are_rejected_without_dispatch_or_fault() -> anyhow::Result<()> {
    let mut zero = request(2);
    zero.dt = Duration::ZERO;
    let mut missing = request(2);
    missing.inputs.clear();
    let mut extra = request(2);
    extra.inputs.insert(input("controller", "extra"), value(99));
    let mut internal = request(2);
    internal
        .inputs
        .insert(input("physics", "command"), value(99));
    let mut wrong_name = request(2);
    wrong_name.inputs.insert(
        input("controller", "action"),
        StepValue::encode(&OtherNumber { value: 2 }),
    );
    let mut wrong_schema = request(2);
    wrong_schema.inputs.insert(
        input("controller", "action"),
        StepValue::encode(&ChangedNumber { value: 2 }),
    );

    for (request, expected) in [
        (zero, StepRequestError::ZeroDt),
        (
            missing,
            StepRequestError::MissingInput(input("controller", "action")),
        ),
        (
            extra,
            StepRequestError::UnexpectedInput(input("controller", "extra")),
        ),
        (
            internal,
            StepRequestError::UnexpectedInput(input("physics", "command")),
        ),
        (
            wrong_name,
            StepRequestError::TypeMismatch {
                input: input("controller", "action"),
                expected: StepType::of::<Number>(),
                actual: StepType::of::<OtherNumber>(),
            },
        ),
        (
            wrong_schema,
            StepRequestError::TypeMismatch {
                input: input("controller", "action"),
                expected: StepType::of::<Number>(),
                actual: StepType::of::<ChangedNumber>(),
            },
        ),
    ] {
        let mut scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
        let before = scheduler.clock();
        let mut executor = Arithmetic::default();
        assert_eq!(
            scheduler.advance(&mut executor, request).await,
            Err(SchedulerError::Request(expected))
        );
        assert_eq!(scheduler.clock(), before);
        assert_eq!(executor.visits, Vec::new());
        scheduler.advance(&mut executor, super::request(2)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn time_overflow_is_rejected_without_dispatch() -> anyhow::Result<()> {
    let mut scheduler = StepScheduler::new(graph(), Duration::MAX)?;
    let mut executor = Arithmetic::default();
    assert_eq!(
        scheduler.advance(&mut executor, request(2)).await,
        Err(SchedulerError::Request(StepRequestError::TimeOverflow))
    );
    assert_eq!(scheduler.clock().time, Duration::MAX);
    assert_eq!(scheduler.clock().next_index, 0);
    assert_eq!(scheduler.clock().phase, ClockPhase::Ready);
    assert_eq!(executor.visits, Vec::new());
    Ok(())
}
