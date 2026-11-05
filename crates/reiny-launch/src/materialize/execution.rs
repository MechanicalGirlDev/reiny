use super::{
    AtomicUsize, BTreeMap, DeploymentPlan, Events, Group, GroupState, Mutex, Ordering, Output,
    Path, PrepareError, PrepareOptions, Result,
};

pub(super) fn execute_groups(
    groups: &[Group],
    plan: &DeploymentPlan,
    cache: &Path,
    options: &PrepareOptions<'_>,
    events: &Events<'_>,
) -> Result<Vec<Output>> {
    let next = AtomicUsize::new(0);
    let outputs = Mutex::new(BTreeMap::new());
    let failure = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..options.jobs.min(groups.len()) {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(group) = groups.get(index) else {
                        break;
                    };
                    if options.cancel.is_cancelled()
                        || failure
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .is_some()
                    {
                        group.event(plan, events, GroupState::Cancelled);
                        continue;
                    }
                    group.event(plan, events, GroupState::Running);
                    let result = group.execute(plan, cache, options, events);
                    match result {
                        Ok(output) => {
                            group.event(
                                plan,
                                events,
                                if matches!(&output, Output::Build(built) if built.fresh) {
                                    GroupState::Cached
                                } else {
                                    GroupState::Done
                                },
                            );
                            outputs
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .insert(index, output);
                        }
                        Err(error) => {
                            let cancelled = error
                                .downcast_ref::<PrepareError>()
                                .is_some_and(|error| matches!(error, PrepareError::Cancelled));
                            group.event(
                                plan,
                                events,
                                if cancelled {
                                    GroupState::Cancelled
                                } else {
                                    GroupState::Failed
                                },
                            );
                            let mut failure = failure
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            if failure.is_none() {
                                *failure = Some(error);
                            }
                            options.cancel.cancel();
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = failure
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        return Err(error);
    }
    options.cancel.check()?;
    Ok(outputs
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .into_values()
        .collect())
}
