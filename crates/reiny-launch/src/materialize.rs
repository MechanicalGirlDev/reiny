//! Plan compatible groups once, execute bounded work, then assemble frozen nodes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Result, ensure};

use crate::artifacts::{self, BuildRequest, Built, Environment};
use crate::file_lock::FileLock;
use crate::modules::DeploymentPlan;
use crate::prepared::{self, PreparedDeployment, PreparedModule};
use crate::progress::{
    Events, GroupKind, GroupState, PrepareError, PrepareEvent, PrepareOptions, PreparePhase,
    PrepareStatus,
};

struct Group {
    id: String,
    kind: GroupKind,
    members: Vec<Member>,
    bins: BTreeSet<String>,
    work: Work,
}

#[derive(Clone)]
enum Member {
    Module(usize),
    Host(String),
    Component(usize),
}

enum Work {
    Build(Box<BuildRequest>),
    Prebuilt(PathBuf),
    Config {
        files: BTreeMap<PathBuf, PathBuf>,
        entry: PathBuf,
        zenoh: bool,
    },
    Resource(usize),
}

enum Output {
    Build(Built),
    Config(PathBuf, PathBuf),
    Resource(PathBuf),
}

mod assembly;
mod execution;
mod group;
mod hosted;
mod hosted_assembly;
mod planning;
use assembly::assemble;
use execution::execute_groups;
use planning::plan_groups;

#[cfg(test)]
mod tests;

pub(crate) fn prepare(
    plan: &DeploymentPlan,
    options: &PrepareOptions<'_>,
) -> Result<PreparedDeployment, PrepareError> {
    let events = Events::new(options.observer);
    events.emit(PrepareEvent::Started);
    events.emit(PrepareEvent::Phase {
        phase: PreparePhase::Resolving,
    });
    let result = options
        .cancel
        .check()
        .and_then(|()| transaction(plan, options, &events));
    let (status, error) = match &result {
        Ok(_) => (PrepareStatus::Success, None),
        Err(PrepareError::Cancelled) => (
            PrepareStatus::Cancelled,
            Some("preparation cancelled".to_owned()),
        ),
        Err(error) => (PrepareStatus::Failed, Some(format!("{error:#}"))),
    };
    events.emit(PrepareEvent::Finished { status, error });
    result
}

fn transaction(
    plan: &DeploymentPlan,
    options: &PrepareOptions<'_>,
    events: &Events<'_>,
) -> Result<PreparedDeployment, PrepareError> {
    options.cancel.check()?;
    let result = (|| -> Result<PreparedDeployment> {
        hosted::validate_plan(plan)?;
        ensure!(options.jobs > 0, "prepare jobs must be positive");
        let root = plan
            .root
            .canonicalize()
            .context("canonicalize preparation root")?;
        let cache = root.join(".reiny");
        std::fs::create_dir_all(&cache)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(cache.join("prepare.lock"))?;
        let _root_lock = match FileLock::new(file) {
            Ok(lock) => lock,
            Err(std::fs::TryLockError::WouldBlock) => return Err(PrepareError::Busy.into()),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        };
        events.emit(PrepareEvent::Phase {
            phase: PreparePhase::Planning,
        });
        let environment: Environment = std::env::vars_os().collect();
        let build_base = options
            .build_cache
            .clone()
            .or_else(|| {
                environment
                    .get(std::ffi::OsStr::new("CARGO_TARGET_DIR"))
                    .map(PathBuf::from)
            })
            .unwrap_or_else(|| cache.join("build"));
        std::fs::create_dir_all(&build_base)?;
        let build_base = build_base.canonicalize()?;
        let groups = plan_groups(plan, &build_base, &environment)?;
        // Failure stops sibling builders without turning a build failure into
        // caller cancellation. Parent cancellation still propagates immediately.
        let work_options = PrepareOptions {
            cancel: options.cancel.child(),
            observer: options.observer,
            jobs: options.jobs,
            build_cache: options.build_cache.clone(),
        };
        events.emit(PrepareEvent::Planned {
            total_groups: groups.len(),
        });
        for group in &groups {
            group.event(plan, events, GroupState::Queued);
        }
        let mut outputs = Vec::with_capacity(groups.len());
        let build_count = groups
            .iter()
            .take_while(|group| matches!(group.work, Work::Build(_) | Work::Prebuilt(_)))
            .count();
        events.emit(PrepareEvent::Phase {
            phase: PreparePhase::Building,
        });
        let builds = execute_groups(&groups[..build_count], plan, &cache, &work_options, events);
        match builds {
            Ok(built) => outputs.extend(built),
            Err(error) => {
                for group in &groups[build_count..] {
                    group.event(plan, events, GroupState::Cancelled);
                }
                return Err(error);
            }
        }
        events.emit(PrepareEvent::Phase {
            phase: PreparePhase::Freezing,
        });
        outputs.extend(execute_groups(
            &groups[build_count..],
            plan,
            &cache,
            &work_options,
            events,
        )?);
        let prepared = assemble(plan, root, groups.iter().zip(outputs))?;
        prepared.verify_with(&options.cancel)?;
        events.emit(PrepareEvent::Phase {
            phase: PreparePhase::Writing,
        });
        options.cancel.publish(|| Ok(prepared)).map_err(Into::into)
    })();
    result.map_err(|error| match error.downcast::<PrepareError>() {
        Ok(error) => error,
        Err(error) => PrepareError::Failed(error),
    })
}
