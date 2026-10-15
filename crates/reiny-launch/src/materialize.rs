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
    nodes: Vec<usize>,
    bins: BTreeSet<String>,
    work: Work,
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

impl Group {
    fn event(&self, plan: &DeploymentPlan, events: &Events<'_>, state: GroupState) {
        events.emit(PrepareEvent::Group {
            id: self.id.clone(),
            kind: self.kind,
            state,
            nodes: self
                .nodes
                .iter()
                .map(|&index| plan.nodes[index].namespace.clone())
                .collect(),
            bins: self.bins.iter().cloned().collect(),
        });
    }

    fn execute(
        &self,
        plan: &DeploymentPlan,
        cache: &Path,
        options: &PrepareOptions<'_>,
        events: &Events<'_>,
    ) -> Result<Output> {
        let cancel = &options.cancel;
        cancel.check()?;
        match &self.work {
            Work::Build(build) => Ok(Output::Build(build.execute(
                &self.bins,
                &cache.join("build"),
                cancel,
                events,
                &self.id,
            )?)),
            Work::Prebuilt(directory) => {
                let executables: Vec<_> = self
                    .bins
                    .iter()
                    .map(|bin| directory.join(artifacts::executable_name(bin)))
                    .collect();
                let first = executables.first().context("empty prebuilt group")?;
                let staged = artifacts::stage_prebuilt_bundle(
                    first,
                    &executables[1..],
                    &cache.join("build"),
                    cancel,
                )?;
                cancel.check()?;
                let directory = staged.parent().context("prebuilt snapshot directory")?;
                Ok(Output::Build(Built {
                    executables: self
                        .bins
                        .iter()
                        .map(|bin| (bin.clone(), directory.join(artifacts::executable_name(bin))))
                        .collect(),
                    fresh: true,
                }))
            }
            Work::Config { files, entry, .. } => {
                let (path, bundle) =
                    prepared::freeze_config(files, entry, &cache.join("config"), cancel)?;
                Ok(Output::Config(path, bundle))
            }
            Work::Resource(index) => {
                let resource = &plan.resources[*index];
                let name = PathBuf::from(resource.source.file_name().context("resource filename")?);
                let files = BTreeMap::from([(name.clone(), resource.source.clone())]);
                let path =
                    artifacts::stage_files_with(&files, &cache.join("artifacts"), &name, cancel)?;
                if let Some(expected) = &resource.sha256 {
                    let found = prepared::digest_file(&path)?;
                    ensure!(
                        &found == expected,
                        "artifact {} SHA-256 mismatch: expected {expected}, found {found}",
                        resource.address,
                    );
                }
                Ok(Output::Resource(path))
            }
        }
    }
}

pub(crate) fn prepare(
    plan: &DeploymentPlan,
    options: &PrepareOptions<'_>,
) -> Result<PreparedDeployment, PrepareError> {
    let events = Events::new(options.observer);
    events.emit(PrepareEvent::Started);
    events.emit(PrepareEvent::Phase {
        phase: PreparePhase::Resolving,
    });
    let result = transaction(plan, options, &events);
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
        let prepared = assemble(plan, root, &groups, outputs)?;
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

fn assemble(
    plan: &DeploymentPlan,
    root: PathBuf,
    groups: &[Group],
    outputs: Vec<Output>,
) -> Result<PreparedDeployment> {
    let mut nodes = Vec::with_capacity(plan.nodes.len());
    for node in &plan.nodes {
        nodes.push(PreparedModule {
            namespace: node.namespace.clone(),
            module_dir: node.module_dir.clone(),
            executable: PathBuf::new(),
            bundle_dir: PathBuf::new(),
            config_dir: None,
            config_bundle_dir: None,
            run: node.run.clone(),
            provider: node.provider.clone(),
            bindings: node.bindings.clone(),
            fingerprint: String::new(),
        });
    }
    let mut resources = BTreeMap::new();
    for (group, output) in groups.iter().zip(outputs) {
        match output {
            Output::Build(built) => {
                for &index in &group.nodes {
                    let node = &mut nodes[index];
                    node.executable = built
                        .executables
                        .get(&node.run.bin)
                        .context("missing prepared executable")?
                        .clone();
                    node.bundle_dir = node
                        .executable
                        .parent()
                        .context("bundle directory")?
                        .to_owned();
                }
            }
            Output::Config(path, bundle) => {
                for &index in &group.nodes {
                    let node = &mut nodes[index];
                    if matches!(group.work, Work::Config { zenoh: true, .. }) {
                        node.provider.zenoh_config = Some(path.clone());
                    } else {
                        node.run.config = Some(path.clone());
                        node.config_dir = Some(path.parent().context("config parent")?.to_owned());
                        node.config_bundle_dir = Some(bundle.clone());
                    }
                }
            }
            Output::Resource(path) => {
                if let Work::Resource(index) = group.work {
                    resources.insert(plan.resources[index].address.clone(), path);
                }
            }
        }
    }
    for node in &mut nodes {
        node.fingerprint = prepared::fingerprint(node, &plan.domain)?;
    }
    let prepared = PreparedDeployment {
        root,
        deployment: plan.deployment.clone(),
        domain: plan.domain.clone(),
        nodes,
        resources,
    };
    Ok(prepared)
}

fn plan_groups(
    plan: &DeploymentPlan,
    base: &Path,
    environment: &Environment,
) -> Result<Vec<Group>> {
    let mut builds: BTreeMap<String, Group> = BTreeMap::new();
    let mut frozen = Vec::new();
    for (index, node) in plan.nodes.iter().enumerate() {
        node.run.validate(&node.namespace)?;
        let mut bins = BTreeSet::new();
        for bin in std::iter::once(&node.run.bin).chain(&node.run.companions) {
            artifacts::validate_executable_name(bin)?;
            ensure!(bins.insert(bin.clone()), "duplicate executable name {bin}");
        }
        let (id, kind, work) = if let Some(spec) = &node.build {
            let build = BuildRequest::new(spec, &node.module_dir, base, environment)?;
            if let artifacts::BuildSpec::Command(command) = &build.spec {
                for bin in &bins {
                    ensure!(
                        command.outputs.contains_key(bin),
                        "{}: command outputs does not declare {bin}",
                        node.namespace
                    );
                }
            }
            (
                format!("build-{}", build.key),
                build.kind(),
                Work::Build(Box::new(build)),
            )
        } else {
            let directory = node.provider.bin_dir.canonicalize().with_context(|| {
                format!("prebuilt directory {}", node.provider.bin_dir.display())
            })?;
            for bin in &bins {
                ensure!(
                    directory.join(artifacts::executable_name(bin)).is_file(),
                    "prepared binary not found: {}",
                    directory.join(artifacts::executable_name(bin)).display()
                );
            }
            (
                format!(
                    "prebuilt-{}",
                    prepared::digest(&serde_json::to_vec(&directory)?)
                ),
                GroupKind::Prebuilt,
                Work::Prebuilt(directory),
            )
        };
        let group = builds.entry(id.clone()).or_insert_with(|| Group {
            id,
            kind,
            nodes: Vec::new(),
            bins: BTreeSet::new(),
            work,
        });
        group.nodes.push(index);
        group.bins.extend(bins);
        frozen.extend(plan_configs(node, index)?);
    }
    for (index, resource) in plan.resources.iter().enumerate() {
        ensure!(
            resource.source.is_file(),
            "artifact {} source not found: {}",
            resource.address,
            resource.source.display()
        );
        frozen.push(Group {
            id: format!("resource-{}", resource.address),
            kind: GroupKind::Resource,
            nodes: Vec::new(),
            bins: BTreeSet::new(),
            work: Work::Resource(index),
        });
    }
    Ok(builds.into_values().chain(frozen).collect())
}

fn plan_configs(node: &crate::modules::ExecutableModule, index: usize) -> Result<Vec<Group>> {
    let mut frozen = Vec::new();
    ensure!(
        node.run.config.is_some() || node.run.config_assets.is_empty(),
        "{}: config_assets requires run.config",
        node.namespace
    );
    for (config, assets, zenoh) in [
        (
            node.run.config.as_ref(),
            node.run.config_assets.as_slice(),
            false,
        ),
        (node.provider.zenoh_config.as_ref(), [].as_slice(), true),
    ] {
        if let Some(config) = config {
            let (files, entry) = prepared::config_files(config, assets).with_context(|| {
                format!(
                    "{}: module {}; selected config {}; declared assets {:?}",
                    node.namespace,
                    node.module_dir.display(),
                    config.display(),
                    assets
                )
            })?;
            frozen.push(Group {
                id: format!(
                    "config-{}-{}",
                    node.namespace,
                    if zenoh { "zenoh" } else { "app" }
                ),
                kind: GroupKind::Config,
                nodes: vec![index],
                bins: BTreeSet::new(),
                work: Work::Config {
                    files,
                    entry,
                    zenoh,
                },
            });
        }
    }
    Ok(frozen)
}

fn execute_groups(
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
