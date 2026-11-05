use super::{Context, Events, PrepareOptions, Result, fixture, fs, stage, transaction};
use crate::{BuildSpec, CommandBuild, GroupState, PrepareEvent, PrepareObserver, PreparePhase};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[test]
fn host_builds_freeze_exact_outputs_without_starting_them_or_using_stale_results() -> Result<()> {
    let (root, mut plan) = fixture()?;
    for host in plan.component_hosts.values_mut() {
        host.spec.build = Some(BuildSpec::Command(CommandBuild {
            argv: vec!["sh".into(), "-c".into(), "printf 'build\\n' >> builds; mkdir -p out; printf '#!/bin/sh\\ntouch host-started\\n' > out/host; chmod +x out/host".into()],
            cwd: root.path().to_owned(),
            env: BTreeMap::new(),
            outputs: BTreeMap::from([("host".into(), "out/host".into())]),
            files: BTreeMap::new(),
        }));
    }

    let prepared = stage(&plan)?;

    assert_eq!(fs::read_to_string(root.path().join("builds"))?, "build\n");
    assert!(!root.path().join("host-started").exists());
    assert_eq!(
        fs::read_to_string(&prepared.component_hosts["control"].executable)?,
        "#!/bin/sh\ntouch host-started\n"
    );
    assert!(prepared.component_hosts["control"].spec.build.is_some());
    for host in plan.component_hosts.values_mut() {
        let Some(BuildSpec::Command(build)) = &mut host.spec.build else {
            anyhow::bail!("command fixture");
        };
        build.argv = vec!["sh".into(), "-c".into(), "exit 23".into()];
    }
    let error = stage(&plan)
        .err()
        .context("failed current builder must not use old output")?;
    assert!(format!("{error:#}").contains("23"), "{error:#}");
    prepared.verify()?;
    assert!(!root.path().join("host-started").exists());
    Ok(())
}

struct RemoveSource {
    source: std::path::PathBuf,
    removal: Mutex<Option<std::io::Result<()>>>,
    events: Mutex<Vec<PrepareEvent>>,
}

impl PrepareObserver for RemoveSource {
    fn event(&self, event: &PrepareEvent) {
        if matches!(
            event,
            PrepareEvent::Phase {
                phase: PreparePhase::Freezing
            }
        ) {
            *self
                .removal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(fs::remove_file(&self.source));
        }
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
    }
}

#[test]
fn first_snapshot_failure_cancels_remaining_groups_and_cleans_staging() -> Result<()> {
    let (root, plan) = fixture()?;
    let observer = RemoveSource {
        source: root.path().join("zenoh.json5"),
        removal: Mutex::new(None),
        events: Mutex::new(Vec::new()),
    };
    let options = PrepareOptions {
        observer: Some(&observer),
        jobs: 1,
        build_cache: Some(root.path().join(".private-build")),
        ..PrepareOptions::default()
    };

    let error = transaction(&plan, &options, &Events::new(Some(&observer)))
        .err()
        .context("source disappears after planning")?;

    assert!(format!("{error:#}").contains("bundle source is not a file"));
    observer
        .removal
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .context("source removal not reached")??;
    let events = observer
        .events
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(events.iter().any(|event| matches!(
        event,
        PrepareEvent::Group {
            state: GroupState::Failed,
            ..
        }
    )));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                PrepareEvent::Group {
                    state: GroupState::Cancelled,
                    ..
                }
            ))
            .count(),
        3
    );
    let cache = root.path().join(".reiny/config");
    assert!(
        fs::read_dir(cache)?.next().is_none(),
        "failed staging leaves no temporary bundle"
    );
    assert!(!root.path().join(".reiny/control.json").exists());
    let executable = root.path().join(".reiny/build");
    let bundle = fs::read_dir(executable)?
        .next()
        .context("completed build is immutable cache")??
        .path();
    crate::artifacts::verify_directory(&bundle, &crate::CancelToken::default())?;
    Ok(())
}
