use super::*;

struct NoProcesses;

impl ModuleObserver for NoProcesses {
    fn ready(&self, _namespace: &str) -> Result<bool> {
        Ok(false)
    }

    fn request_stop(&self, _namespace: &str) -> Result<()> {
        Ok(())
    }
}

#[test]
fn completed_revision_notifies_before_the_next_desired_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let prepared = PreparedDeployment {
        root: root.path().to_owned(),
        deployment: format!(
            "terminal-{}",
            root.path()
                .file_name()
                .context("temp directory")?
                .to_string_lossy()
        ),
        domain: "terminal-tests".into(),
        nodes: Vec::new(),
        components: Vec::new(),
        component_hosts: BTreeMap::new(),
        resources: BTreeMap::new(),
    };
    let mut owner = Owner::new(prepared, &NoProcesses)?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let stream = TcpStream::connect(listener.local_addr()?)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    let subscription = TerminalSubscription {
        stream,
        owner_generation: owner.status.owner_generation.clone(),
    };
    owner.waiters.push(Waiter {
        stream: listener.accept()?.0,
        action: Action::WaitTerminal,
        deadline: None,
    });
    owner.status.phase = DeploymentPhase::Completed;
    owner.publish()?;
    let completed_revision = owner.status.revision;
    owner.status.phase = DeploymentPhase::Starting;
    owner.publish()?;
    let completed = subscription.wait()?;
    assert_eq!(completed.phase, DeploymentPhase::Completed);
    assert_eq!(completed.revision, completed_revision);
    assert!(completed.owner_alive);
    assert!(owner.waiters.is_empty());
    Ok(())
}

#[test]
fn hosted_status_refreshes_changed_frozen_resources() -> Result<()> {
    let root = tempfile::Builder::new()
        .prefix("resource-status-")
        .tempdir()?;
    std::fs::create_dir(root.path().join("bin"))?;
    std::fs::create_dir(root.path().join("app"))?;
    std::fs::write(
        root.path()
            .join("bin")
            .join(format!("host{}", std::env::consts::EXE_SUFFIX)),
        "host bytes",
    )?;
    std::fs::write(
        root.path().join("app/main.yaml"),
        "version: 4\ncomponent: {factory: unit, factory_revision: '1'}\n",
    )?;
    std::fs::write(root.path().join("resource.bin"), "first")?;
    let domain = root
        .path()
        .file_name()
        .context("fixture root")?
        .to_string_lossy();
    std::fs::write(
        root.path().join("main.yaml"),
        format!(
            r"version: 4
deployment: resource-status
domain: resource-{domain}
providers:
  local: {{type: process, bin_dir: bin}}
  files: {{type: artifact}}
component_hosts:
  host: {{provider: local, bin: host}}
placement:
  resource-status/app: host
modules:
  app: {{source: app}}
resources:
  table: {{type: artifact.file, provider: files, source: resource.bin}}
"
        ),
    )?;
    let plan = crate::DeploymentPlan::load(root.path(), false)?;
    let mut owner = Owner::new(plan.prepare()?, &NoProcesses)?;
    std::fs::write(root.path().join("resource.bin"), "second")?;
    let desired = plan.prepare()?;
    let expected = desired.resources.clone();
    assert_ne!(owner.status.resources, expected);
    owner.desired = desired;
    owner.install_desired_status();
    assert_eq!(owner.status.resources, expected);
    Ok(())
}
