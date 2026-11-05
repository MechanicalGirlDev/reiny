use super::{
    Action, AtomicBool, DeploymentPhase, ModuleObserver, Ordering, Owner, PreparedDeployment,
    Result, reply,
};

/// Run one deployment owner until stop, cancellation, or an owner error.
///
/// `on_listening` runs after the lock and authenticated endpoint are published,
/// before process startup. It may notify a parent rendezvous but must not make
/// a synchronous request back to this event loop.
pub fn serve(
    prepared: PreparedDeployment,
    observer: &dyn ModuleObserver,
    stop: &AtomicBool,
    on_listening: &dyn Fn() -> Result<()>,
) -> Result<()> {
    serve_remote(
        prepared,
        std::collections::BTreeMap::new(),
        observer,
        stop,
        on_listening,
    )
}

/// Run explicit SSH process hosts alongside ordinary local process hosts.
///
/// Remote assignments are immutable for this owner's lifetime. A changed apply
/// is refused: stop, inspect Unknown ownership, reconcile exact generations, and
/// create a new owner with explicit targets. The old `serve` API stays local.
pub fn serve_remote(
    prepared: PreparedDeployment,
    targets: std::collections::BTreeMap<String, super::SshTarget>,
    observer: &dyn ModuleObserver,
    stop: &AtomicBool,
    on_listening: &dyn Fn() -> Result<()>,
) -> Result<()> {
    anyhow::ensure!(
        targets
            .keys()
            .all(|name| prepared.component_hosts.contains_key(name)),
        "SSH targets must name prepared component hosts"
    );
    let mut owner = Owner::new(prepared, observer)?;
    owner.remote = targets;
    let mut result = (|| {
        on_listening()?;
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        observer.configure(&owner.desired)?;
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        owner.reconcile()?;
        owner.publish()?;
        owner.run(stop)
    })();
    if let Err(error) = owner.stop_all() {
        result = Err(match result {
            Ok(()) => error,
            Err(original) => original.context(format!("cleanup also failed: {error:#}")),
        });
    }
    if let Err(error) = super::remote::refuse_unresolved(&owner.desired.root) {
        result = Err(match result {
            Ok(()) => error,
            Err(original) => original.context(format!("remote ownership retained: {error:#}")),
        });
    }
    if let Err(error) = owner.remove_endpoint() {
        result = Err(match result {
            Ok(()) => error,
            Err(original) => original.context(format!("endpoint cleanup also failed: {error:#}")),
        });
    }
    owner.status.owner_alive = false;
    if let Err(error) = &result {
        owner.status.phase = DeploymentPhase::Failed;
        owner.status.error = Some(format!("{error:#}"));
    } else {
        owner.status.phase = DeploymentPhase::Stopped;
    }
    if let Err(error) = owner.publish() {
        result = Err(match result {
            Ok(()) => error.context("persisting final deployment status"),
            Err(original) => original.context(format!("final persistence also failed: {error:#}")),
        });
    }
    for waiter in owner.waiters.drain(..) {
        let response = if matches!(waiter.action, Action::WaitStopped | Action::WaitTerminal)
            && result.is_ok()
        {
            Ok(owner.status.clone())
        } else {
            Err(anyhow::anyhow!(
                "deployment owner failed: {}",
                result.as_ref().err().map_or_else(
                    || "owner stopped before the requested state".to_owned(),
                    |error| format!("{error:#}")
                )
            ))
        };
        reply(waiter.stream, response);
    }
    result
}
