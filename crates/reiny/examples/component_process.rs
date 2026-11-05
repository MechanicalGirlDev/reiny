//! Runnable application-owned process endpoint.
//!
//! `cargo run -p reiny --example component_process -- --self-test` spawns this
//! executable as a real child and performs a validated loopback admission.
//! Without `--self-test`, stdin must carry one private framed Bootstrap.

use std::sync::Arc;

use reiny::component::process::protocol::{FactoryRegistration, HostOutcome};
use reiny::component::process::{Bootstrap, ProcessHost, ProcessLimits, ProcessRegistry};
use reiny::component::{
    Component, ComponentContext, ComponentFactory, ComponentFuture, StopReason,
};

#[path = "component_process/owner.rs"]
mod owner;

struct Factory;
struct Service;

impl ComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        Ok(Box::new(Service))
    }
}

impl Component for Service {
    fn prepare<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            println!("run");
            ctx.shutdown().await;
            Ok(())
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async {
            println!("stop");
            Ok(())
        })
    }
}

fn main() -> reiny::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--self-test") {
        return owner::exercise();
    }
    let bootstrap = Bootstrap::read(&mut std::io::stdin().lock())?;
    let mut registry = ProcessRegistry::new();
    registry.register(
        FactoryRegistration {
            factory: "service".into(),
            revision: "1".into(),
        },
        Arc::new(Factory),
    )?;
    let result = ProcessHost::run(bootstrap, registry, ProcessLimits::default())?;
    // Exit the actual executable, including when native component work never
    // joined. Returning an Unresponsive outcome is not proof of cleanup.
    match result {
        HostOutcome::Stopped {} => Ok(()),
        HostOutcome::Failed { .. } | HostOutcome::Unresponsive { .. } => std::process::exit(1),
    }
}
