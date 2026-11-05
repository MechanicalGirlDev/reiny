//! Compare complete process-step results with the Local scheduler.

use super::{
    Arc, BUDGET, ComponentHost, ComponentInstanceSpec, ComponentRegistry, Duration, Factory,
    HostOptions, Number, ProcessStepOwner, Result, bindings, ensure, graph, request, schema,
    timeout, value,
};

pub(super) async fn compare_local(
    owner: ProcessStepOwner,
    steps: reiny::component::process::ProcessStepClient,
    children: Vec<tokio::process::Child>,
) -> Result<()> {
    let mut registry = ComponentRegistry::new();
    registry.register_step("counter".into(), schema(), Arc::new(Factory))?;
    let instances = ["a", "b", "c"]
        .into_iter()
        .map(|id| ComponentInstanceSpec {
            factory_id: "counter".into(),
            bindings: bindings(id),
            config: None,
            config_origin: None,
        })
        .collect();
    let (local, local_steps) = ComponentHost::launch_with_steps(
        Arc::new(registry),
        HostOptions {
            domain: "local-reference".into(),
            engine: Arc::new(reiny::engine::Local::new()),
            startup_timeout: BUDGET,
            stop_timeout: BUDGET,
        },
        instances,
        graph(value(17)),
        Duration::ZERO,
    )?;
    local.wait_running().await?;
    for index in 0..3 {
        let remote = steps.try_step(request(), BUDGET)?.wait().await?;
        let reference = local_steps.try_step(request(), BUDGET)?.wait().await?;
        ensure!(
            remote.outputs == reference.outputs
                && remote.tick.start == reference.tick.start
                && remote.tick.end == reference.tick.end
                && remote.tick.id.index == reference.tick.id.index,
            "local/process equivalence"
        );
        println!(
            "tick={index} time={:?} outputs={:?}",
            remote.tick.end,
            remote
                .outputs
                .iter()
                .map(|(p, v)| Ok((p.instance.clone(), v.decode::<Number>()?.value)))
                .collect::<Result<Vec<_>>>()?
        );
    }
    owner.stop().await?;
    ensure!(
        matches!(local.stop().await, reiny::component::HostOutcome::Stopped),
        "local joined"
    );
    for mut child in children {
        ensure!(
            timeout(BUDGET, child.wait()).await??.success(),
            "child failed"
        );
    }
    println!("all owned direct children reaped; no remote-tree containment claimed");
    Ok(())
}
