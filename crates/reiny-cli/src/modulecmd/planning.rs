use super::{
    BTreeMap, DeploymentClient, DeploymentPlan, IsTerminal, Path, Result, Serialize, print_json,
};

pub(crate) fn plan(path: &Path, update: bool, json: bool) -> Result<()> {
    let plan = DeploymentPlan::load(path, update)?;
    let current = DeploymentClient::find(&plan.root)?
        .map(|client| client.status())
        .transpose()?;
    if let Some(current) = &current {
        anyhow::ensure!(
            current.deployment == plan.deployment && current.domain == plan.domain,
            "stop the existing owner before changing deployment or domain"
        );
    }
    let changes = planned_changes(&plan, current.as_ref());
    if json {
        print_json(&PlanView {
            declaration: &plan,
            changes: &changes,
        })?;
    } else {
        println!("deployment {} (domain {})", plan.deployment, plan.domain);
        for node in &plan.nodes {
            let preparation = if node.build.is_some() {
                "check declared build"
            } else {
                "verify installed binary"
            };
            println!("  {}: {} — {preparation}", node.namespace, node.run.bin);
            for (port, input) in &node.bindings.inputs {
                let routes = plan
                    .connections
                    .iter()
                    .filter(|route| {
                        route.destination.namespace == node.namespace
                            && route.destination.name == *port
                    })
                    .map(|route| {
                        let via = route
                            .via
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>();
                        if via.is_empty() {
                            route.source.to_string()
                        } else {
                            format!("{} via {}", route.source, via.join(" <- "))
                        }
                    })
                    .collect::<Vec<_>>();
                println!(
                    "    in.{port}: {} <- {}",
                    input.contract.type_name,
                    routes.join(", ")
                );
            }
            for (port, output) in &node.bindings.outputs {
                println!(
                    "    out.{port}: {} {:?}",
                    output.contract.type_name, output.contract.kind
                );
            }
        }
        for resource in &plan.resources {
            println!(
                "  artifact {} ← {}",
                resource.address,
                resource.source.display()
            );
        }
        for (name, host) in &plan.component_hosts {
            println!("  host {name}: {}", host.spec.bin);
        }
        for component in &plan.components {
            println!(
                "  {}: {}@{} on {}",
                component.namespace,
                component.component.factory,
                component.component.factory_revision,
                component.host
            );
        }
        for change in &changes {
            println!("  {}: {}", change.action, change.namespace);
        }
        print_flow(&plan);
    }
    Ok(())
}

fn planned_changes(
    plan: &DeploymentPlan,
    current: Option<&reiny_launch::DeploymentStatus>,
) -> Vec<PlannedChange> {
    let mut changes = Vec::new();
    for node in &plan.nodes {
        let active = current
            .and_then(|state| state.modules.get(&node.namespace))
            .is_some_and(|module| module.pid.is_some());
        changes.push(PlannedChange {
            namespace: node.namespace.clone(),
            action: if !active {
                "create"
            } else if node.build.is_some() {
                "prepare_and_compare"
            } else {
                "verify_and_compare"
            },
        });
    }
    for component in &plan.components {
        let active = current
            .and_then(|state| state.components.get(&component.namespace))
            .and_then(|instance| current?.component_hosts.get(&instance.host))
            .is_some_and(|host| host.pid.is_some());
        changes.push(PlannedChange {
            namespace: component.namespace.clone(),
            action: if active {
                "prepare_and_compare"
            } else {
                "create"
            },
        });
    }
    if let Some(current) = current {
        for (namespace, module) in &current.modules {
            if module.pid.is_some() && !plan.nodes.iter().any(|node| &node.namespace == namespace) {
                changes.push(PlannedChange {
                    namespace: namespace.clone(),
                    action: "remove",
                });
            }
        }
        for namespace in current.components.keys() {
            if !plan
                .components
                .iter()
                .any(|component| &component.namespace == namespace)
            {
                changes.push(PlannedChange {
                    namespace: namespace.clone(),
                    action: "remove",
                });
            }
        }
    }
    changes
}

fn print_flow(plan: &DeploymentPlan) {
    let namespaces: Vec<_> = plan
        .nodes
        .iter()
        .map(|node| node.namespace.as_str())
        .chain(
            plan.components
                .iter()
                .map(|component| component.namespace.as_str()),
        )
        .collect();
    let indices: BTreeMap<_, _> = namespaces
        .iter()
        .enumerate()
        .map(|(index, namespace)| (*namespace, index))
        .collect();
    let mut connections: BTreeMap<(String, String, String, Option<String>), Vec<usize>> =
        BTreeMap::new();
    for route in &plan.connections {
        let index = indices[route.destination.namespace.as_str()];
        let subscribers = connections
            .entry((
                route.source.namespace.clone(),
                route.source.name.clone(),
                route.contract.type_name.clone(),
                route.contract.response.clone(),
            ))
            .or_default();
        if !subscribers.contains(&index) {
            subscribers.push(index);
        }
    }
    let edges: Vec<_> = connections
        .into_iter()
        .map(|((source, port, ty, reply), clients)| {
            let server = vec![indices[source.as_str()]];
            let (pubs, subs) = if reply.is_some() {
                (clients, server)
            } else {
                (server, clients)
            };
            crate::flowart::Edge {
                ty: format!("{port}: {ty}"),
                pubs,
                subs,
                reply,
            }
        })
        .collect();
    let prefix = format!("{}/", plan.deployment);
    let names: Vec<_> = namespaces
        .iter()
        .map(|namespace| {
            namespace
                .strip_prefix(&prefix)
                .unwrap_or(namespace)
                .to_string()
        })
        .collect();
    let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    for line in crate::flowart::render(&names, &edges, color) {
        println!("{line}");
    }
}

#[derive(Serialize)]
struct PlannedChange {
    namespace: String,
    action: &'static str,
}

#[derive(Serialize)]
struct PlanView<'a> {
    #[serde(flatten)]
    declaration: &'a DeploymentPlan,
    changes: &'a [PlannedChange],
}
