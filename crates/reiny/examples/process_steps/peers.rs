//! Authenticated bootstrap for two owned local OS processes.

use super::{
    AsyncWriteExt, BTreeMap, BTreeSet, BUDGET, Bootstrap, Command, ComponentAssignment,
    HostGeneration, HostId, Milliseconds, Nonce, Prepare, PreparedDigest, ProcessStepPeer, Result,
    SessionIdentity, Stdio, StepAssignment, StepSessionKey, TcpListener, TransportSettings,
    bindings, encode_payload, frame_header, registration, timeout, wire_schema,
};

pub(super) async fn spawn_hosts(
    key: &StepSessionKey,
    fault_mode: Option<&str>,
    entropy: &mut std::fs::File,
) -> Result<(Vec<ProcessStepPeer>, Vec<tokio::process::Child>)> {
    let mut random = [0; 32];
    let mut peers = Vec::new();
    let mut children = Vec::new();
    let placement = BTreeMap::from([
        ("a".into(), "host1".into()),
        ("b".into(), "host2".into()),
        ("c".into(), "host1".into()),
    ]);
    for (host, ids) in [("host1", vec!["a", "c"]), ("host2", vec!["b"])] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        std::io::Read::read_exact(entropy, &mut random)?;
        let nonce = Nonce::new(random);
        let identity = SessionIdentity {
            owner_generation: key.owner_generation,
            graph_generation: key.graph_generation,
            host_id: HostId(host.into()),
            host_generation: HostGeneration(1),
            graph_digest: key.graph_digest.clone(),
            bundle_digest: PreparedDigest("same-example-binary".into()),
        };
        let bootstrap = Bootstrap {
            address: listener.local_addr()?,
            identity: identity.clone(),
            nonce: nonce.clone(),
        };
        let mut command = Command::new(std::env::current_exe()?);
        command
            .arg("--host")
            .stdin(Stdio::piped())
            .kill_on_drop(true);
        if fault_mode.is_some() && host == "host2" {
            command.arg("--block").stdout(Stdio::piped());
        }
        let mut child = command.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| anyhow::anyhow!("missing child pid"))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing bootstrap pipe"))?;
        let bytes = encode_payload(&bootstrap)?;
        stdin.write_all(&frame_header(&bytes)?).await?;
        stdin.write_all(&bytes).await?;
        drop(stdin);
        let (stream, _) = timeout(BUDGET, listener.accept()).await??;
        println!("{host} pid={pid} instances={ids:?}");
        let assignments = ids
            .iter()
            .map(|id| ComponentAssignment {
                implementation: registration(),
                bindings: bindings(id),
                config_origin: None,
            })
            .collect();
        let steps = ids
            .iter()
            .map(|id| {
                (
                    (*id).into(),
                    StepAssignment {
                        schema: wire_schema(),
                        contract: b"counter-v1:seed=17".to_vec(),
                        seed_outputs: if *id == "c" {
                            BTreeSet::from(["out".into()])
                        } else {
                            BTreeSet::new()
                        },
                    },
                )
            })
            .collect();
        peers.push(ProcessStepPeer {
            stream,
            identity,
            nonce,
            prepare: Prepare {
                assignments,
                transport: TransportSettings::Local {
                    domain: "process-steps".into(),
                    placement: placement.clone(),
                },
                remaining_budget: Milliseconds(10000),
            },
            steps,
        });
        children.push(child);
    }
    Ok((peers, children))
}
