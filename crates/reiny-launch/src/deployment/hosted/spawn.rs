use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::Arc;

use serde::Serialize;

use super::super::{OwnedProcess, ProcessObservation};
use super::{
    BTreeSet, Context, Hosted, Owner, Phase, Result, Session, SessionIdentity, TcpListener,
    assignment, codec, ensure, protocol,
};

// This engine-neutral shape is the SDK ProcessHost's inherited Bootstrap.
#[derive(Serialize)]
struct Bootstrap<'a> {
    address: SocketAddr,
    identity: &'a SessionIdentity,
    nonce: &'a protocol::Nonce,
}

impl Hosted {
    pub(super) fn spawn(&mut self, owner: &mut Owner<'_>) -> Result<()> {
        let names: Vec<_> = owner.desired.component_hosts.keys().cloned().collect();
        for name in names {
            if let Err(error) = self.spawn_one(owner, &name) {
                self.fail(owner, &name, &format!("{error:#}"))?;
                break;
            }
        }
        Ok(())
    }

    fn spawn_one(&mut self, owner: &mut Owner<'_>, name: &str) -> Result<()> {
        let host = owner
            .desired
            .component_hosts
            .get(name)
            .context("missing prepared host")?;
        let mut assignments = owner
            .desired
            .components
            .iter()
            .filter(|component| component.host == name)
            .map(assignment::instance)
            .collect::<Result<Vec<_>>>()?;
        assignments.sort_by(|left, right| left.bindings.namespace.cmp(&right.bindings.namespace));
        let transport = assignment::transport(host, &owner.desired)?;
        owner.next_generation = owner
            .next_generation
            .checked_add(1)
            .context("host generation exhausted")?;
        let generation = owner.next_generation;
        let identity = self.identity(owner, name, generation)?;
        let mut secret = [0; 32];
        getrandom::fill(&mut secret)
            .map_err(|_| anyhow::anyhow!("host nonce entropy unavailable"))?;
        let nonce = protocol::Nonce::new(secret);
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let bootstrap = bootstrap(
            listener.local_addr()?,
            owner.remote.get(name).map(|target| target.control_port),
            &identity,
            &nonce,
        )?;
        let (reader, writer) = std::io::pipe()?;
        let directory = owner
            .cache
            .join("generations")
            .join(&owner.status.owner_generation)
            .join(generation.to_string());
        std::fs::create_dir_all(&directory)?;
        let Launch {
            mut command,
            remote,
            stdout_reader,
        } = launch(
            owner,
            name,
            &mut assignments,
            &identity,
            &directory,
            listener.local_addr()?.port(),
        )?;
        command
            .stdin(Stdio::from(reader))
            .stderr(File::create(directory.join("stderr.log"))?);
        let process = OwnedProcess::spawn(
            command,
            ProcessObservation::Hosted,
            Arc::clone(&owner.runtime),
        )
        .with_context(|| format!("spawning component host {name}"))?;
        let pid = process.child.id().context("spawned host has no PID")?;
        owner.children.insert(name.to_owned(), process);
        let mut local_bootstrap = None;
        let gate = if let Some(stdout) = stdout_reader {
            Some(super::super::remote::Gate::new(stdout, writer, bootstrap))
        } else {
            local_bootstrap = Some((writer, bootstrap));
            None
        };
        self.sessions.insert(
            name.to_owned(),
            Session {
                listener: Some(listener),
                channel: None,
                identity,
                nonce: Some(nonce),
                assignments,
                transport,
                phase: Phase::Authenticating,
                reports: BTreeSet::new(),
                startup_deadline: self.deadline,
                stop_deadline: None,
                stop_reason: None,
                failure: None,
                remote,
                gate,
            },
        );
        let status = owner
            .status
            .component_hosts
            .get_mut(name)
            .context("missing host status")?;
        status.pid = Some(pid);
        status.generation = generation;
        status.graph_generation = owner.graph_generation;
        if let Some((mut writer, bootstrap)) = local_bootstrap {
            writer.write_all(&bootstrap)?;
        }
        Ok(())
    }

    fn identity(&self, owner: &Owner<'_>, name: &str, generation: u64) -> Result<SessionIdentity> {
        Ok(SessionIdentity {
            owner_generation: protocol::OwnerGeneration(u64::from_str_radix(
                owner
                    .status
                    .owner_generation
                    .get(..16)
                    .context("invalid owner generation")?,
                16,
            )?),
            graph_generation: protocol::GraphGeneration(owner.graph_generation),
            host_id: protocol::HostId(name.to_owned()),
            host_generation: protocol::HostGeneration(generation),
            graph_digest: protocol::PreparedDigest(self.digest.clone()),
            bundle_digest: protocol::PreparedDigest(
                owner.desired.component_hosts[name].fingerprint.clone(),
            ),
        })
    }
}

struct Launch {
    command: Command,
    remote: Option<super::super::remote::RemoteOwnership>,
    stdout_reader: Option<std::io::PipeReader>,
}

fn bootstrap(
    mut address: SocketAddr,
    remote_port: Option<u16>,
    identity: &SessionIdentity,
    nonce: &protocol::Nonce,
) -> Result<Vec<u8>> {
    if let Some(port) = remote_port {
        address.set_port(port);
    }
    let bytes = codec::frame(&Bootstrap {
        address,
        identity,
        nonce,
    })?;
    // One small inherited pipe write cannot depend on a peer making progress.
    ensure!(bytes.len() <= 4096, "host bootstrap exceeds pipe bound");
    Ok(bytes)
}

fn launch(
    owner: &Owner<'_>,
    name: &str,
    assignments: &mut [protocol::ComponentAssignment],
    identity: &SessionIdentity,
    directory: &std::path::Path,
    local_port: u16,
) -> Result<Launch> {
    let host = &owner.desired.component_hosts[name];
    let target = owner.remote.get(name);
    let mut remote = None;
    let mut stdout_reader = None;
    let command = if let Some(target) = target {
        let (mut command, target_digest) =
            target.command(host, &owner.desired, assignments, directory, local_port)?;
        let record = super::super::remote::RemoteOwnership {
            identity: identity.clone(),
            target_digest,
            local_client_reaped: false,
            cooperative_outcome: None,
            remote_command_exit: None,
            tree_cleanup: super::super::remote::RemoteTreeCleanup::Unknown,
            reconciliation: None,
        };
        // A crash after this reservation can only leave Unknown, never erase
        // the remote execution obligation. No SSH effects precede this fsync.
        super::super::remote::persist(&owner.desired.root, &record, true)?;
        remote = Some(record);
        let (read, write) = std::io::pipe()?;
        stdout_reader = Some(read);
        command.stdout(write);
        command
    } else {
        let mut command = Command::new(&host.executable);
        command
            .current_dir(&host.module_dir)
            .env("REINY_BUNDLE_DIR", &host.bundle_dir)
            .env_remove("REINY_CONFIG_DIR")
            .env_remove("REINY_MODULE_REPORT")
            .stdout(File::create(directory.join("stdout.log"))?);
        command
    };
    Ok(Launch {
        command,
        remote,
        stdout_reader,
    })
}
