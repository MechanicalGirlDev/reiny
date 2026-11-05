use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;

use anyhow::{Context, Result, bail};
use reiny_core::bindings::{
    EndpointAddress, MODULE_REPORT_VERSION, ModuleReport, PortEndpoint, PortReport,
};
use reiny_core::host_protocol::{
    Authenticate, Compatibility, Envelope, FactoryRegistration, FailureCode, HostFailure,
    HostMessage, HostOutcome, HostPhase, Nonce, OwnerMessage, ProtocolVersion, Sequence,
    SessionIdentity, StopReason,
};
use serde::{Deserialize, Serialize};

use super::{Event, EventKind, Fault, Fixture, support::DEADLINE};

#[derive(Deserialize)]
struct Bootstrap {
    address: std::net::SocketAddr,
    identity: SessionIdentity,
    nonce: Nonce,
}

pub(super) fn read<T: serde::de::DeserializeOwned>(input: &mut impl Read) -> Result<T> {
    let mut header = [0; 4];
    input.read_exact(&mut header)?;
    let mut bytes = vec![0; reiny_core::host_protocol::payload_length(header)?];
    input.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn write(output: &mut impl Write, message: &impl Serialize) -> Result<()> {
    let payload = serde_json::to_vec(message)?;
    output.write_all(&reiny_core::host_protocol::frame_header(&payload)?)?;
    output.write_all(&payload)?;
    Ok(())
}

struct Peer {
    socket: TcpStream,
    events: TcpStream,
    identity: SessionIdentity,
    next: u64,
}

impl Peer {
    fn send(&mut self, body: HostMessage) -> Result<()> {
        write(
            &mut self.socket,
            &Envelope {
                version: ProtocolVersion,
                session: self.identity.clone(),
                sequence: Sequence(self.next),
                body,
            },
        )?;
        self.next = self.next.checked_add(1).context("peer sequence overflow")?;
        Ok(())
    }

    fn event(&mut self, kind: EventKind) -> Result<()> {
        writeln!(
            &mut self.events,
            "{}",
            serde_json::to_string(&Event {
                host: self.identity.host_id.0.clone(),
                kind,
            })?
        )?;
        Ok(())
    }

    fn command(&mut self) -> Result<OwnerMessage> {
        let message = read::<Envelope<OwnerMessage>>(&mut self.socket)?.body;
        match &message {
            OwnerMessage::Steps(_) => bail!("step extension not negotiated by service fixture"),
            OwnerMessage::Run {} => self.event(EventKind::Run)?,
            OwnerMessage::Authenticated { .. }
            | OwnerMessage::Prepare(_)
            | OwnerMessage::Commit {}
            | OwnerMessage::Stop { .. } => {}
        }
        Ok(message)
    }

    fn finish(&mut self, message: &OwnerMessage) -> Result<()> {
        match message {
            OwnerMessage::Stop {
                reason: StopReason::AdmissionAborted,
                ..
            } => {
                let failure = HostFailure {
                    code: FailureCode::Cancelled,
                    phase: HostPhase::Prepared,
                    instance: None,
                    message: "owner aborted admission".to_owned(),
                };
                self.send(HostMessage::Failure(failure.clone()))?;
                self.send(HostMessage::Outcome(HostOutcome::Failed {
                    primary: failure,
                    cleanup: Vec::new(),
                }))
            }
            OwnerMessage::Stop { .. } => self.send(HostMessage::Outcome(HostOutcome::Stopped {})),
            OwnerMessage::Steps(_)
            | OwnerMessage::Authenticated { .. }
            | OwnerMessage::Prepare(_)
            | OwnerMessage::Commit {}
            | OwnerMessage::Run {} => bail!("unexpected owner transition"),
        }
    }

    fn watch_commands(&self) -> Result<std::sync::mpsc::Receiver<()>> {
        let commands = BufReader::new(self.events.try_clone()?);
        let (release_report, report_gate) = std::sync::mpsc::sync_channel(1);
        let control = self.socket.try_clone()?;
        std::thread::spawn(move || {
            for line in commands.lines() {
                match line {
                    Ok(line) if line == "exit" => std::process::exit(23),
                    Ok(line) if line == "report" => {
                        if let Err(error) = release_report.send(()) {
                            eprintln!("fixture report gate: {error}");
                            break;
                        }
                    }
                    Ok(_) => break,
                    Err(error) => {
                        eprintln!("fixture command: {error}");
                        break;
                    }
                }
            }
            if let Err(error) = control.shutdown(std::net::Shutdown::Both) {
                eprintln!("fixture control shutdown: {error}");
            }
        });
        Ok(report_gate)
    }
}

pub(super) fn run() -> Result<()> {
    let config: Fixture = serde_json::from_slice(&std::fs::read("peer.json")?)?;
    let bootstrap: Bootstrap = read(&mut std::io::stdin().lock())?;
    let fault = if bootstrap.identity.host_id.0 == "failing" {
        config.fault
    } else {
        Fault::Healthy
    };
    let mut peer = Peer {
        socket: TcpStream::connect(bootstrap.address)?,
        events: TcpStream::connect(config.address)?,
        identity: bootstrap.identity,
        next: 0,
    };
    peer.socket.set_read_timeout(Some(DEADLINE))?;
    peer.socket.set_write_timeout(Some(DEADLINE))?;
    peer.event(EventKind::Connected)?;
    let report_gate = peer.watch_commands()?;
    authentication::authenticate(&mut peer, bootstrap.nonce, fault)?;
    let prepare = match peer.command()? {
        OwnerMessage::Authenticated { .. } => match peer.command()? {
            OwnerMessage::Prepare(prepare) => prepare,
            message => return peer.finish(&message),
        },
        message => return peer.finish(&message),
    };
    peer.event(EventKind::Prepared)?;
    if matches!(fault, Fault::GraphSchema) {
        report_gate.recv_timeout(DEADLINE)?;
    }
    match fault {
        Fault::PrepareEof => return Ok(()),
        Fault::BlockPrepare => {
            let command = peer.command()?;
            return peer.finish(&command);
        }
        Fault::Nonce
        | Fault::Digest
        | Fault::Generation
        | Fault::Sequence
        | Fault::Compatibility
        | Fault::DuplicateJson
        | Fault::NestedDuplicateJson
        | Fault::Oversize
        | Fault::UnknownField
        | Fault::FirstStatus
        | Fault::ReportIdentity
        | Fault::GraphSchema
        | Fault::DuplicateReport
        | Fault::Commit
        | Fault::UnrequestedCancel
        | Fault::Healthy => {}
    }
    reports::send(&mut peer, prepare.assignments, fault)?;
    peer.send(HostMessage::PreparedComplete {})?;
    match peer.command()? {
        OwnerMessage::Commit {} => {}
        message => return peer.finish(&message),
    }
    if matches!(fault, Fault::Commit | Fault::UnrequestedCancel) {
        let failure = HostFailure {
            code: if matches!(fault, Fault::UnrequestedCancel) {
                FailureCode::Cancelled
            } else {
                FailureCode::Commit
            },
            phase: HostPhase::Prepared,
            instance: None,
            message: "fixture commit failed".to_owned(),
        };
        peer.send(HostMessage::Failure(failure.clone()))?;
        return peer.send(HostMessage::Outcome(HostOutcome::Failed {
            primary: failure,
            cleanup: Vec::new(),
        }));
    }
    peer.send(HostMessage::Committed {})?;
    match peer.command()? {
        OwnerMessage::Run {} => {}
        message => return peer.finish(&message),
    }
    peer.send(HostMessage::Running {})?;
    let message = peer.command()?;
    peer.finish(&message)
}

#[path = "peer/authentication.rs"]
mod authentication;

#[path = "peer/reports.rs"]
mod reports;
