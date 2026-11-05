//! Nonblocking session I/O; bounded work keeps owner control responsive.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};
use reiny_core::host_protocol::{
    Envelope, HostMessage, OwnerMessage, ProtocolVersion, Sequence, SessionIdentity, payload_length,
};

use super::super::IO_TIMEOUT;
use super::codec;

struct Pending {
    bytes: Vec<u8>,
    written: usize,
    deadline: Instant,
}

pub(super) struct Channel {
    socket: TcpStream,
    identity: SessionIdentity,
    previous: Option<Sequence>,
    next: Sequence,
    header: [u8; 4],
    header_used: usize,
    payload: Vec<u8>,
    payload_used: usize,
    read_deadline: Option<Instant>,
    outgoing: VecDeque<Pending>,
}

impl Channel {
    pub(super) fn new(socket: TcpStream, identity: SessionIdentity) -> Result<Self> {
        socket.set_nonblocking(true)?;
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            identity,
            previous: None,
            next: Sequence(0),
            header: [0; 4],
            header_used: 0,
            payload: Vec::new(),
            payload_used: 0,
            read_deadline: None,
            outgoing: VecDeque::new(),
        })
    }

    pub(super) fn queue(&mut self, body: OwnerMessage) -> Result<()> {
        ensure!(self.outgoing.len() < 2, "host outbound queue exhausted");
        let bytes = codec::frame(&Envelope {
            version: ProtocolVersion,
            session: self.identity.clone(),
            sequence: self.next,
            body,
        })?;
        self.next = Sequence(
            self.next
                .0
                .checked_add(1)
                .context("host sequence exhausted")?,
        );
        self.outgoing.push_back(Pending {
            bytes,
            written: 0,
            deadline: Instant::now() + IO_TIMEOUT,
        });
        self.flush()
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        let mut budget = 64 * 1024;
        while budget > 0 {
            let Some(pending) = self.outgoing.front_mut() else {
                break;
            };
            ensure!(Instant::now() < pending.deadline, "host write timed out");
            let end = pending.bytes.len().min(pending.written + budget);
            match self.socket.write(&pending.bytes[pending.written..end]) {
                Ok(0) => bail!("host control disconnected"),
                Ok(count) => {
                    pending.written += count;
                    budget -= count;
                    if pending.written == pending.bytes.len() {
                        self.outgoing.pop_front();
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Result<Option<HostMessage>> {
        ensure!(
            self.read_deadline
                .is_none_or(|deadline| Instant::now() < deadline),
            "host partial frame timed out"
        );
        let mut budget = 64 * 1024;
        while budget > 0 {
            let reading_header = self.header_used < self.header.len();
            let buffer = if reading_header {
                &mut self.header[self.header_used..]
            } else {
                let end = self.payload.len().min(self.payload_used + budget);
                &mut self.payload[self.payload_used..end]
            };
            let count = match self.socket.read(buffer) {
                Ok(0) => bail!("host control disconnected"),
                Ok(count) => count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            budget -= count;
            self.read_deadline
                .get_or_insert_with(|| Instant::now() + IO_TIMEOUT);
            if reading_header {
                self.header_used += count;
                if self.header_used == self.header.len() {
                    self.payload.resize(payload_length(self.header)?, 0);
                }
            } else {
                self.payload_used += count;
                if self.payload_used == self.payload.len() {
                    let envelope: Envelope<HostMessage> = codec::decode(&self.payload)?;
                    envelope.check(&self.identity, self.previous)?;
                    self.previous = Some(envelope.sequence);
                    self.header_used = 0;
                    self.payload.clear();
                    self.payload_used = 0;
                    self.read_deadline = None;
                    return Ok(Some(envelope.body));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;
    use reiny_core::host_protocol::{
        GraphGeneration, HostGeneration, HostId, OwnerGeneration, PreparedDigest,
    };

    #[test]
    fn queued_control_is_sent_before_unrelated_owner_work() -> Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let sender = TcpStream::connect(listener.local_addr()?)?;
        let (mut receiver, _) = listener.accept()?;
        receiver.set_read_timeout(Some(IO_TIMEOUT))?;
        let identity = SessionIdentity {
            owner_generation: OwnerGeneration(1),
            graph_generation: GraphGeneration(1),
            host_id: HostId("channel-test".into()),
            host_generation: HostGeneration(1),
            graph_digest: PreparedDigest("a".repeat(64)),
            bundle_digest: PreparedDigest("b".repeat(64)),
        };
        let mut channel = Channel::new(sender, identity.clone())?;
        channel.queue(OwnerMessage::Commit {})?;
        // Model unrelated owner work crossing the queued frame's deadline.
        // A writable small control frame should already have left the queue.
        for pending in &mut channel.outgoing {
            pending.deadline = Instant::now();
        }
        channel.flush()?;
        let mut header = [0; 4];
        receiver.read_exact(&mut header)?;
        let mut bytes = vec![0; payload_length(header)?];
        receiver.read_exact(&mut bytes)?;
        let envelope: Envelope<OwnerMessage> = codec::decode(&bytes)?;
        envelope.check(&identity, None)?;
        assert_eq!(envelope.body, OwnerMessage::Commit {});
        Ok(())
    }
}
