use std::future::Future;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::{Instant, timeout};

use super::protocol::{
    Envelope, EnvelopeError, FailureCode, HostMessage, OwnerMessage, Sequence, SessionIdentity,
};
use super::{ProcessError, codec, protocol};

pub(super) struct Reader {
    socket: OwnedReadHalf,
    identity: SessionIdentity,
    previous: Option<Sequence>,
    header: [u8; 4],
    header_used: usize,
    payload: Vec<u8>,
    payload_used: usize,
    frame_deadline: Option<Instant>,
    frame_budget: Duration,
}

impl Reader {
    pub(super) fn new(
        socket: OwnedReadHalf,
        identity: SessionIdentity,
        frame_budget: Duration,
    ) -> Self {
        Self {
            socket,
            identity,
            previous: None,
            header: [0; 4],
            header_used: 0,
            payload: Vec::new(),
            payload_used: 0,
            frame_deadline: None,
            frame_budget,
        }
    }

    // All partial progress and its deadline survive cancellation by a stage.
    pub(super) async fn next(&mut self) -> Result<OwnerMessage, ProcessError> {
        loop {
            let read = async {
                if self.header_used < self.header.len() {
                    self.socket.read(&mut self.header[self.header_used..]).await
                } else {
                    self.socket
                        .read(&mut self.payload[self.payload_used..])
                        .await
                }
            };
            let count = tokio::select! {
                biased;
                () = expires(self.frame_deadline) => return Err(ProcessError::new(FailureCode::Timeout)),
                result = read => result.map_err(ProcessError::io)?,
            };
            if count == 0 {
                return Err(ProcessError::new(FailureCode::Disconnected));
            }
            self.frame_deadline
                .get_or_insert_with(|| Instant::now() + self.frame_budget);
            if self.header_used < self.header.len() {
                self.header_used += count;
                if self.header_used == self.header.len() {
                    let length =
                        protocol::payload_length(self.header).map_err(ProcessError::frame)?;
                    self.payload.resize(length, 0);
                }
            } else {
                self.payload_used += count;
                if self.payload_used == self.payload.len() {
                    let envelope: Envelope<OwnerMessage> = codec::decode(&self.payload)?;
                    envelope
                        .check(&self.identity, self.previous)
                        .map_err(|error| {
                            ProcessError::new(match error {
                                EnvelopeError::Identity => FailureCode::Identity,
                                EnvelopeError::Sequence => FailureCode::Sequence,
                                EnvelopeError::Version(_) => FailureCode::Compatibility,
                            })
                        })?;
                    self.previous = Some(envelope.sequence);
                    self.header_used = 0;
                    self.payload.clear();
                    self.payload_used = 0;
                    self.frame_deadline = None;
                    return Ok(envelope.body);
                }
            }
        }
    }
}

pub(super) async fn expires(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

pub(super) struct Writer {
    pub socket: OwnedWriteHalf,
    pub identity: SessionIdentity,
    pub sequence: Sequence,
    pub budget: Duration,
    pub writable: bool,
}

impl Writer {
    pub(super) async fn send(&mut self, body: HostMessage) -> Result<(), ProcessError> {
        if !self.writable {
            return Err(ProcessError::new(FailureCode::Disconnected));
        }
        let envelope = Envelope {
            version: protocol::ProtocolVersion,
            session: self.identity.clone(),
            sequence: self.sequence,
            body,
        };
        let payload = codec::encode(&envelope)?;
        let header = protocol::frame_header(&payload).map_err(ProcessError::frame)?;
        self.writable = false;
        timeout(self.budget, async {
            self.socket.write_all(&header).await?;
            self.socket.write_all(&payload).await
        })
        .await
        .map_err(|_| ProcessError::new(FailureCode::Timeout))?
        .map_err(ProcessError::io)?;
        self.sequence = Sequence(
            self.sequence
                .0
                .checked_add(1)
                .ok_or_else(|| ProcessError::new(FailureCode::Sequence))?,
        );
        self.writable = true;
        Ok(())
    }
}

pub(super) struct Guard {
    pub reader: Reader,
    pub deadline: Option<Instant>,
    pub stop: Option<protocol::StopReason>,
    pub cleanup_budget: Duration,
}

pub(super) struct SessionIo {
    pub guard: Guard,
    pub writer: Writer,
    pub phase: protocol::HostPhase,
}

impl Guard {
    pub(super) async fn next(&mut self) -> Result<OwnerMessage, ProcessError> {
        let message = tokio::select! {
            biased;
            () = expires(self.deadline) => return Err(ProcessError::new(FailureCode::Timeout)),
            message = self.reader.next() => message?,
        };
        match message {
            OwnerMessage::Stop {
                reason,
                remaining_budget,
            } => {
                self.stop = Some(reason);
                self.cleanup_budget = self
                    .cleanup_budget
                    .min(Duration::from_millis(remaining_budget.0));
                Err(ProcessError::new(FailureCode::Cancelled))
            }
            OwnerMessage::Steps(_)
            | OwnerMessage::Authenticated { .. }
            | OwnerMessage::Prepare(_)
            | OwnerMessage::Commit {}
            | OwnerMessage::Run {} => Ok(message),
        }
    }

    pub(super) async fn during<T>(
        &mut self,
        work: impl Future<Output = Result<T, ProcessError>>,
    ) -> Result<T, ProcessError> {
        tokio::select! {
            biased;
            command = self.next() => {
                command?;
                Err(ProcessError::new(FailureCode::Transition))
            }
            result = work => result,
        }
    }
}
