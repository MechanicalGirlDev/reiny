//! Bounded, correlated control-channel frames.

use super::super::{
    ProcessError, codec,
    protocol::{
        Envelope, FailureCode, HostMessage, OwnerMessage, ProtocolVersion, Sequence,
        SessionIdentity, frame_header, payload_length,
    },
};
use super::invalid;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout_at},
};

pub(super) struct Channel {
    pub(super) stream: TcpStream,
    pub(super) identity: SessionIdentity,
    pub(super) sent: u64,
    pub(super) received: Option<Sequence>,
    pub(super) healthy: bool,
}

impl Channel {
    pub(super) async fn send(
        &mut self,
        body: OwnerMessage,
        deadline: Instant,
    ) -> Result<(), ProcessError> {
        let next = self.sent.checked_add(1).ok_or_else(invalid)?;
        let payload = codec::encode(&Envelope {
            version: ProtocolVersion,
            session: self.identity.clone(),
            sequence: Sequence(self.sent),
            body,
        })?;
        let header = frame_header(&payload).map_err(ProcessError::frame)?;
        self.healthy = false;
        timeout_at(deadline, async {
            self.stream.write_all(&header).await?;
            self.stream.write_all(&payload).await
        })
        .await
        .map_err(|_| ProcessError::new(FailureCode::Timeout))?
        .map_err(ProcessError::io)?;
        self.sent = next;
        self.healthy = true;
        Ok(())
    }

    pub(super) async fn receive(&mut self, deadline: Instant) -> Result<HostMessage, ProcessError> {
        self.healthy = false;
        let message = timeout_at(deadline, async {
            let mut header = [0; 4];
            self.stream
                .read_exact(&mut header)
                .await
                .map_err(ProcessError::io)?;
            let mut payload = vec![0; payload_length(header).map_err(ProcessError::frame)?];
            self.stream
                .read_exact(&mut payload)
                .await
                .map_err(ProcessError::io)?;
            let envelope: Envelope<HostMessage> = codec::decode(&payload)?;
            envelope
                .check(&self.identity, self.received)
                .map_err(|_| ProcessError::new(FailureCode::Identity))?;
            self.received = Some(envelope.sequence);
            Ok(envelope.body)
        })
        .await
        .map_err(|_| ProcessError::new(FailureCode::Timeout))??;
        self.healthy = true;
        Ok(message)
    }
}
