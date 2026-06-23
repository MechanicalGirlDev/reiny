use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::{debug, info};

use crate::CommError;

/// UDP sender
pub struct UdpSender {
    socket: UdpSocket,
    remote_addr: SocketAddr,
}

impl UdpSender {
    pub async fn new(local_addr: &str, remote_addr: SocketAddr) -> Result<Self, CommError> {
        let socket = UdpSocket::bind(local_addr).await?;
        info!(
            "UDP sender bound to {}, target: {}",
            local_addr, remote_addr
        );
        Ok(Self {
            socket,
            remote_addr,
        })
    }

    pub async fn send(&self, data: &[u8]) -> Result<usize, CommError> {
        let len = self.socket.send_to(data, self.remote_addr).await?;
        debug!("Sent {} bytes to {}", len, self.remote_addr);
        Ok(len)
    }

    pub fn set_remote(&mut self, addr: SocketAddr) {
        self.remote_addr = addr;
    }
}
