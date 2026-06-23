use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::CommError;

/// UDP receiver
pub struct UdpReceiver {
    socket: UdpSocket,
    buffer_size: usize,
}

impl UdpReceiver {
    pub async fn bind(addr: &str) -> Result<Self, CommError> {
        let socket = UdpSocket::bind(addr).await?;
        info!("UDP receiver bound to {}", addr);
        Ok(Self {
            socket,
            buffer_size: 65535,
        })
    }

    /// Start receive loop and send data to channel
    pub async fn run(self, tx: mpsc::Sender<(Vec<u8>, SocketAddr)>) -> Result<(), CommError> {
        let mut buf = vec![0u8; self.buffer_size];

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    debug!("Received {} bytes from {}", len, addr);
                    let data = buf[..len].to_vec();
                    if tx.send((data, addr)).await.is_err() {
                        warn!("Channel closed, stopping receiver");
                        break;
                    }
                }
                Err(e) => {
                    warn!("Receive error: {}", e);
                }
            }
        }

        Ok(())
    }
}
