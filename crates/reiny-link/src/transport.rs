//! The host side's "thing that carries bytes" — [`Transport`].
//!
//! This is where the I/O that [`Link`](crate::Link) knows nothing about gets attached. A stream
//! (serial / TCP / pty / `tokio::io::duplex`) only needs wrapping in [`Stream`]; datagrams use
//! [`Udp`]. A carrier of your own joins by implementing the two methods `send` and `recv`.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{ToSocketAddrs, UdpSocket};

/// Something that can carry bytes.
///
/// When `DATAGRAM` is true, `send` sends one frame as one datagram and `recv` returns one datagram.
/// When it is false, it is a stream that may be split anywhere.
pub trait Transport: Send {
    /// Whether the carrier preserves message boundaries (UDP).
    const DATAGRAM: bool;

    /// Send bytes. On a stream, write all of them.
    fn send(&mut self, bytes: &[u8]) -> impl Future<Output = io::Result<()>> + Send;

    /// Receive bytes. `Ok(0)` is EOF on a stream and an empty datagram on a datagram carrier.
    fn recv(&mut self, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send;
}

/// Anything `AsyncRead + AsyncWrite` is a stream carrier.
pub struct Stream<T>(pub T);

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for Stream<T> {
    const DATAGRAM: bool = false;

    async fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.write_all(bytes).await?;
        self.0.flush().await
    }

    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf).await
    }
}

/// Point-to-point over UDP.
///
/// Either pin the peer's address with [`Udp::with_peer`], or let it be remembered from the source of
/// the first datagram that arrives. Nothing starts if neither side knows the other, so pin at least
/// one of them (the typical case being that the MCU knows the host's address). Once pinned,
/// datagrams from anyone else are dropped.
pub struct Udp {
    socket: UdpSocket,
    peer: Option<SocketAddr>,
}

impl Udp {
    /// Bind to `local`.
    pub async fn bind(local: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            socket: UdpSocket::bind(local).await?,
            peer: None,
        })
    }

    /// Pin the peer.
    #[must_use]
    pub fn with_peer(mut self, peer: SocketAddr) -> Self {
        self.peer = Some(peer);
        self
    }

    /// The peer currently being sent to.
    #[must_use]
    pub fn peer(&self) -> Option<SocketAddr> {
        self.peer
    }

    /// The bound address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl Transport for Udp {
    const DATAGRAM: bool = true;

    async fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self.peer {
            Some(peer) => self.socket.send_to(bytes, peer).await.map(|_| ()),
            None => Ok(()), // drop until the peer is known (the Hello is re-sent periodically)
        }
    }

    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let (n, from) = match self.socket.recv_from(buf).await {
                Ok(x) => x,
                // Windows reports "the peer's port is closed" via ICMP as a recv error
                // (WSAECONNRESET). The peer has simply not started yet, so do not treat it as a
                // disconnect.
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            };
            match self.peer {
                Some(peer) if peer != from => {} // anyone but the pinned peer: drop and wait
                Some(_) => return Ok(n),
                None => {
                    self.peer = Some(from);
                    return Ok(n);
                }
            }
        }
    }
}

/// A serial port (`tokio-serial`).
#[cfg(feature = "serial")]
pub mod serial {
    use std::io;

    use tokio_serial::SerialPortBuilderExt;
    pub use tokio_serial::SerialStream;

    use super::Stream;

    /// Open `path` (`/dev/ttyACM0` / `COM3`) at `baud`. 8N1, no flow control.
    ///
    /// Bandwidth, roughly: 115200 baud ≈ 11 KB/s. That is not enough for a 40-byte state at 1 kHz,
    /// so use USB CDC (where baud is ignored) or 921600 and up.
    pub fn open(path: &str, baud: u32) -> io::Result<Stream<SerialStream>> {
        let port = tokio_serial::new(path, baud)
            .open_native_async()
            .map_err(io::Error::other)?;
        Ok(Stream(port))
    }
}
