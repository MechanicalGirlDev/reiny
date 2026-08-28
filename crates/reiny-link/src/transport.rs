//! ホスト側の「バイトを運ぶ者」—— [`Transport`]。
//!
//! [`Link`](crate::Link) が知らない I/O をここで足す。stream(シリアル / TCP / pty /
//! `tokio::io::duplex`)は [`Stream`] で包むだけ、datagram は [`Udp`]。自前の運び手は
//! `send` / `recv` の 2 メソッドで参加できる。

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{ToSocketAddrs, UdpSocket};

/// バイトを送れる者。
///
/// `DATAGRAM` が true なら `send` は 1 フレームを 1 datagram として送り、`recv` は
/// 1 datagram を返す。false なら任意の切れ方の stream。
pub trait Transport: Send {
    /// 境界を保つ運び手か(UDP)。
    const DATAGRAM: bool;

    /// バイトを送る。stream なら全部書き切る。
    fn send(&mut self, bytes: &[u8]) -> impl Future<Output = io::Result<()>> + Send;

    /// バイトを受ける。`Ok(0)` は stream では EOF、datagram では空の datagram。
    fn recv(&mut self, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send;
}

/// `AsyncRead + AsyncWrite` なら何でも stream の運び手になる。
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

/// UDP の点対点。
///
/// 相手のアドレスは [`Udp::with_peer`] で固定するか、最初に届いた datagram の送り元で覚える。
/// どちらの側も相手を知らなければ何も始まらないので、少なくとも片方は固定すること
/// (MCU がホストのアドレスを知っている、が典型)。固定した相手以外からの datagram は捨てる。
pub struct Udp {
    socket: UdpSocket,
    peer: Option<SocketAddr>,
}

impl Udp {
    /// `local` に bind する。
    pub async fn bind(local: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            socket: UdpSocket::bind(local).await?,
            peer: None,
        })
    }

    /// 相手を固定する。
    #[must_use]
    pub fn with_peer(mut self, peer: SocketAddr) -> Self {
        self.peer = Some(peer);
        self
    }

    /// いま送り先にしている相手。
    #[must_use]
    pub fn peer(&self) -> Option<SocketAddr> {
        self.peer
    }

    /// bind したアドレス。
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl Transport for Udp {
    const DATAGRAM: bool = true;

    async fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self.peer {
            Some(peer) => self.socket.send_to(bytes, peer).await.map(|_| ()),
            None => Ok(()), // 相手が分かるまでは捨てる(Hello は周期的に出し直される)
        }
    }

    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let (n, from) = match self.socket.recv_from(buf).await {
                Ok(x) => x,
                // Windows は「相手のポートが閉じている」を ICMP 経由で recv のエラーにする
                // (WSAECONNRESET)。相手がまだ起動していないだけなので、切断にしない。
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
                Some(peer) if peer != from => {} // 固定した相手以外: 捨てて次を待つ
                Some(_) => return Ok(n),
                None => {
                    self.peer = Some(from);
                    return Ok(n);
                }
            }
        }
    }
}

/// シリアルポート(`tokio-serial`)。
#[cfg(feature = "serial")]
pub mod serial {
    use std::io;

    use tokio_serial::SerialPortBuilderExt;
    pub use tokio_serial::SerialStream;

    use super::Stream;

    /// `path`(`/dev/ttyACM0` / `COM3`)を `baud` で開く。8N1、フロー制御なし。
    ///
    /// 帯域の目安: 115200 baud ≈ 11 KB/s。40 バイトの状態量を 1 kHz なら足りないので、
    /// USB CDC(baud は無視される)か 921600 以上を。
    pub fn open(path: &str, baud: u32) -> io::Result<Stream<SerialStream>> {
        let port = tokio_serial::new(path, baud)
            .open_native_async()
            .map_err(io::Error::other)?;
        Ok(Stream(port))
    }
}
