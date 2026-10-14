//! [`Link`] — the sans-I/O state machine of a point-to-point link.
//!
//! It owns no I/O. Feed the bytes you received to [`Link::feed`], take the bytes to send out of
//! [`Link::drain`] (stream) / [`Link::drain_frame`] (datagram). It owns no clock either, so liveness
//! is driven by handing the current time (ms) to [`Link::tick`]. Whether it sits on UART / USB CDC /
//! UDP / embassy / RTIC / tokio is decided by a few lines on the caller's side.
//!
//! How a connection goes:
//!
//! 1. Both sides send a Hello (id + the list of types they declare) at startup. On receiving the
//!    peer's Hello, a Hello with `ack` is sent back (an ack is never answered).
//! 2. Receiving a Hello is what makes the link `Connected`. The peer's declared types are kept in a
//!    table (hash → flags / schema), and `send` of a type the peer does not subscribe to never
//!    reaches the wire.
//! 3. After `ping_interval_ms` of silence a Ping goes out; after `timeout_ms` without receiving
//!    anything the link goes `Disconnected`. While disconnected, a fresh Hello is re-sent every
//!    `ping_interval_ms` (so whether the peer starts later or the cable is unplugged and back, the
//!    link comes up the moment either Hello lands).
//!
//! The schema fingerprint (`MessageType::SCHEMA_FINGERPRINT`) is checked exactly once, at Hello, and Data of a
//! mismatching type is dropped. Every declared type must carry a fingerprint.

use core::fmt;

use prost::Message;
use reiny_core::{MessageType, PortDirection, PortType, Service, StaticPort};

use crate::wire::{self, DELIMITER, HEADER, HelloEntry, Kind, OVERHEAD, flags};

/// The liveness knobs. Latency differs per physical layer, so the defaults are a starting point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkConfig {
    /// Send a Ping after this much silence on the send side (and, while disconnected, the interval
    /// at which the Hello is re-sent).
    pub ping_interval_ms: u32,
    /// Go `Disconnected` after this long without receiving anything. Two to three times
    /// `ping_interval_ms` is a reasonable choice.
    pub timeout_ms: u32,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            ping_interval_ms: 1000,
            timeout_ms: 3000,
        }
    }
}

/// The maximum id length in bytes. Hello's `id_len` is a u8, so it is at most that; this value is
/// the fixed size used in the table.
pub const ID_MAX: usize = 32;

#[derive(Clone, Copy)]
struct IdBuf {
    bytes: [u8; ID_MAX],
    len: u8,
}

impl IdBuf {
    fn new(s: &str) -> Result<Self, Error> {
        let len = u8::try_from(s.len())
            .ok()
            .filter(|&n| usize::from(n) <= ID_MAX)
            .ok_or(Error::IdTooLong)?;
        let mut bytes = [0u8; ID_MAX];
        bytes[..s.len()].copy_from_slice(s.as_bytes());
        Ok(Self { bytes, len })
    }

    fn as_str(&self) -> &str {
        // Copied wholesale out of a `&str`, so it is always UTF-8.
        core::str::from_utf8(&self.bytes[..usize::from(self.len)]).unwrap_or("")
    }
}

#[derive(Clone, Copy)]
struct LocalType {
    hash: u32,
    name: &'static str,
    schema: u64,
    flags: u8,
    port: &'static StaticPort,
}

struct PeerContract<const N: usize> {
    peer: IdBuf,
    remote: [Option<RemoteType>; N],
    ack: bool,
    bridge: bool,
}

/// One type's worth of what the peer declared in its Hello.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteType {
    /// See [`wire::endpoint_hash`].
    pub hash: u32,
    /// The OR of [`wire::flags`].
    pub flags: u8,
    /// The peer's `MessageType::SCHEMA_FINGERPRINT`.
    pub schema: u64,
    /// This and our own `SCHEMA_FINGERPRINT` differ. Data of this type is dropped.
    pub mismatch: bool,
}

/// A handle on a received frame. Read the contents with [`Link::payload`] / [`Link::decode`].
///
/// It is `Copy`, so it can be passed around, but the bytes it points at are only valid **until the
/// next [`Link::next`] / [`Link::feed`]**. Reading `payload` through a stale handle returns an
/// empty slice, and `decode` through one returns `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    /// The kind.
    pub kind: Kind,
    /// The exact output endpoint hash.
    pub hash: u32,
    /// Running counter / correlation id.
    pub seq: u8,
    len: u16,
    generation: u16,
}

/// What [`Link::next`] / [`Link::tick`] report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// The peer's Hello arrived (the first one, or the peer restarted). The payload is the Hello
    /// itself (`wire::hello_parse` reads it down to the type names). Latched types should be
    /// re-sent here.
    Connected(Frame),
    /// Nothing was received for `timeout_ms`.
    Disconnected,
    /// Data of a subscribed type.
    Data(Frame),
    /// A Request for a served request type. Answer with [`Link::reply`] / [`Link::reply_err`].
    Request(Frame),
    /// The response to our own [`Link::request`]. Match it up by `hash` / `seq`.
    Reply(Frame),
    /// An error response to our own [`Link::request`] (see [`Link::error_message`]).
    Error(Frame),
}

/// How much was thrown away. For debugging.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Frames whose COBS / CRC / kind was broken.
    pub bad_frames: u32,
    /// Data dropped because the schema fingerprints did not match.
    pub dropped_schema: u32,
    /// Data of an unsubscribed type, or a Request for an unserved type.
    pub dropped_unwanted: u32,
    /// Frames longer than the receive buffer (it filled before a delimiter arrived).
    pub rx_overflow: u32,
    /// How many of the peer's declared types did not fit the table (`N`).
    pub remote_overflow: u32,
    /// Frames that could not be sent because the send buffer was full.
    pub tx_full: u32,
}

/// A [`Link`] failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// The id is longer than [`ID_MAX`].
    IdTooLong,
    /// More than `N` types were declared.
    TableFull,
    /// Two endpoint addresses hashed to the same value. Choose a different port name.
    HashCollision {
        /// The type name that was registered first.
        existing: &'static str,
        /// The colliding type name.
        new: &'static str,
    },
    /// The same type name was declared with different fingerprints.
    SchemaMismatch,
    /// A type was declared after I/O had started. Declarations must precede the Hello, or the peer
    /// never hears about them.
    Started,
    /// `send` of a type that was never passed to `publishes`.
    NotDeclared,
    /// The named port is absent from the generated table.
    UnknownPort,
    /// The named port does not permit this direction.
    WrongDirection,
    /// The Rust type does not match the named port.
    WrongType,
    /// The static table contains duplicate or invalid port contracts.
    InvalidPorts,
    /// The peer does not serve that request type (which includes not being connected).
    NoPeerService,
    /// The frame does not fit `FRAME`.
    TooLarge,
    /// No room left in the send buffer. `drain` and retry.
    Full,
    /// prost failed to encode.
    Encode,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdTooLong => write!(f, "id longer than {ID_MAX} bytes"),
            Self::TableFull => f.write_str("type table full (raise N)"),
            Self::HashCollision { existing, new } => {
                write!(
                    f,
                    "endpoint hash collision between '{existing}' and '{new}'"
                )
            }
            Self::SchemaMismatch => f.write_str("schema fingerprint mismatch"),
            Self::Started => f.write_str("types must be declared before the link starts"),
            Self::NotDeclared => f.write_str("port not declared"),
            Self::UnknownPort => f.write_str("port absent from the static table"),
            Self::WrongDirection => f.write_str("port direction mismatch"),
            Self::WrongType => f.write_str("port type mismatch"),
            Self::InvalidPorts => f.write_str("invalid static port table"),
            Self::NoPeerService => f.write_str("peer does not serve this request type"),
            Self::TooLarge => f.write_str("frame does not fit FRAME"),
            Self::Full => f.write_str("tx buffer full"),
            Self::Encode => f.write_str("prost encode failed"),
        }
    }
}

impl core::error::Error for Error {}

/// The point-to-point link's state machine.
///
/// - `B`: the type of the send / receive buffers. `[u8; N]`, `&mut [u8]` or `Vec<u8>` all work.
/// - `N`: the cap on how many types we declare, and separately on how many the peer declares.
/// - `FRAME`: the cap on a raw frame (header + payload + CRC). The payload goes up to `FRAME - 11`.
///
/// Make the send buffer at least [`wire::encoded_max`]`(FRAME)`, and the receive buffer at least
/// that much again, sized to the peer's `FRAME`.
#[allow(clippy::struct_excessive_bools)] // started / connected / bridge / peer_bridge are orthogonal
pub struct Link<B, const N: usize, const FRAME: usize = 256> {
    id: IdBuf,
    peer: Option<IdBuf>,
    local: [Option<LocalType>; N],
    ports: &'static [StaticPort],
    declared: [Option<&'static StaticPort>; N],
    remote: [Option<RemoteType>; N],
    peer_hello: [u8; FRAME],
    peer_hello_len: usize,
    pending: [Option<(u32, u8)>; N],
    tx: B,
    tx_len: usize,
    rx: B,
    rx_len: usize,
    /// How many bytes the frame returned by the previous `next` occupied. Dropped on the following
    /// `next` / `feed` (deferred consumption).
    rx_consumed: usize,
    /// After an overflow: currently discarding up to the next delimiter.
    rx_skip: bool,
    rx_generation: u16,
    scratch: [u8; FRAME],
    config: LinkConfig,
    started: bool,
    connected: bool,
    /// We are the bridge (the side that mirrors the peer's declarations, see
    /// [`wire::HELLO_BRIDGE`]).
    bridge: bool,
    /// The peer declared itself a bridge.
    peer_bridge: bool,
    data_seq: u8,
    req_seq: u8,
    now_ms: u32,
    last_rx_ms: u32,
    last_tx_ms: u32,
    stats: Stats,
}

impl<B: AsRef<[u8]> + AsMut<[u8]>, const N: usize, const FRAME: usize> Link<B, N, FRAME> {
    /// The cap on one frame including COBS and the delimiter. Make `drain_frame`'s `out` at least
    /// this large.
    pub const MAX_ENCODED: usize = wire::encoded_max(FRAME);

    /// Build one from an id and the two buffers. Finish declaring types ([`Link::publishes`] and
    /// friends) before starting any I/O.
    ///
    /// The generated static port table is mandatory:
    ///
    /// ```compile_fail,E0061
    /// let link = reiny_link::Link::<_, 8>::new("board", [0u8; 512], [0u8; 512]);
    /// ```
    ///
    /// Application code cannot enable the internal bridge adapter:
    ///
    /// ```compile_fail,E0624
    /// let link = reiny_link::Link::<_, 8>::new("board", &[], [0u8; 512], [0u8; 512])
    ///     .unwrap().into_bridge();
    /// ```
    pub fn new(id: &str, ports: &'static [StaticPort], tx: B, rx: B) -> Result<Self, Error> {
        if FRAME < OVERHEAD {
            return Err(Error::TooLarge);
        }
        for (index, port) in ports.iter().enumerate() {
            let service = matches!(
                port.direction,
                PortDirection::RpcServer | PortDirection::RpcClient
            );
            if !wire::valid_port(port.name)
                || port.ty.is_empty()
                || (matches!(
                    port.direction,
                    PortDirection::Input | PortDirection::RpcClient
                ) && port.sources.is_empty())
                || (matches!(
                    port.direction,
                    PortDirection::Output | PortDirection::RpcServer
                ) && !port.sources.is_empty())
                || (port.direction == PortDirection::RpcClient && port.sources.len() != 1)
                || port.sources.iter().enumerate().any(|(i, source)| {
                    !wire::valid_namespace(source.namespace)
                        || !wire::valid_port(source.port)
                        || port.sources[..i].contains(source)
                })
                || service != port.response.is_some()
                || ports[..index].iter().any(|other| {
                    other.name == port.name
                        && matches!(
                            other.direction,
                            PortDirection::Output | PortDirection::RpcServer
                        ) == matches!(
                            port.direction,
                            PortDirection::Output | PortDirection::RpcServer
                        )
                })
            {
                return Err(Error::InvalidPorts);
            }
        }
        if !wire::valid_namespace(id) {
            return Err(Error::InvalidPorts);
        }
        Ok(Self {
            id: IdBuf::new(id)?,
            peer: None,
            local: [None; N],
            ports,
            declared: [None; N],
            remote: [None; N],
            peer_hello: [0; FRAME],
            peer_hello_len: 0,
            pending: [None; N],
            tx,
            tx_len: 0,
            rx,
            rx_len: 0,
            rx_consumed: 0,
            rx_skip: false,
            rx_generation: 0,
            scratch: [0; FRAME],
            config: LinkConfig::default(),
            started: false,
            connected: false,
            bridge: false,
            peer_bridge: false,
            data_seq: 0,
            req_seq: 0,
            now_ms: 0,
            last_rx_ms: 0,
            last_tx_ms: 0,
            stats: Stats::default(),
        })
    }

    /// Replace the liveness knobs.
    #[must_use]
    pub fn with_config(mut self, config: LinkConfig) -> Self {
        self.config = config;
        self
    }

    /// Behave as a **bridge**: declare no types, accept everything the peer publishes, be allowed to
    /// send everything the peer subscribes to, and serve every request type the peer calls (a mirror
    /// of the peer's declarations). This is the bridge to zenoh (`reiny bridge serial …`). Like a
    /// declaration, set it before I/O starts. A bridge has no type table of its own, so it checks no
    /// fingerprints — it carries the fingerprints from the peer's Hello straight across to the far
    /// side (zenoh's attachment).
    #[must_use]
    #[cfg(any(feature = "std", test))]
    pub(crate) fn into_bridge(mut self) -> Self {
        self.bridge = true;
        self
    }

    /// Whether we are a bridge.
    #[must_use]
    pub fn is_bridge(&self) -> bool {
        self.bridge
    }

    /// Our own id.
    #[must_use]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    // -----------------------------------------------------------------------
    // Declarations
    // -----------------------------------------------------------------------

    /// Publish the type `T`.
    ///
    /// A type-only declaration cannot bypass the named table:
    ///
    /// ```compile_fail,E0061
    /// struct State;
    /// impl reiny_link::MessageType for State {
    ///     const TYPE_NAME: &'static str = "State";
    ///     const SCHEMA_FINGERPRINT: u64 = 1;
    ///     const DESCRIPTOR: Option<reiny_link::Descriptor> = None;
    /// }
    /// let mut link = reiny_link::Link::<_, 8>::new("board", &[], [0u8; 512], [0u8; 512])
    ///     .unwrap();
    /// link.publishes::<State>();
    /// ```
    pub fn publishes<T: MessageType>(&mut self, name: &str) -> Result<(), Error> {
        self.declare::<T>(name, PortDirection::Output, flags::PUB, None)
    }

    /// Publish the type `T` latched — a promise to re-send the most recent value on every
    /// `Connected`. The re-sending itself is the caller's job (`Link` holds no values).
    pub fn publishes_latched<T: MessageType>(&mut self, name: &str) -> Result<(), Error> {
        self.declare::<T>(
            name,
            PortDirection::Output,
            flags::PUB | flags::LATCHED,
            None,
        )
    }

    /// Subscribe to the type `T`.
    pub fn subscribes<T: MessageType>(&mut self, name: &str) -> Result<(), Error> {
        self.declare::<T>(name, PortDirection::Input, flags::SUB, None)
    }

    /// Serve the request type `S`.
    pub fn rpc_server<S: Service>(&mut self, name: &str) -> Result<(), Error> {
        self.declare_rpc::<S>(name, PortDirection::RpcServer, flags::SERVE)
    }

    /// Declare the named client port for request type `S`.
    pub fn rpc_client<S: Service>(&mut self, name: &str) -> Result<(), Error> {
        self.declare_rpc::<S>(name, PortDirection::RpcClient, flags::CALLS)
    }

    fn declare_rpc<S: Service>(
        &mut self,
        name: &str,
        direction: PortDirection,
        flag: u8,
    ) -> Result<(), Error> {
        self.declare::<S>(
            name,
            direction,
            flag,
            Some(PortType {
                ty: S::Response::TYPE_NAME,
                schema: S::Response::SCHEMA_FINGERPRINT,
            }),
        )
    }

    fn port<T: MessageType>(
        &self,
        name: &str,
        direction: PortDirection,
        response: Option<PortType>,
    ) -> Result<&'static StaticPort, Error> {
        let port = self
            .ports
            .iter()
            .find(|port| port.name == name && port.direction == direction)
            .ok_or_else(|| {
                if self.ports.iter().any(|port| port.name == name) {
                    Error::WrongDirection
                } else {
                    Error::UnknownPort
                }
            })?;
        if port.ty != T::TYPE_NAME || port.response.map(|ty| ty.ty) != response.map(|ty| ty.ty) {
            return Err(Error::WrongType);
        }
        if port.schema != T::SCHEMA_FINGERPRINT
            || port.response.map(|ty| ty.schema) != response.map(|ty| ty.schema)
        {
            return Err(Error::SchemaMismatch);
        }
        Ok(port)
    }

    pub(crate) fn check_port<T: MessageType>(
        &self,
        name: &str,
        direction: PortDirection,
        response: Option<PortType>,
    ) -> Result<(), Error> {
        let port = self.port::<T>(name, direction, response)?;
        if !self
            .declared
            .iter()
            .flatten()
            .any(|declared| core::ptr::eq(*declared, port))
        {
            return Err(Error::NotDeclared);
        }
        Ok(())
    }

    fn declare<T: MessageType>(
        &mut self,
        name: &str,
        direction: PortDirection,
        flag: u8,
        response: Option<PortType>,
    ) -> Result<(), Error> {
        if self.started {
            return Err(Error::Started);
        }
        let port = self.port::<T>(name, direction, response)?;
        let already_declared = self
            .declared
            .iter()
            .flatten()
            .any(|declared| core::ptr::eq(*declared, port));
        if !already_declared && self.declared.iter().all(Option::is_some) {
            return Err(Error::TableFull);
        }
        let previous = self.local;
        self.declare_type(port, flag)?;
        if let Err(error) = self.hello_payload(false) {
            self.local = previous;
            return Err(error);
        }
        if !already_declared {
            let free = self
                .declared
                .iter_mut()
                .find(|slot| slot.is_none())
                .ok_or(Error::TableFull)?;
            *free = Some(port);
        }
        Ok(())
    }

    fn declare_type(&mut self, port: &'static StaticPort, flag: u8) -> Result<(), Error> {
        let hash = wire::endpoint_hash(self.id(), port.name);
        for slot in self.local.iter_mut().flatten() {
            if slot.hash == hash && slot.port.name != port.name {
                return Err(Error::HashCollision {
                    existing: slot.port.name,
                    new: port.name,
                });
            }
            if core::ptr::eq(slot.port, port) {
                slot.flags |= flag;
                return Ok(());
            }
        }
        let free = self
            .local
            .iter_mut()
            .find(|s| s.is_none())
            .ok_or(Error::TableFull)?;
        *free = Some(LocalType {
            hash,
            name: port.ty,
            schema: port.schema,
            flags: flag,
            port,
        });
        Ok(())
    }

    fn local_type(&self, hash: u32) -> Option<&LocalType> {
        self.local
            .iter()
            .flatten()
            .find(|t| t.hash == hash && t.flags & (flags::PUB | flags::SERVE) != 0)
    }

    fn local_source(&self, hash: u32, flag: u8) -> Option<&LocalType> {
        self.local.iter().flatten().find(|t| {
            t.flags & flag != 0
                && t.port
                    .sources
                    .iter()
                    .any(|s| wire::endpoint_hash(s.namespace, s.port) == hash)
        })
    }

    fn remote_type(&self, hash: u32) -> Option<&RemoteType> {
        self.remote
            .iter()
            .flatten()
            .find(|t| t.hash == hash && t.flags & (flags::PUB | flags::SERVE) != 0)
    }

    fn hello(&self) -> Option<wire::Hello<'_>> {
        wire::hello_parse(&self.peer_hello[..self.peer_hello_len]).ok()
    }

    fn remote_source(&self, hash: u32, flag: u8) -> Option<HelloEntry<'_>> {
        self.hello()?.entries().find(|e| {
            e.flags & flag != 0
                && e.sources
                    .iter()
                    .any(|s| wire::endpoint_hash(s.namespace, s.port) == hash)
        })
    }

    /// Resolve one declared RPC caller to its exact destination server.
    pub(crate) fn request_hash<S: Service>(&self, name: &str) -> Result<u32, Error> {
        self.check_port::<S>(
            name,
            PortDirection::RpcClient,
            Some(PortType {
                ty: S::Response::TYPE_NAME,
                schema: S::Response::SCHEMA_FINGERPRINT,
            }),
        )?;
        let port = self.port::<S>(
            name,
            PortDirection::RpcClient,
            Some(PortType {
                ty: S::Response::TYPE_NAME,
                schema: S::Response::SCHEMA_FINGERPRINT,
            }),
        )?;
        let source = port.sources.first().ok_or(Error::InvalidPorts)?;
        Ok(wire::endpoint_hash(source.namespace, source.port))
    }

    /// Release a timed-out request without reusing another live correlation slot.
    pub(crate) fn cancel_request(&mut self, hash: u32, seq: u8) {
        if let Some(slot) = self
            .pending
            .iter_mut()
            .find(|slot| **slot == Some((hash, seq)))
        {
            *slot = None;
        }
    }

    // -----------------------------------------------------------------------
    // I/O
    // -----------------------------------------------------------------------

    /// Hand over the bytes received. Any split is fine. Returns how much was accepted — which is
    /// short only when the receive buffer holds a frame nobody has taken yet, so run [`Link::next`]
    /// and hand over the rest afterwards.
    pub fn feed(&mut self, bytes: &[u8]) -> usize {
        self.ensure_started();
        self.rx_compact();
        let mut rest = bytes;
        while !rest.is_empty() {
            if self.rx_skip {
                // After an overflow: discard up to the next delimiter to resynchronize.
                match rest.iter().position(|&b| b == DELIMITER) {
                    Some(i) => {
                        rest = &rest[i + 1..];
                        self.rx_skip = false;
                    }
                    None => return bytes.len(),
                }
                continue;
            }
            let free = self.rx.as_ref().len() - self.rx_len;
            if free == 0 {
                if self.rx.as_ref()[..self.rx_len].contains(&DELIMITER) {
                    // Only a completed frame nobody has taken. The caller needs to run next().
                    return bytes.len() - rest.len();
                }
                // Full before a delimiter arrived = the frame is longer than the buffer. Discard and
                // resynchronize.
                self.rx_len = 0;
                self.rx_skip = true;
                self.stats.rx_overflow = self.stats.rx_overflow.wrapping_add(1);
                continue;
            }
            let n = free.min(rest.len());
            let rx = self.rx.as_mut();
            rx[self.rx_len..self.rx_len + n].copy_from_slice(&rest[..n]);
            self.rx_len += n;
            rest = &rest[n..];
        }
        bytes.len()
    }

    /// Copy the bytes to send into `out` and return the length (for streams: any split will do).
    /// Zero means there is nothing to send.
    pub fn drain(&mut self, out: &mut [u8]) -> usize {
        self.ensure_started();
        let n = self.tx_len.min(out.len());
        out[..n].copy_from_slice(&self.tx.as_ref()[..n]);
        self.tx_consume(n);
        n
    }

    /// Copy **exactly one whole frame** to send into `out` and return its length (for datagrams:
    /// one frame = one datagram). `None` means there is nothing to send. A frame that does not fit
    /// because `out` is shorter than [`Link::MAX_ENCODED`] is dropped (rather than blocking the
    /// queue).
    pub fn drain_frame(&mut self, out: &mut [u8]) -> Option<usize> {
        self.ensure_started();
        let end = self.tx.as_ref()[..self.tx_len]
            .iter()
            .position(|&b| b == DELIMITER)?
            + 1;
        if end > out.len() {
            self.tx_consume(end);
            self.stats.tx_full = self.stats.tx_full.wrapping_add(1);
            return None;
        }
        out[..end].copy_from_slice(&self.tx.as_ref()[..end]);
        self.tx_consume(end);
        Some(end)
    }

    /// How many bytes are waiting to be sent.
    #[must_use]
    pub fn tx_pending(&self) -> usize {
        self.tx_len
    }

    /// Advance the clock. `now_ms` is monotonic milliseconds (wrapping is fine). Ping / Hello
    /// re-sending and the `Disconnected` decision happen nowhere else, so call it regularly.
    pub fn tick(&mut self, now_ms: u32) -> Option<Event> {
        self.now_ms = now_ms;
        if !self.started {
            self.last_rx_ms = now_ms;
            self.ensure_started();
            return None;
        }
        let since_rx = now_ms.wrapping_sub(self.last_rx_ms);
        let since_tx = now_ms.wrapping_sub(self.last_tx_ms);
        if self.connected {
            if since_rx > self.config.timeout_ms {
                self.connected = false;
                self.peer = None;
                self.peer_bridge = false;
                self.remote = [None; N];
                self.peer_hello_len = 0;
                self.pending = [None; N];
                return Some(Event::Disconnected);
            }
            if since_tx >= self.config.ping_interval_ms {
                let _ = self.emit(Kind::Ping, 0, 0, 0);
            }
        } else if since_tx >= self.config.ping_interval_ms {
            let _ = self.queue_hello(false);
        }
        None
    }

    /// Take the next event out of the receive buffer, or `None` if there is none.
    ///
    /// Hello / Ping are handled internally (a Hello surfaces as `Connected`). Data of an
    /// unsubscribed type, Data whose fingerprint mismatches, and Requests for an unserved type are
    /// all dropped (the last of those gets an error response). The [`Frame`] returned is valid
    /// until the next `next` / `feed`.
    ///
    /// It is not an `Iterator` because reading the contents of the [`Frame`] it returns needs
    /// `&self` (the handle scheme, a borrow-checker consequence), which cannot be shaped into
    /// something a `for` loop drives. Only the name matches.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Event> {
        self.ensure_started();
        self.rx_compact();
        loop {
            let end = self.rx.as_ref()[..self.rx_len]
                .iter()
                .position(|&b| b == DELIMITER)?;
            let consumed = end + 1;
            if end == 0 {
                self.rx_discard(consumed);
                continue;
            }
            let Some(frame) = self.decode_frame(end) else {
                self.stats.bad_frames = self.stats.bad_frames.wrapping_add(1);
                self.rx_discard(consumed);
                continue;
            };
            // From here on the frame is valid. Consume it lazily: if we return it, it is dropped on
            // the next call.
            self.rx_consumed = consumed;
            self.rx_generation = self.rx_generation.wrapping_add(1);
            self.last_rx_ms = self.now_ms;
            let frame = Frame {
                generation: self.rx_generation,
                ..frame
            };
            if let Some(event) = self.dispatch(frame) {
                return Some(event);
            }
            self.rx_compact();
        }
    }

    /// COBS-decode `rx[..end]` and read the header. `None` if it is broken.
    fn decode_frame(&mut self, end: usize) -> Option<Frame> {
        let rx = self.rx.as_mut();
        let raw_len = wire::decode_in_place(&mut rx[..end]).ok()?;
        if raw_len > FRAME {
            return None;
        }
        let (header, payload) = wire::parse(&rx[..raw_len]).ok()?;
        Some(Frame {
            kind: header.kind,
            hash: header.hash,
            seq: header.seq,
            len: u16::try_from(payload.len()).ok()?,
            generation: 0,
        })
    }

    /// Handle a decoded frame according to its kind. `Some` if it goes back to the caller.
    fn dispatch(&mut self, frame: Frame) -> Option<Event> {
        match frame.kind {
            Kind::Hello => self.on_hello(frame),
            Kind::Ping => None,
            Kind::Data => {
                let wanted = if self.bridge {
                    self.remote_type(frame.hash)
                        .is_some_and(|t| t.flags & flags::PUB != 0)
                } else {
                    self.local_source(frame.hash, flags::SUB).is_some()
                };
                if !wanted {
                    self.stats.dropped_unwanted = self.stats.dropped_unwanted.wrapping_add(1);
                    return None;
                }
                if !self.connected
                    || (!self.peer_bridge
                        && self
                            .remote_type(frame.hash)
                            .is_none_or(|t| t.mismatch || t.flags & flags::PUB == 0))
                {
                    self.stats.dropped_schema = self.stats.dropped_schema.wrapping_add(1);
                    return None;
                }
                Some(Event::Data(frame))
            }
            Kind::Request => {
                let served = self
                    .local_type(frame.hash)
                    .is_some_and(|t| t.flags & flags::SERVE != 0);
                let caller = self.remote_source(frame.hash, flags::CALLS).is_some();
                if !self.connected || (!self.peer_bridge && !caller) {
                    self.stats.dropped_unwanted = self.stats.dropped_unwanted.wrapping_add(1);
                    let _ = self.emit_error(frame.hash, frame.seq, "undeclared source");
                    return None;
                }
                if served || (self.bridge && caller) {
                    Some(Event::Request(frame))
                } else {
                    self.stats.dropped_unwanted = self.stats.dropped_unwanted.wrapping_add(1);
                    let _ = self.emit_error(frame.hash, frame.seq, "no such service");
                    None
                }
            }
            Kind::Reply | Kind::Error => {
                if !self.connected || !self.pending.contains(&Some((frame.hash, frame.seq))) {
                    self.stats.dropped_unwanted = self.stats.dropped_unwanted.wrapping_add(1);
                    return None;
                }
                self.cancel_request(frame.hash, frame.seq);
                Some(if frame.kind == Kind::Reply {
                    Event::Reply(frame)
                } else {
                    Event::Error(frame)
                })
            }
        }
    }

    /// Validate every entry before publishing any part of a peer contract.
    fn on_hello(&mut self, frame: Frame) -> Option<Event> {
        let payload = &self.rx.as_ref()[HEADER..HEADER + usize::from(frame.len)];
        let result = self.validate_hello(payload);
        let Ok(PeerContract {
            peer,
            remote,
            ack,
            bridge,
        }) = result
        else {
            self.stats.bad_frames = self.stats.bad_frames.wrapping_add(1);
            if matches!(result, Err(Error::TableFull)) {
                self.stats.remote_overflow = self.stats.remote_overflow.wrapping_add(1);
            }
            let connected = self.connected;
            self.connected = false;
            self.peer = None;
            self.peer_bridge = false;
            self.remote = [None; N];
            self.peer_hello_len = 0;
            self.pending = [None; N];
            return connected.then_some(Event::Disconnected);
        };
        let was_connected = self.connected;
        self.peer_hello[..payload.len()].copy_from_slice(payload);
        self.peer_hello_len = payload.len();
        self.peer = Some(peer);
        self.peer_bridge = bridge;
        self.remote = remote;
        self.connected = true;
        if !ack {
            let _ = self.queue_hello(true);
        }
        (!ack || !was_connected).then_some(Event::Connected(frame))
    }

    fn validate_hello(&self, payload: &[u8]) -> Result<PeerContract<N>, Error> {
        let hello = wire::hello_parse(payload).map_err(|_| Error::InvalidPorts)?;
        let peer = IdBuf::new(hello.id)?;
        if payload.len() > FRAME {
            return Err(Error::TooLarge);
        }
        if self.connected
            && (self.peer_hello_len != payload.len()
                || self.peer_hello[0] & !wire::HELLO_ACK != payload[0] & !wire::HELLO_ACK
                || self.peer_hello[1..self.peer_hello_len] != payload[1..])
        {
            return Err(Error::InvalidPorts);
        }
        let mut remote = [None; N];
        for (index, entry) in hello.entries().enumerate() {
            if index >= N {
                return Err(Error::TableFull);
            }
            for local in self.local.iter().flatten() {
                let from_peer = local
                    .port
                    .sources
                    .iter()
                    .any(|s| s.namespace == hello.id && s.port == entry.port)
                    && entry.flags & (flags::PUB | flags::SERVE) != 0;
                let to_peer = entry
                    .sources
                    .iter()
                    .any(|s| s.namespace == self.id() && s.port == local.port.name)
                    && local.flags & (flags::PUB | flags::SERVE) != 0;
                if (from_peer || to_peer)
                    && (local.name != entry.name
                        || local.schema != entry.schema
                        || local.port.response.map(|r| (r.ty, r.schema))
                            != entry.response.map(|r| (r.name, r.schema))
                        || (local.flags & (flags::CALLS | flags::SERVE) != 0)
                            != (entry.flags & (flags::CALLS | flags::SERVE) != 0))
                {
                    return Err(Error::SchemaMismatch);
                }
                // Include our source addresses in collision checks against the entire peer contract.
                for source in local.port.sources {
                    if wire::endpoint_hash(source.namespace, source.port) == entry.hash
                        && (source.namespace != hello.id || source.port != entry.port)
                    {
                        return Err(Error::InvalidPorts);
                    }
                }
            }
            let remote_addresses = core::iter::once(wire::EndpointRef {
                namespace: hello.id,
                port: entry.port,
            })
            .chain(entry.sources.iter());
            for address in remote_addresses {
                for local in self.local.iter().flatten() {
                    let own = wire::EndpointRef {
                        namespace: self.id(),
                        port: local.port.name,
                    };
                    let local_addresses =
                        core::iter::once(own).chain(local.port.sources.iter().map(|s| {
                            wire::EndpointRef {
                                namespace: s.namespace,
                                port: s.port,
                            }
                        }));
                    if local_addresses.into_iter().any(|other| {
                        other != address
                            && wire::endpoint_hash(other.namespace, other.port)
                                == wire::endpoint_hash(address.namespace, address.port)
                    }) {
                        return Err(Error::InvalidPorts);
                    }
                }
            }
            remote[index] = Some(RemoteType {
                hash: entry.hash,
                flags: entry.flags,
                schema: entry.schema,
                mismatch: false,
            });
        }
        Ok(PeerContract {
            peer,
            remote,
            ack: hello.ack,
            bridge: hello.bridge,
        })
    }

    // -----------------------------------------------------------------------
    // Sending
    // -----------------------------------------------------------------------

    /// Send the type `T`. If the peer does not subscribe to `T` nothing reaches the wire and this
    /// returns `Ok(false)`.
    pub fn send<T: MessageType + Message>(&mut self, name: &str, msg: &T) -> Result<bool, Error> {
        self.check_port::<T>(name, PortDirection::Output, None)?;
        let hash = wire::endpoint_hash(self.id(), name);
        self.check_publishes(hash)?;
        if self
            .local_type(hash)
            .is_some_and(|t| t.schema != T::SCHEMA_FINGERPRINT)
        {
            return Err(Error::SchemaMismatch);
        }
        if !self.peer_subscribes_hash(hash) {
            return Ok(false);
        }
        let len = self.encode_payload(msg)?;
        self.emit_data(hash, len)
    }

    /// The pre-encoded form of [`Link::send`]: send by type hash and prost bytes (the door through
    /// which a bridge passes on what it took off zenoh). Unless we are a bridge, the type still has
    /// to have been passed to `publishes`.
    #[cfg(any(feature = "engine", test))]
    pub(crate) fn send_raw(&mut self, hash: u32, payload: &[u8]) -> Result<bool, Error> {
        self.check_publishes(hash)?;
        if !self.peer_subscribes_hash(hash) {
            return Ok(false);
        }
        let len = self.copy_payload(payload)?;
        self.emit_data(hash, len)
    }

    /// Send the request type `S` and return the correlation id (`seq`). The response comes back as
    /// [`Event::Reply`] / [`Event::Error`].
    pub fn request<S: Service>(&mut self, name: &str, req: &S) -> Result<u8, Error> {
        self.check_port::<S>(
            name,
            PortDirection::RpcClient,
            Some(PortType {
                ty: S::Response::TYPE_NAME,
                schema: S::Response::SCHEMA_FINGERPRINT,
            }),
        )?;
        let hash = self.request_hash::<S>(name)?;
        if !self.peer_serves_hash(hash) {
            return Err(Error::NoPeerService);
        }
        if self
            .remote_type(hash)
            .is_some_and(|t| t.schema != S::SCHEMA_FINGERPRINT)
        {
            return Err(Error::SchemaMismatch);
        }
        let len = self.encode_payload(req)?;
        self.emit_request(hash, len)
    }

    /// The pre-encoded form of [`Link::request`].
    #[cfg(any(feature = "std", test))]
    pub(crate) fn request_raw(&mut self, hash: u32, payload: &[u8]) -> Result<u8, Error> {
        if !self.bridge && self.local_source(hash, flags::CALLS).is_none() {
            return Err(Error::NotDeclared);
        }
        if !self.peer_serves_hash(hash) {
            return Err(Error::NoPeerService);
        }
        let len = self.copy_payload(payload)?;
        self.emit_request(hash, len)
    }

    /// Answer an [`Event::Request`]. `seq` is the request frame's.
    pub fn reply<S: Service>(
        &mut self,
        name: &str,
        seq: u8,
        resp: &S::Response,
    ) -> Result<(), Error> {
        self.check_port::<S>(
            name,
            PortDirection::RpcServer,
            Some(PortType {
                ty: S::Response::TYPE_NAME,
                schema: S::Response::SCHEMA_FINGERPRINT,
            }),
        )?;
        let len = self.encode_payload(resp)?;
        self.emit_reply(
            wire::endpoint_hash(self.id(), name),
            seq,
            S::Response::SCHEMA_FINGERPRINT,
            len,
        )
    }

    /// The pre-encoded form of [`Link::reply`]. `hash` is the *request* type's.
    #[cfg(any(feature = "engine", test))]
    pub(crate) fn reply_raw(
        &mut self,
        seq: u8,
        hash: u32,
        schema: u64,
        payload: &[u8],
    ) -> Result<(), Error> {
        let len = self.copy_payload(payload)?;
        self.emit_reply(hash, seq, schema, len)
    }

    /// Answer an [`Event::Request`] with an error.
    pub fn reply_err<S: Service>(
        &mut self,
        name: &str,
        seq: u8,
        message: &str,
    ) -> Result<(), Error> {
        self.check_port::<S>(
            name,
            PortDirection::RpcServer,
            Some(PortType {
                ty: S::Response::TYPE_NAME,
                schema: S::Response::SCHEMA_FINGERPRINT,
            }),
        )?;
        self.emit_error(wire::endpoint_hash(self.id(), name), seq, message)
    }

    /// The type-hash form of [`Link::reply_err`].
    #[cfg(feature = "engine")]
    pub(crate) fn reply_err_raw(&mut self, seq: u8, hash: u32, message: &str) -> Result<(), Error> {
        self.emit_error(hash, seq, message)
    }

    fn emit_reply(&mut self, hash: u32, seq: u8, schema: u64, len: usize) -> Result<(), Error> {
        let reply_len = len.checked_add(8).ok_or(Error::TooLarge)?;
        if reply_len > FRAME.saturating_sub(OVERHEAD) {
            return Err(Error::TooLarge);
        }
        self.scratch.copy_within(HEADER..HEADER + len, HEADER + 8);
        self.scratch[HEADER..HEADER + 8].copy_from_slice(&schema.to_le_bytes());
        self.emit(Kind::Reply, hash, seq, reply_len)
    }

    fn check_publishes(&self, hash: u32) -> Result<(), Error> {
        if !self.bridge
            && self
                .local_type(hash)
                .is_none_or(|t| t.flags & flags::PUB == 0)
        {
            return Err(Error::NotDeclared);
        }
        Ok(())
    }

    fn emit_data(&mut self, hash: u32, len: usize) -> Result<bool, Error> {
        let seq = self.data_seq;
        self.data_seq = seq.wrapping_add(1);
        self.emit(Kind::Data, hash, seq, len)?;
        Ok(true)
    }

    fn emit_request(&mut self, hash: u32, len: usize) -> Result<u8, Error> {
        let free = self
            .pending
            .iter()
            .position(Option::is_none)
            .ok_or(Error::TableFull)?;
        let seq = (0..=u8::MAX)
            .map(|offset| self.req_seq.wrapping_add(offset))
            .find(|seq| !self.pending.contains(&Some((hash, *seq))))
            .ok_or(Error::TableFull)?;
        self.emit(Kind::Request, hash, seq, len)?;
        self.req_seq = seq.wrapping_add(1);
        self.pending[free] = Some((hash, seq));
        Ok(seq)
    }

    fn emit_error(&mut self, hash: u32, seq: u8, message: &str) -> Result<(), Error> {
        let bytes = message.as_bytes();
        let len = bytes.len().min(FRAME.saturating_sub(OVERHEAD));
        self.scratch[HEADER..HEADER + len].copy_from_slice(&bytes[..len]);
        self.emit(Kind::Error, hash, seq, len)
    }

    /// prost-encode into `scratch[HEADER..]` and return the payload length.
    fn encode_payload<M: Message>(&mut self, msg: &M) -> Result<usize, Error> {
        let len = msg.encoded_len();
        if len > FRAME.saturating_sub(OVERHEAD) {
            return Err(Error::TooLarge);
        }
        let mut slot: &mut [u8] = &mut self.scratch[HEADER..HEADER + len];
        msg.encode(&mut slot).map_err(|_| Error::Encode)?;
        Ok(len)
    }

    /// Copy pre-encoded bytes into `scratch[HEADER..]` and return the payload length.
    #[cfg(any(feature = "std", test))]
    fn copy_payload(&mut self, payload: &[u8]) -> Result<usize, Error> {
        if payload.len() > FRAME.saturating_sub(OVERHEAD) {
            return Err(Error::TooLarge);
        }
        self.scratch[HEADER..HEADER + payload.len()].copy_from_slice(payload);
        Ok(payload.len())
    }

    /// Seal the payload in `scratch` with a header and CRC, COBS-wrap it and push it onto the send
    /// buffer.
    fn emit(&mut self, kind: Kind, hash: u32, seq: u8, payload_len: usize) -> Result<(), Error> {
        let raw_len = wire::seal(
            &mut self.scratch,
            wire::Header { kind, hash, seq },
            payload_len,
        )
        .map_err(|_| Error::TooLarge)?;
        let tx = self.tx.as_mut();
        if tx.len() - self.tx_len < wire::encoded_max(raw_len) {
            self.stats.tx_full = self.stats.tx_full.wrapping_add(1);
            return Err(Error::Full);
        }
        let n = wire::encode(&self.scratch[..raw_len], &mut tx[self.tx_len..])
            .map_err(|_| Error::Full)?;
        self.tx_len += n;
        self.last_tx_ms = self.now_ms;
        Ok(())
    }

    fn hello_payload(&mut self, ack: bool) -> Result<usize, Error> {
        let entries = self.local.iter().flatten().map(|t| HelloEntry {
            hash: t.hash,
            flags: t.flags,
            schema: t.schema,
            name: t.name,
            port: t.port.name,
            response: t.port.response.map(|r| wire::HelloType {
                name: r.ty,
                schema: r.schema,
            }),
            sources: wire::Sources::Static(t.port.sources),
        });
        let len = wire::hello_write(
            &mut self.scratch[HEADER..FRAME - wire::TRAILER],
            ack,
            self.bridge,
            self.id.as_str(),
            entries,
        )
        .map_err(|_| Error::TooLarge)?;
        wire::hello_parse(&self.scratch[HEADER..HEADER + len]).map_err(|_| Error::InvalidPorts)?;
        Ok(len)
    }

    fn queue_hello(&mut self, ack: bool) -> Result<(), Error> {
        let len = self.hello_payload(ack)?;
        self.emit(Kind::Hello, 0, 0, len)
    }

    /// Queue the Hello on the first I/O. Declarations are done by then (afterwards they are
    /// `Started`).
    fn ensure_started(&mut self) {
        if !self.started {
            self.started = true;
            let _ = self.queue_hello(false);
        }
    }

    fn tx_consume(&mut self, n: usize) {
        let tx = self.tx.as_mut();
        tx.copy_within(n..self.tx_len, 0);
        self.tx_len -= n;
    }

    fn rx_discard(&mut self, n: usize) {
        let rx = self.rx.as_mut();
        rx.copy_within(n..self.rx_len, 0);
        self.rx_len -= n;
        self.rx_generation = self.rx_generation.wrapping_add(1);
    }

    fn rx_compact(&mut self) {
        if self.rx_consumed > 0 {
            let n = self.rx_consumed;
            self.rx_consumed = 0;
            self.rx_discard(n);
        }
    }

    // -----------------------------------------------------------------------
    // Reading
    // -----------------------------------------------------------------------

    /// Whether the handle still points at the frame sitting in the receive buffer. Everything that
    /// reads through a [`Frame`] goes through here — a stale handle must never be answered with
    /// plausible-looking data.
    fn is_fresh(&self, frame: &Frame) -> bool {
        frame.generation == self.rx_generation
    }

    /// The frame's payload. Empty if the handle is stale.
    #[must_use]
    pub fn payload(&self, frame: &Frame) -> &[u8] {
        if !self.is_fresh(frame) {
            return &[];
        }
        &self.rx.as_ref()[HEADER..HEADER + usize::from(frame.len)]
    }

    #[cfg(feature = "std")]
    pub(crate) fn frame_address(&self, frame: &Frame) -> Option<wire::EndpointRef<'_>> {
        if frame.kind == Kind::Data {
            if self.peer_bridge {
                let local = self.local_source(frame.hash, flags::SUB)?;
                let source = local
                    .port
                    .sources
                    .iter()
                    .find(|s| wire::endpoint_hash(s.namespace, s.port) == frame.hash)?;
                return Some(wire::EndpointRef {
                    namespace: source.namespace,
                    port: source.port,
                });
            }
            let hello = self.hello()?;
            let entry = hello
                .entries()
                .find(|e| e.hash == frame.hash && e.flags & flags::PUB != 0)?;
            return Some(wire::EndpointRef {
                namespace: hello.id,
                port: entry.port,
            });
        }
        if let Some(local) = self.local_type(frame.hash) {
            return Some(wire::EndpointRef {
                namespace: self.id(),
                port: local.port.name,
            });
        }
        self.remote_source(frame.hash, flags::CALLS)?
            .sources
            .iter()
            .find(|source| wire::endpoint_hash(source.namespace, source.port) == frame.hash)
    }

    pub(crate) fn frame_contract(&self, frame: &Frame) -> Option<(&str, u64)> {
        if !self.is_fresh(frame) {
            return None;
        }
        let local = match frame.kind {
            Kind::Data if self.peer_bridge => self.local_source(frame.hash, flags::SUB),
            Kind::Request => self.local_type(frame.hash),
            Kind::Reply => self.local_source(frame.hash, flags::CALLS),
            Kind::Hello | Kind::Data | Kind::Error | Kind::Ping => None,
        };
        if let Some(local) = local {
            return Some((local.name, local.schema));
        }
        let hello = self.hello()?;
        let entry = if frame.kind == Kind::Request {
            self.remote_source(frame.hash, flags::CALLS)?
        } else {
            hello
                .entries()
                .find(|e| e.hash == frame.hash && e.flags & (flags::PUB | flags::SERVE) != 0)?
        };
        Some((entry.name, entry.schema))
    }

    /// Decode against the contract selected by the frame's exact endpoint.
    #[must_use]
    pub fn decode<T: MessageType + Message + Default>(&self, frame: &Frame) -> Option<T> {
        if !matches!(frame.kind, Kind::Data | Kind::Request)
            || self.frame_contract(frame) != Some((T::TYPE_NAME, T::SCHEMA_FINGERPRINT))
        {
            return None;
        }
        T::decode(self.payload(frame)).ok()
    }

    /// Decode the response fingerprint after selecting its named server contract.
    #[must_use]
    pub fn decode_reply<S: Service>(&self, frame: &Frame) -> Option<S::Response> {
        if frame.kind != Kind::Reply
            || self.frame_contract(frame) != Some((S::TYPE_NAME, S::SCHEMA_FINGERPRINT))
        {
            return None;
        }
        let payload = self.payload(frame);
        let schema = u64::from_le_bytes(payload.get(..8)?.try_into().ok()?);
        if schema != S::Response::SCHEMA_FINGERPRINT {
            return None;
        }
        S::Response::decode(payload.get(8..)?).ok()
    }

    /// The message of an [`Event::Error`].
    #[must_use]
    pub fn error_message(&self, frame: &Frame) -> &str {
        core::str::from_utf8(self.payload(frame)).unwrap_or("")
    }

    // -----------------------------------------------------------------------
    // The peer
    // -----------------------------------------------------------------------

    /// Whether the peer's Hello has arrived and something has been received within `timeout_ms`.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// The peer's id (only while connected).
    #[must_use]
    pub fn peer_id(&self) -> Option<&str> {
        self.peer.as_ref().map(IdBuf::as_str)
    }

    /// The types the peer declared.
    pub fn peer_types(&self) -> impl Iterator<Item = &RemoteType> {
        self.remote.iter().flatten()
    }

    /// Whether the peer is a bridge (false when disconnected). A bridge subscribes to and serves
    /// anything.
    #[must_use]
    pub fn peer_is_bridge(&self) -> bool {
        self.connected && self.peer_bridge
    }

    /// Whether the peer subscribes to this declared output port.
    #[must_use]
    pub fn peer_subscribes<T: MessageType>(&self, name: &str) -> bool {
        self.check_port::<T>(name, PortDirection::Output, None)
            .is_ok()
            && self.peer_subscribes_hash(wire::endpoint_hash(self.id(), name))
    }

    #[must_use]
    /// Test subscription to a particular output endpoint hash.
    pub fn peer_subscribes_hash(&self, hash: u32) -> bool {
        self.connected && (self.peer_bridge || self.remote_source(hash, flags::SUB).is_some())
    }

    /// Whether this declared caller's exact destination server is available.
    #[must_use]
    pub fn peer_serves<T: Service>(&self, name: &str) -> bool {
        self.request_hash::<T>(name)
            .is_ok_and(|hash| self.peer_serves_hash(hash))
    }

    #[must_use]
    /// Test service availability at a particular output endpoint hash.
    pub fn peer_serves_hash(&self, hash: u32) -> bool {
        self.connected
            && (self.peer_bridge
                || self
                    .remote_type(hash)
                    .is_some_and(|t| t.flags & flags::SERVE != 0))
    }

    /// How much was thrown away.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.stats
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct Pos {
        #[prost(int32, tag = "1")]
        x: i32,
    }
    impl MessageType for Pos {
        const TYPE_NAME: &'static str = "Pos";
        const SCHEMA_FINGERPRINT: u64 = 0xaa;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }

    /// The same type name as `Pos` with a different fingerprint (a same-named type from another
    /// project).
    #[derive(Clone, PartialEq, prost::Message)]
    struct PosV2 {
        #[prost(int32, tag = "1")]
        x: i32,
    }
    impl MessageType for PosV2 {
        const TYPE_NAME: &'static str = "Pos";
        const SCHEMA_FINGERPRINT: u64 = 0xbb;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Cmd {
        #[prost(uint32, tag = "1")]
        v: u32,
    }
    impl MessageType for Cmd {
        const TYPE_NAME: &'static str = "Cmd";
        const SCHEMA_FINGERPRINT: u64 = 0xcc;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Add {
        #[prost(int32, tag = "1")]
        a: i32,
        #[prost(int32, tag = "2")]
        b: i32,
    }
    impl MessageType for Add {
        const TYPE_NAME: &'static str = "Add";
        const SCHEMA_FINGERPRINT: u64 = 0xdd;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct Sum {
        #[prost(int32, tag = "1")]
        s: i32,
    }
    impl MessageType for Sum {
        const TYPE_NAME: &'static str = "Sum";
        const SCHEMA_FINGERPRINT: u64 = 0xee;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }
    impl Service for Add {
        type Response = Sum;
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct AddV2 {
        #[prost(int32, tag = "1")]
        a: i32,
        #[prost(int32, tag = "2")]
        b: i32,
    }
    impl MessageType for AddV2 {
        const TYPE_NAME: &'static str = Add::TYPE_NAME;
        const SCHEMA_FINGERPRINT: u64 = Add::SCHEMA_FINGERPRINT + 1;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }
    impl Service for AddV2 {
        type Response = Sum;
    }

    const fn fixture<T: MessageType>(
        name: &'static str,
        direction: PortDirection,
        response: Option<PortType>,
    ) -> StaticPort {
        StaticPort {
            name,
            direction,
            ty: T::TYPE_NAME,
            schema: T::SCHEMA_FINGERPRINT,
            response,
            sources: match direction {
                PortDirection::Input => &[reiny_core::StaticEndpointAddress {
                    namespace: "a",
                    port: "position",
                }],
                PortDirection::RpcClient => &[reiny_core::StaticEndpointAddress {
                    namespace: "b",
                    port: "addition",
                }],
                PortDirection::Output | PortDirection::RpcServer => &[],
            },
        }
    }

    const PORTS: &[StaticPort] = &[
        fixture::<Pos>("position", PortDirection::Input, None),
        fixture::<Pos>("position", PortDirection::Output, None),
        fixture::<Pos>("position_copy", PortDirection::Output, None),
        fixture::<PosV2>("position_v2", PortDirection::Input, None),
        fixture::<PosV2>("position_v2", PortDirection::Output, None),
        fixture::<Cmd>("command", PortDirection::Input, None),
        fixture::<Cmd>("command", PortDirection::Output, None),
        StaticPort {
            sources: &[reiny_core::StaticEndpointAddress {
                namespace: "a",
                port: "other",
            }],
            ..fixture::<Other>("other", PortDirection::Input, None)
        },
        fixture::<Other>("other", PortDirection::Output, None),
        fixture::<Add>(
            "addition",
            PortDirection::RpcServer,
            Some(PortType {
                ty: Sum::TYPE_NAME,
                schema: Sum::SCHEMA_FINGERPRINT,
            }),
        ),
        fixture::<Add>(
            "addition",
            PortDirection::RpcClient,
            Some(PortType {
                ty: Sum::TYPE_NAME,
                schema: Sum::SCHEMA_FINGERPRINT,
            }),
        ),
        fixture::<AddV2>(
            "addition_v2",
            PortDirection::RpcClient,
            Some(PortType {
                ty: Sum::TYPE_NAME,
                schema: Sum::SCHEMA_FINGERPRINT,
            }),
        ),
    ];

    type L = Link<[u8; 1024], 8, 1024>;

    fn link(id: &str) -> L {
        let other = match id {
            "b" => "a",
            "host" => "mcu",
            "mcu" => "bridge",
            "caller" => "server",
            "server" => "caller",
            "sender" => "receiver",
            "receiver" => "sender",
            "old" => "new",
            "new" => "old",
            _ => "b",
        };
        let ports = PORTS
            .iter()
            .map(|port| {
                let mut port = *port;
                if matches!(
                    port.direction,
                    PortDirection::Input | PortDirection::RpcClient
                ) {
                    port.sources = Box::leak(Box::new([reiny_core::StaticEndpointAddress {
                        namespace: other,
                        port: if port.name == "addition_v2" {
                            "addition"
                        } else {
                            port.name
                        },
                    }]));
                }
                port
            })
            .collect::<Vec<_>>();
        L::new(
            id,
            Box::leak(ports.into_boxed_slice()),
            [0; 1024],
            [0; 1024],
        )
        .unwrap()
    }

    /// Pour each side's output into the other (stream-like: one byte at a time). True if anything
    /// moved.
    fn pump(a: &mut L, b: &mut L) -> bool {
        fn one_way(from: &mut L, to: &mut L) -> bool {
            let mut buf = [0u8; 2048];
            let n = from.drain(&mut buf);
            for byte in &buf[..n] {
                assert_eq!(to.feed(core::slice::from_ref(byte)), 1);
            }
            n > 0
        }
        let ab = one_way(a, b);
        let ba = one_way(b, a);
        ab || ba
    }

    fn connect(a: &mut L, b: &mut L) {
        a.tick(0);
        b.tick(0);
        while pump(a, b) {}
        let ea = a.next();
        let eb = b.next();
        assert!(matches!(ea, Some(Event::Connected(_))), "{ea:?}");
        assert!(matches!(eb, Some(Event::Connected(_))), "{eb:?}");
        assert!(a.next().is_none() && b.next().is_none());
    }

    /// Run both sides until nothing moves and no events are left. `connect` returns with each side's
    /// ack Hello still queued (it is only produced while handling the peer's Hello), so a test that
    /// looks at what `drain` produces next has to flush that first.
    fn settle(a: &mut L, b: &mut L) {
        while pump(a, b) {
            while a.next().is_some() {}
            while b.next().is_some() {}
        }
    }

    #[test]
    fn handshake_exchanges_ids_and_types() {
        let mut a = link("host");
        let mut b = link("mcu");
        let ports = b
            .ports
            .iter()
            .map(|port| {
                let mut port = *port;
                if port.direction == PortDirection::Input {
                    port.sources = &[reiny_core::StaticEndpointAddress {
                        namespace: "host",
                        port: "command",
                    }];
                }
                port
            })
            .collect::<Vec<_>>();
        b.ports = Box::leak(ports.into_boxed_slice());
        a.subscribes::<Pos>("position").unwrap();
        a.publishes::<Cmd>("command").unwrap();
        a.rpc_client::<Add>("addition").unwrap();
        b.publishes_latched::<Pos>("position").unwrap();
        b.subscribes::<Cmd>("command").unwrap();
        b.rpc_server::<Add>("addition").unwrap();
        connect(&mut a, &mut b);

        assert_eq!(a.peer_id(), Some("mcu"));
        assert_eq!(b.peer_id(), Some("host"));
        assert!(a.peer_serves::<Add>("addition"));
        assert!(!b.peer_serves::<Add>("addition"));
        assert!(a.peer_subscribes::<Cmd>("command"));
        assert!(b.peer_subscribes::<Pos>("position"));
        let pos = a
            .peer_types()
            .find(|t| t.hash == wire::endpoint_hash("mcu", "position"))
            .unwrap();
        assert_eq!(pos.flags, flags::PUB | flags::LATCHED);
        assert_eq!(pos.schema, 0xaa);
        assert!(!pos.mismatch);
        // Declarations cannot be added after the Hello.
        assert_eq!(a.subscribes::<Cmd>("command"), Err(Error::Started));
    }

    /// `rpc_client` is the only reason a request type's *name* ever reaches a bridge, which is what lets
    /// the bridge route the call onward. It must ride in the Hello with the CALLS flag and must not
    /// make us look like a server.
    #[test]
    fn rpc_client_announces_the_request_type_without_serving_it() {
        let mut a = link("a");
        let mut b = link("b");
        a.rpc_client::<Add>("addition").unwrap();
        connect(&mut a, &mut b);
        let add = b
            .peer_types()
            .find(|t| t.hash == wire::endpoint_hash("a", "addition"))
            .unwrap();
        assert_eq!(add.flags, flags::CALLS);
        assert!(!b.peer_serves_hash(wire::endpoint_hash("a", "addition")));
    }

    #[test]
    fn rpc_responses_are_advertised_without_data_permissions() {
        let mut caller = link("caller");
        let mut peer = link("peer");
        caller.rpc_client::<Add>("addition").unwrap();
        connect(&mut caller, &mut peer);
        let hello = peer.hello().unwrap();
        let rpc = hello.entries().next().unwrap();
        assert_eq!(
            rpc.response,
            Some(wire::HelloType {
                name: Sum::TYPE_NAME,
                schema: Sum::SCHEMA_FINGERPRINT
            })
        );
        assert_eq!(hello.entries().count(), 1);
        assert_eq!(rpc.flags, flags::CALLS);
        assert_eq!(
            caller.send("addition", &Sum::default()),
            Err(Error::WrongDirection)
        );
    }

    #[test]
    fn failed_rpc_metadata_registration_leaves_no_partial_permission() {
        let mut tiny = Link::<_, 1>::new("tiny", PORTS, [0u8; 1024], [0u8; 1024]).unwrap();
        tiny.publishes::<Pos>("position").unwrap();
        assert_eq!(tiny.rpc_client::<Add>("addition"), Err(Error::TableFull));
        assert_eq!(
            tiny.request("addition", &Add::default()),
            Err(Error::NotDeclared)
        );
        assert!(
            tiny.local_type(wire::endpoint_hash("tiny", "addition"))
                .is_none()
        );
        assert!(
            tiny.local_type(wire::endpoint_hash("tiny", "position"))
                .is_some()
        );
    }

    #[test]
    fn data_flows_only_to_subscribers() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>("position").unwrap();
        a.publishes::<Cmd>("command").unwrap();
        b.subscribes::<Pos>("position").unwrap();
        connect(&mut a, &mut b);

        assert_eq!(
            a.send("command", &Cmd { v: 1 }),
            Ok(false),
            "peer does not subscribe"
        );
        assert_eq!(a.send("position", &Pos { x: 42 }), Ok(true));
        assert_eq!(b.send("position", &Pos { x: 1 }), Err(Error::NotDeclared));
        while pump(&mut a, &mut b) {}
        let Some(Event::Data(f)) = b.next() else {
            panic!("expected data")
        };
        assert_eq!(b.decode::<Pos>(&f), Some(Pos { x: 42 }));
        assert_eq!(b.decode::<Cmd>(&f), None, "wrong type");
        assert!(b.next().is_none());
        // The handle goes stale on the next next().
        assert_eq!(b.payload(&f), b"");
    }

    /// A stale handle must decode to `None`, not to a default-valued message. An empty payload is a
    /// legal encoding of `T::default()`, so "stale" and "empty" have to be told apart before the
    /// payload is decoded — otherwise a caller holding an old handle silently reads zeros.
    #[test]
    fn stale_handles_decode_to_none() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>("position").unwrap();
        a.publishes::<Cmd>("command").unwrap();
        b.subscribes::<Pos>("position").unwrap();
        b.subscribes::<Cmd>("command").unwrap();
        connect(&mut a, &mut b);

        a.send("position", &Pos { x: 42 }).unwrap();
        a.send("command", &Cmd { v: 1 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Data(first)) = b.next() else {
            panic!("expected data")
        };
        assert_eq!(b.decode::<Pos>(&first), Some(Pos { x: 42 }));
        // Pulling the next frame invalidates the previous handle.
        assert!(matches!(b.next(), Some(Event::Data(_))));
        assert_eq!(b.payload(&first), b"");
        assert_eq!(b.decode::<Pos>(&first), None, "stale handle decoded anyway");
    }

    #[test]
    fn request_reply_roundtrip_and_error() {
        let mut a = link("a");
        let mut b = link("b");
        a.rpc_client::<Add>("addition").unwrap();
        b.rpc_server::<Add>("addition").unwrap();
        b.rpc_client::<Add>("addition").unwrap();
        connect(&mut a, &mut b);

        assert_eq!(
            b.request("addition", &Add { a: 1, b: 2 }),
            Err(Error::NoPeerService)
        );
        let seq = a.request("addition", &Add { a: 1, b: 2 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Request(req)) = b.next() else {
            panic!("expected request")
        };
        let add = b.decode::<Add>(&req).unwrap();
        b.reply::<Add>("addition", req.seq, &Sum { s: add.a + add.b })
            .unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Reply(rep)) = a.next() else {
            panic!("expected reply")
        };
        assert_eq!(rep.seq, seq);
        assert_eq!(a.decode_reply::<Add>(&rep), Some(Sum { s: 3 }));
        assert_eq!(
            a.decode_reply::<AddV2>(&rep),
            None,
            "wrong request contract"
        );
        assert_eq!(
            a.decode::<Sum>(&rep),
            None,
            "reply carries the request hash"
        );

        let seq = a.request("addition", &Add { a: 0, b: 0 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Request(req)) = b.next() else {
            panic!("expected request")
        };
        b.reply_err::<Add>("addition", req.seq, "busy").unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Error(err)) = a.next() else {
            panic!("expected error")
        };
        assert_eq!(err.seq, seq);
        assert_eq!(a.error_message(&err), "busy");
    }

    #[test]
    fn replies_without_the_matching_response_fingerprint_are_rejected() {
        for schema in [None, Some(Sum::SCHEMA_FINGERPRINT ^ 1)] {
            let mut a = link("a");
            let mut b = link("b");
            a.rpc_client::<Add>("addition").unwrap();
            b.rpc_server::<Add>("addition").unwrap();
            connect(&mut a, &mut b);
            a.request("addition", &Add { a: 1, b: 2 }).unwrap();
            while pump(&mut a, &mut b) {}
            let Some(Event::Request(req)) = b.next() else {
                panic!("expected request")
            };
            let payload = Sum { s: 3 }.encode_to_vec();
            if let Some(schema) = schema {
                b.reply_raw(req.seq, req.hash, schema, &payload).unwrap();
            } else {
                // A legacy peer sends only the prost bytes.
                let len = b.copy_payload(&payload).unwrap();
                b.emit(Kind::Reply, req.hash, req.seq, len).unwrap();
            }
            while pump(&mut a, &mut b) {}
            let Some(Event::Reply(reply)) = a.next() else {
                panic!("expected reply")
            };
            assert_eq!(a.decode_reply::<Add>(&reply), None);
        }
    }

    /// The correlation id is a u8 that wraps. Two outstanding requests must not be told apart by
    /// anything else, and the sequence has to advance by one per request.
    #[test]
    fn request_seq_advances_and_wraps() {
        let mut a = link("a");
        let mut b = link("b");
        a.rpc_client::<Add>("addition").unwrap();
        b.rpc_server::<Add>("addition").unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);
        let mut previous: Option<u8> = None;
        for _ in 0..300 {
            let seq = a.request("addition", &Add::default()).unwrap();
            if let Some(p) = previous {
                assert_eq!(seq, p.wrapping_add(1));
            }
            previous = Some(seq);
            while pump(&mut a, &mut b) {}
            let Some(Event::Request(request)) = b.next() else {
                panic!("request");
            };
            b.reply::<Add>("addition", request.seq, &Sum { s: 1 })
                .unwrap();
            while pump(&mut a, &mut b) {}
            let Some(Event::Reply(reply)) = a.next() else {
                panic!("reply");
            };
            assert_eq!(reply.seq, seq);
        }
    }

    #[test]
    fn unserved_request_gets_error_reply() {
        let mut a = link("a");
        let mut b = link("b");
        a.rpc_client::<Add>("addition").unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);
        let len = a.encode_payload(&Add::default()).unwrap();
        let seq = a
            .emit_request(wire::endpoint_hash("b", "addition"), len)
            .unwrap();
        while pump(&mut a, &mut b) {}
        assert!(b.next().is_none());
        while pump(&mut a, &mut b) {}
        let Some(Event::Error(error)) = a.next() else {
            panic!("error");
        };
        assert_eq!(error.seq, seq);
        assert_eq!(a.error_message(&error), "no such service");
        assert_eq!(b.stats().dropped_unwanted, 1);
    }

    #[test]
    fn schema_mismatch_drops_data_once_connected() {
        let ports = Box::leak(Box::new([fixture::<PosV2>(
            "position",
            PortDirection::Output,
            None,
        )]));
        let mut a = L::new("a", ports, [0; 1024], [0; 1024]).unwrap();
        let mut b = link("b");
        a.publishes::<PosV2>("position").unwrap();
        b.subscribes::<Pos>("position").unwrap();
        while pump(&mut a, &mut b) {}
        assert!(a.next().is_none());
        assert!(b.next().is_none());
        assert!(!a.is_connected() && !b.is_connected());
        assert_eq!(b.peer_types().count(), 0);
        let len = a.encode_payload(&PosV2 { x: 1 }).unwrap();
        a.emit_data(wire::endpoint_hash("a", "position"), len)
            .unwrap();
        while pump(&mut a, &mut b) {}
        assert!(b.next().is_none());
        assert_eq!(b.stats().dropped_schema, 1);
    }

    #[test]
    fn typed_send_and_call_cannot_use_another_fingerprint_for_the_same_name() {
        let mut sender = link("sender");
        sender.publishes::<Pos>("position").unwrap();
        sender.rpc_client::<Add>("addition").unwrap();
        let mut receiver = link("receiver");
        receiver.subscribes::<Pos>("position").unwrap();
        receiver.rpc_server::<Add>("addition").unwrap();
        connect(&mut sender, &mut receiver);
        assert_eq!(
            sender.send("position", &PosV2 { x: 1 }),
            Err(Error::SchemaMismatch)
        );
        assert_eq!(
            sender.request("addition", &AddV2 { a: 1, b: 2 }),
            Err(Error::SchemaMismatch)
        );
    }

    #[test]
    fn request_fingerprint_mismatch_is_replied_to_without_dispatching() {
        let mut caller = link("caller");
        caller.rpc_client::<AddV2>("addition_v2").unwrap();
        let mut server = link("server");
        server.rpc_server::<Add>("addition").unwrap();
        while pump(&mut caller, &mut server) {}
        assert!(caller.next().is_none());
        assert!(server.next().is_none());
        assert!(!caller.is_connected() && !server.is_connected());
        assert_eq!(server.peer_types().count(), 0);
        assert_eq!(
            caller.request("addition_v2", &AddV2::default()),
            Err(Error::NoPeerService)
        );
    }

    /// A legacy peer cannot negotiate a connection or deliver frames without a fingerprint.
    #[test]
    fn missing_fingerprint_rejects_hello_and_data() {
        for (bridge, restart) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut legacy = link("old");
            legacy.publishes::<Pos>("position").unwrap();
            let mut receiver = link("new");
            if bridge {
                receiver = receiver.into_bridge();
            } else {
                receiver.subscribes::<Pos>("position").unwrap();
            }
            if restart {
                connect(&mut legacy, &mut receiver);
                settle(&mut legacy, &mut receiver);
            }
            legacy.ensure_started();
            legacy.tx_len = 0;
            let len = wire::hello_write(
                &mut legacy.scratch[HEADER..],
                false,
                false,
                "old",
                [HelloEntry {
                    hash: wire::endpoint_hash("old", "position"),
                    flags: flags::PUB,
                    schema: Pos::SCHEMA_FINGERPRINT,
                    name: Pos::TYPE_NAME,
                    port: "position",
                    response: None,
                    sources: wire::Sources::Static(&[]),
                }]
                .into_iter(),
            )
            .unwrap();
            // [hello flags][id length][old][count][hash u32][entry flags]
            legacy.scratch[HEADER + 12] &= !flags::SCHEMA_FINGERPRINT;
            legacy.emit(Kind::Hello, 0, 0, len).unwrap();
            let len = legacy.encode_payload(&Pos { x: 3 }).unwrap();
            legacy
                .emit_data(wire::endpoint_hash("old", "position"), len)
                .unwrap();
            while pump(&mut legacy, &mut receiver) {}
            if restart {
                assert_eq!(receiver.next(), Some(Event::Disconnected));
            }
            assert!(receiver.next().is_none());
            assert!(!receiver.is_connected());
            assert_eq!(receiver.peer_id(), None);
            assert_eq!(receiver.peer_types().count(), 0);
            assert_eq!(receiver.stats().dropped_schema, u32::from(!bridge));
            assert_eq!(receiver.stats().dropped_unwanted, u32::from(bridge));
        }
    }

    #[test]
    fn ping_keeps_alive_and_silence_disconnects() {
        let mut a = link("a");
        let mut b = link("b");
        connect(&mut a, &mut b);
        // 1.5 s: a sends Pings and b learns it is alive from them.
        for t in (100..=1500).step_by(100) {
            assert!(a.tick(t).is_none());
            assert!(b.tick(t).is_none());
            while pump(&mut a, &mut b) {}
            assert!(a.next().is_none() && b.next().is_none());
        }
        assert!(a.is_connected() && b.is_connected());
        // b goes quiet (a's traffic still reaches b, but b's is thrown away).
        for t in (1600..=6000).step_by(100) {
            let ev = a.tick(t);
            let mut sink = [0u8; 2048];
            let _ = b.drain(&mut sink);
            let mut buf = [0u8; 2048];
            let n = a.drain(&mut buf);
            b.feed(&buf[..n]);
            while b.next().is_some() {}
            if ev == Some(Event::Disconnected) {
                // b's last send was the Ping at t=1000; the first tick past timeout 3000 drops it.
                assert_eq!(t, 4100, "disconnect at {t}");
                break;
            }
            assert!(t < 6000, "never disconnected");
        }
        assert!(!a.is_connected());
        assert_eq!(a.peer_id(), None);
        // While disconnected the Hello is re-sent, and once b answers (through next()) the link is
        // back up.
        a.tick(8000);
        assert_eq!(b.tick(8000), Some(Event::Disconnected));
        loop {
            let moved = pump(&mut a, &mut b);
            while b.next().is_some() {}
            if !moved {
                break;
            }
        }
        assert!(matches!(a.next(), Some(Event::Connected(_))));
        assert!(a.is_connected() && b.is_connected());
    }

    /// The timeouts are knobs, not constants: a link over a slow physical layer sets its own, and
    /// both the Ping interval and the disconnect deadline have to follow them.
    #[test]
    fn config_drives_the_ping_and_timeout_deadlines() {
        let config = LinkConfig {
            ping_interval_ms: 50,
            timeout_ms: 120,
        };
        let mut a = link("a").with_config(config);
        let mut b = link("b").with_config(config);
        connect(&mut a, &mut b);

        // No Ping before the interval elapses, one after.
        let mut sink = [0u8; 512];
        let _ = a.drain(&mut sink);
        assert!(a.tick(40).is_none());
        assert_eq!(a.tx_pending(), 0, "pinged too early");
        assert!(a.tick(60).is_none());
        assert!(a.tx_pending() > 0, "no ping after the interval");

        // Silence past timeout_ms disconnects. The last receive was at t=0, so 100 is still inside
        // the 120 ms window and 130 is past it — with the default 3000 neither would be.
        assert!(a.tick(100).is_none(), "disconnected inside the window");
        assert_eq!(a.tick(130), Some(Event::Disconnected));
    }

    #[test]
    fn peer_restart_reconnects_and_reannounces() {
        let mut a = link("a");
        let mut b = link("b");
        b.publishes_latched::<Pos>("position").unwrap();
        a.subscribes::<Pos>("position").unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);
        let mut b2 = link("b");
        b2.publishes::<Pos>("position").unwrap();
        while pump(&mut a, &mut b2) {}
        assert_eq!(a.next(), Some(Event::Disconnected));
        assert!(!a.is_connected());
        assert_eq!(a.peer_types().count(), 0);
    }

    #[test]
    fn garbage_and_split_frames_resync() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>("position").unwrap();
        b.subscribes::<Pos>("position").unwrap();
        connect(&mut a, &mut b);
        a.send("position", &Pos { x: 7 }).unwrap();
        let mut buf = [0u8; 256];
        let n = a.drain(&mut buf);
        // Garbage in front, a split in the middle, a broken frame behind.
        b.feed(&[0x55, 0xaa, 0x01]);
        b.feed(&buf[..n / 2]);
        assert!(b.next().is_none(), "half a frame is not a frame");
        b.feed(&buf[n / 2..n]);
        b.feed(&[0x03, 0x01, 0x02, 0x00]); // valid COBS but far too short
        let Some(Event::Data(f)) = b.next() else {
            panic!("expected data after resync")
        };
        assert_eq!(b.decode::<Pos>(&f), Some(Pos { x: 7 }));
        assert!(b.next().is_none());
        assert_eq!(b.stats().bad_frames, 2);
    }

    #[test]
    fn oversized_input_is_dropped_and_resyncs() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>("position").unwrap();
        b.subscribes::<Pos>("position").unwrap();
        connect(&mut a, &mut b);
        // 1500 bytes with no delimiter (longer than the 1024-byte receive buffer).
        let junk = [0x11u8; 1500];
        assert_eq!(b.feed(&junk), 1500);
        assert_eq!(b.stats().rx_overflow, 1);
        b.feed(&[0x00]);
        a.send("position", &Pos { x: 9 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Data(f)) = b.next() else {
            panic!("expected data after overflow")
        };
        assert_eq!(b.decode::<Pos>(&f), Some(Pos { x: 9 }));
    }

    /// When the receive buffer is full of *complete* frames, `feed` accepts a short count instead of
    /// dropping bytes on the floor — that short return is the caller's signal to run `next()` and
    /// hand over the rest, so nothing is lost.
    #[test]
    fn feed_reports_backpressure_instead_of_dropping() {
        let mut a = link("a");
        // A deliberately tiny receiver: 64 bytes of rx.
        let mut b = Link::<[u8; 256], 4>::new("b", PORTS, [0; 256], [0; 256]).unwrap();
        a.publishes::<Pos>("position").unwrap();
        b.subscribes::<Pos>("position").unwrap();

        // Hand-rolled handshake: the shared `pump` helper only works between two links of the same
        // type, and this receiver is deliberately a different one.
        a.tick(0);
        b.tick(0);
        let mut buf = [0u8; 2048];
        for _ in 0..3 {
            let n = a.drain(&mut buf);
            b.feed(&buf[..n]);
            while b.next().is_some() {}
            let n = b.drain(&mut buf);
            a.feed(&buf[..n]);
            while a.next().is_some() {}
        }
        assert!(a.is_connected() && b.is_connected());

        // Fill the receive buffer with data frames — without draining the events — until feed goes
        // short.
        let mut short = false;
        for _ in 0..32 {
            assert_eq!(a.send("position", &Pos { x: 1 }), Ok(true));
            let n = a.drain(&mut buf);
            if b.feed(&buf[..n]) < n {
                short = true;
                break;
            }
        }
        assert!(short, "feed never reported backpressure");
        // Draining the events makes room again, and no byte was silently dropped.
        while b.next().is_some() {}
        assert_eq!(b.stats().rx_overflow, 0, "backpressure is not overflow");
    }

    /// The datagram path: one call yields exactly one whole frame, and a frame that does not fit
    /// `out` is dropped rather than wedging the queue behind it.
    #[test]
    fn drain_frame_emits_one_frame_at_a_time() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>("position").unwrap();
        b.subscribes::<Pos>("position").unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);

        a.send("position", &Pos { x: 1 }).unwrap();
        a.send("position", &Pos { x: 2 }).unwrap();
        let mut out = [0u8; 256];
        let first = a.drain_frame(&mut out).expect("first frame");
        assert_eq!(out[first - 1], DELIMITER);
        assert!(!out[..first - 1].contains(&DELIMITER), "exactly one frame");
        let n = b.feed(&out[..first]);
        assert_eq!(n, first);
        let Some(Event::Data(f)) = b.next() else {
            panic!("expected the first datagram")
        };
        assert_eq!(b.decode::<Pos>(&f), Some(Pos { x: 1 }));

        // A too-small `out` drops that frame and counts it, and the next one still comes through.
        a.send("position", &Pos { x: 3 }).unwrap();
        let mut tiny = [0u8; 2];
        assert_eq!(a.drain_frame(&mut tiny), None);
        assert!(a.stats().tx_full >= 1);
        let third = a.drain_frame(&mut out).expect("the queue is not wedged");
        b.feed(&out[..third]);
        let Some(Event::Data(f)) = b.next() else {
            panic!("expected the frame after the dropped one")
        };
        assert_eq!(b.decode::<Pos>(&f), Some(Pos { x: 3 }));
        assert_eq!(a.drain_frame(&mut out), None, "nothing left to send");
    }

    #[test]
    fn tx_full_and_too_large_are_reported() {
        let mut small = Link::<[u8; 128], 3, 128>::new("s", PORTS, [0; 128], [0; 128]).unwrap();
        small.publishes::<Cmd>("command").unwrap();
        small.rpc_server::<Add>("addition").unwrap();
        let mut sink = [0u8; 256];
        let _ = small.drain(&mut sink);
        assert_eq!(small.send("command", &Cmd { v: 1 }), Ok(false));
        let mut full = false;
        for _ in 0..16 {
            match small.reply_err::<Add>("addition", 0, "xxxxxxxxxx") {
                Ok(()) => {}
                Err(Error::Full) => {
                    full = true;
                    break;
                }
                Err(e) => panic!("{e}"),
            }
        }
        assert!(full);
        assert!(small.stats().tx_full >= 1);
        assert_eq!(small.copy_payload(&[0; 128]), Err(Error::TooLarge));
        let mut tiny = Link::<[u8; 64], 1, 32>::new("tiny", PORTS, [0; 64], [0; 64]).unwrap();
        assert_eq!(tiny.publishes::<Pos>("position"), Err(Error::TooLarge));
        assert_eq!(
            tiny.send("position", &Pos::default()),
            Err(Error::NotDeclared)
        );
    }

    #[test]
    fn bridge_mirrors_the_peer_without_declaring_anything() {
        let mut mcu = link("mcu");
        mcu.publishes_latched::<Pos>("position").unwrap();
        mcu.subscribes::<Cmd>("command").unwrap();
        mcu.rpc_server::<Add>("addition").unwrap();
        mcu.rpc_client::<Add>("addition").unwrap();
        let mut bridge = link("bridge").into_bridge();
        connect(&mut mcu, &mut bridge);
        assert!(mcu.peer_is_bridge() && !bridge.peer_is_bridge());
        // The bridge takes / sends / serves anything.
        assert!(mcu.peer_subscribes::<Pos>("position") && mcu.peer_serves::<Add>("addition"));
        assert!(
            bridge.peer_subscribes_hash(wire::endpoint_hash("bridge", "command"))
                && !bridge.peer_subscribes_hash(wire::endpoint_hash("mcu", "position"))
        );

        assert_eq!(mcu.send("position", &Pos { x: 5 }), Ok(true));
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Data(f)) = bridge.next() else {
            panic!("bridge should receive undeclared data")
        };
        assert_eq!(f.hash, wire::endpoint_hash("mcu", "position"));
        assert_eq!(bridge.decode::<Pos>(&f), Some(Pos { x: 5 }));

        let cmd = Cmd { v: 3 }.encode_to_vec();
        assert_eq!(
            bridge.send_raw(wire::endpoint_hash("bridge", "command"), &cmd),
            Ok(true)
        );
        assert_eq!(
            bridge.send_raw(wire::endpoint_hash("mcu", "position"), &cmd),
            Ok(false)
        );
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Data(f)) = mcu.next() else {
            panic!("mcu should receive cmd")
        };
        assert_eq!(mcu.decode::<Cmd>(&f), Some(Cmd { v: 3 }));

        // bridge → mcu service call (raw).
        let req = Add { a: 2, b: 3 }.encode_to_vec();
        let seq = bridge
            .request_raw(wire::endpoint_hash("mcu", "addition"), &req)
            .unwrap();
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Request(r)) = mcu.next() else {
            panic!("mcu should receive request")
        };
        mcu.reply::<Add>("addition", r.seq, &Sum { s: 5 }).unwrap();
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Reply(rep)) = bridge.next() else {
            panic!("bridge should receive reply")
        };
        assert_eq!(rep.seq, seq);
        assert_eq!(bridge.decode_reply::<Add>(&rep), Some(Sum { s: 5 }));

        // mcu → bridge service call: a bridge accepts requests for types it does not serve.
        let seq = mcu.request("addition", &Add { a: 1, b: 1 }).unwrap();
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Request(r)) = bridge.next() else {
            panic!("bridge should receive any request")
        };
        assert_eq!(Add::decode(bridge.payload(&r)), Ok(Add { a: 1, b: 1 }));
        bridge
            .reply_raw(
                r.seq,
                r.hash,
                Sum::SCHEMA_FINGERPRINT,
                &Sum { s: 2 }.encode_to_vec(),
            )
            .unwrap();
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Reply(rep)) = mcu.next() else {
            panic!("mcu should receive reply")
        };
        assert_eq!(rep.seq, seq);
        assert_eq!(mcu.decode_reply::<Add>(&rep), Some(Sum { s: 2 }));
        assert_eq!(bridge.stats().dropped_unwanted, 0);
    }

    /// A bridge holds no type table, so it never compares fingerprints: it carries the peer's across
    /// to the far side untouched. Data that a normal subscriber would drop must reach the bridge.
    #[test]
    fn bridge_does_not_enforce_fingerprints() {
        let mut mcu = link("mcu");
        mcu.publishes::<PosV2>("position_v2").unwrap(); // SCHEMA_FINGERPRINT = 0xbb
        let mut bridge = link("bridge").into_bridge();
        connect(&mut mcu, &mut bridge);

        assert_eq!(mcu.send("position_v2", &PosV2 { x: 8 }), Ok(true));
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Data(f)) = bridge.next() else {
            panic!("a bridge must not drop on fingerprints")
        };
        assert_eq!(f.hash, wire::endpoint_hash("mcu", "position_v2"));
        assert_eq!(bridge.stats().dropped_schema, 0);
        // The peer's fingerprint is still visible, so it can be carried onward.
        assert_eq!(
            bridge
                .peer_types()
                .find(|t| t.hash == wire::endpoint_hash("mcu", "position_v2"))
                .map(|t| t.schema),
            Some(0xbb)
        );
    }

    /// `N` caps the peer's table too. Overflow is counted rather than silently truncating the link
    /// into a state where it thinks the peer never declared the type.
    #[test]
    fn peer_table_overflow_is_counted() {
        let mut small = Link::<[u8; 1024], 1>::new("small", PORTS, [0; 1024], [0; 1024]).unwrap();
        let mut big = link("big");
        big.publishes::<Pos>("position").unwrap();
        big.publishes::<Cmd>("command").unwrap();
        big.rpc_server::<Add>("addition").unwrap();
        small.subscribes::<Pos>("position").unwrap();

        small.tick(0);
        big.tick(0);
        let mut buf = [0u8; 2048];
        loop {
            let n = big.drain(&mut buf);
            if n == 0 {
                break;
            }
            small.feed(&buf[..n]);
            while small.next().is_some() {}
            let n = small.drain(&mut buf);
            if n == 0 {
                break;
            }
            big.feed(&buf[..n]);
            while big.next().is_some() {}
        }
        assert!(!small.is_connected());
        assert_eq!(small.peer_types().count(), 0);
        // The table is rebuilt on every Hello (the peer's first one and its ack), so the two types
        // that did not fit are counted once per Hello rather than once in total.
        assert!(small.stats().remote_overflow >= 1);
    }

    /// The same `TYPE_NAME` (same hash, same name) as `Pos`.
    struct Alias;
    impl MessageType for Alias {
        const TYPE_NAME: &'static str = "Pos";
        const SCHEMA_FINGERPRINT: u64 = Pos::SCHEMA_FINGERPRINT;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }
    struct Other;
    impl MessageType for Other {
        const TYPE_NAME: &'static str = "Other";
        const SCHEMA_FINGERPRINT: u64 = 0xff;
        const DESCRIPTOR: Option<reiny_core::Descriptor> = None;
    }

    #[test]
    fn declaration_errors() {
        let mut l = Link::<[u8; 256], 2>::new("l", PORTS, [0; 256], [0; 256]).unwrap();
        l.publishes::<Pos>("position").unwrap();
        l.subscribes::<Pos>("position").unwrap(); // input and output occupy separate contract slots
        assert_eq!(l.subscribes::<Cmd>("command"), Err(Error::TableFull));
        assert!(L::new(&"x".repeat(ID_MAX + 1), PORTS, [0; 1024], [0; 1024]).is_err());
        let mut l = Link::<[u8; 256], 3>::new("l", PORTS, [0; 256], [0; 256]).unwrap();
        l.publishes::<Pos>("position").unwrap();
        assert_eq!(
            l.subscribes::<Alias>("position"),
            Ok(()),
            "same name, same hash: fine"
        );
        assert_eq!(l.subscribes::<Other>("other"), Ok(()));
        assert_eq!(
            l.subscribes::<PosV2>("position"),
            Err(Error::SchemaMismatch)
        );
    }

    #[test]
    fn static_ports_refuse_undeclared_names_directions_types_and_schemas() {
        let mut empty = L::new("empty", &[], [0; 1024], [0; 1024]).unwrap();
        assert_eq!(empty.publishes::<Pos>("position"), Err(Error::UnknownPort));
        assert_eq!(
            empty.send("position", &Pos { x: 1 }),
            Err(Error::UnknownPort)
        );

        let mut l = link("checked");
        assert_eq!(l.publishes::<Pos>("missing"), Err(Error::UnknownPort));
        assert_eq!(
            l.subscribes::<Pos>("position_copy"),
            Err(Error::WrongDirection)
        );
        assert_eq!(l.publishes::<Cmd>("position"), Err(Error::WrongType));
        assert_eq!(l.publishes::<PosV2>("position"), Err(Error::SchemaMismatch));
        assert_eq!(l.send("position", &Pos { x: 1 }), Err(Error::NotDeclared));
        assert_eq!(
            l.request("addition", &Add { a: 1, b: 2 }),
            Err(Error::NotDeclared)
        );
        assert_eq!(
            l.reply::<Add>("addition", 0, &Sum { s: 3 }),
            Err(Error::NotDeclared)
        );
        assert_eq!(
            l.reply_err::<Add>("addition", 0, "error"),
            Err(Error::NotDeclared)
        );
        l.publishes::<Pos>("position").unwrap();
        assert_eq!(
            l.send("position_copy", &Pos { x: 1 }),
            Err(Error::NotDeclared)
        );
        assert_eq!(l.send("position", &Cmd { v: 1 }), Err(Error::WrongType));
        assert_eq!(
            l.send("position", &PosV2 { x: 1 }),
            Err(Error::SchemaMismatch)
        );
        assert_eq!(
            l.request("position", &Add { a: 1, b: 2 }),
            Err(Error::WrongDirection)
        );
        assert_eq!(
            l.reply::<Add>("position", 0, &Sum { s: 3 }),
            Err(Error::WrongDirection)
        );
        assert_eq!(l.tx_pending(), 0, "refused operations cannot emit frames");
    }

    #[test]
    fn service_response_contract_is_checked_before_declaration() {
        const WRONG_RESPONSE: &[StaticPort] = &[StaticPort {
            name: "addition",
            direction: PortDirection::RpcServer,
            ty: Add::TYPE_NAME,
            schema: Add::SCHEMA_FINGERPRINT,
            sources: &[],
            response: Some(PortType {
                ty: Pos::TYPE_NAME,
                schema: Pos::SCHEMA_FINGERPRINT,
            }),
        }];
        const WRONG_SCHEMA: &[StaticPort] = &[StaticPort {
            sources: &[reiny_core::StaticEndpointAddress {
                namespace: "server",
                port: "addition",
            }],
            name: "addition",
            direction: PortDirection::RpcClient,
            ty: Add::TYPE_NAME,
            schema: Add::SCHEMA_FINGERPRINT,
            response: Some(PortType {
                ty: Sum::TYPE_NAME,
                schema: Sum::SCHEMA_FINGERPRINT + 1,
            }),
        }];
        let mut server = L::new("server", WRONG_RESPONSE, [0; 1024], [0; 1024]).unwrap();
        assert_eq!(server.rpc_server::<Add>("addition"), Err(Error::WrongType));
        let mut client = L::new("client", WRONG_SCHEMA, [0; 1024], [0; 1024]).unwrap();
        assert_eq!(
            client.rpc_client::<Add>("addition"),
            Err(Error::SchemaMismatch)
        );
    }

    #[test]
    fn invalid_static_tables_are_refused_and_feed_closes_declarations() {
        const DUPLICATE: &[StaticPort] = &[
            fixture::<Pos>("position", PortDirection::Output, None),
            fixture::<Pos>("position", PortDirection::Output, None),
        ];
        const MISSING_RESPONSE: &[StaticPort] =
            &[fixture::<Add>("addition", PortDirection::RpcClient, None)];
        assert_eq!(
            L::new("duplicate", DUPLICATE, [0; 1024], [0; 1024]).err(),
            Some(Error::InvalidPorts)
        );
        assert_eq!(
            L::new("missing", MISSING_RESPONSE, [0; 1024], [0; 1024]).err(),
            Some(Error::InvalidPorts)
        );
        let mut l = link("started");
        l.feed(&[]);
        assert_eq!(l.publishes::<Pos>("position"), Err(Error::Started));
    }

    /// Two different names hashing alike is the one failure a 32-bit type hash can produce, and it
    /// has to be caught at declaration time with both names in the message — not left to corrupt
    /// traffic at run time. ("costarring" / "liquid" is a documented FNV-1a 32 collision; the test
    /// asserts that rather than trusting it.)
    #[test]
    fn hash_collision_is_rejected_at_declaration() {
        const COLLISIONS: &[StaticPort] = &[
            fixture::<Pos>("p0edd4c2", PortDirection::Output, None),
            fixture::<Pos>("p0lzv8oe", PortDirection::Output, None),
        ];
        assert_eq!(
            wire::endpoint_hash("l", "p0edd4c2"),
            wire::endpoint_hash("l", "p0lzv8oe")
        );
        let mut l = Link::<[u8; 256], 4>::new("l", COLLISIONS, [0; 256], [0; 256]).unwrap();
        l.publishes::<Pos>("p0edd4c2").unwrap();
        assert_eq!(
            l.publishes::<Pos>("p0lzv8oe"),
            Err(Error::HashCollision {
                existing: "p0edd4c2",
                new: "p0lzv8oe"
            })
        );
    }

    /// The id is bounded by [`ID_MAX`], and exactly `ID_MAX` still has to work — an off-by-one here
    /// would reject a legitimate id or write past the fixed buffer.
    #[test]
    fn id_length_boundary() {
        let exact = "x".repeat(ID_MAX);
        let l = L::new(&exact, PORTS, [0; 1024], [0; 1024]).unwrap();
        assert_eq!(l.id(), exact);
        assert_eq!(
            L::new(&"x".repeat(ID_MAX + 1), PORTS, [0; 1024], [0; 1024])
                .err()
                .unwrap(),
            Error::IdTooLong
        );
    }

    /// Nothing is known about the peer before the handshake, so every "can I send this?" question
    /// answers no — a `send` before connection must not reach the wire.
    #[test]
    fn nothing_is_routable_before_the_handshake() {
        let mut a = link("a");
        a.publishes::<Pos>("position").unwrap();
        a.rpc_server::<Add>("addition").unwrap();
        a.rpc_client::<Add>("addition").unwrap();
        assert!(!a.is_connected());
        assert_eq!(a.peer_id(), None);
        assert!(!a.peer_subscribes::<Pos>("position"));
        assert!(!a.peer_serves::<Add>("addition"));
        assert!(!a.peer_is_bridge());
        assert_eq!(a.send("position", &Pos { x: 1 }), Ok(false));
        assert_eq!(
            a.request("addition", &Add { a: 1, b: 1 }),
            Err(Error::NoPeerService)
        );
    }

    #[test]
    fn sequence_wrap_never_replaces_a_live_request() {
        let mut a = link("a");
        let mut b = link("b");
        a.rpc_client::<Add>("addition").unwrap();
        b.rpc_server::<Add>("addition").unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);
        let first = a.request("addition", &Add::default()).unwrap();
        while pump(&mut a, &mut b) {}
        assert!(matches!(b.next(), Some(Event::Request(_))));
        // Keep the first call alive across more than a full sequence epoch.
        for _ in 0..300 {
            let seq = a.request("addition", &Add::default()).unwrap();
            assert_ne!(seq, first);
            while pump(&mut a, &mut b) {}
            let Some(Event::Request(request)) = b.next() else {
                panic!("request");
            };
            b.reply::<Add>("addition", request.seq, &Sum { s: 2 })
                .unwrap();
            while pump(&mut a, &mut b) {}
            let Some(Event::Reply(reply)) = a.next() else {
                panic!("reply");
            };
            assert_eq!(a.decode_reply::<Add>(&reply), Some(Sum { s: 2 }));
        }
        b.reply::<Add>("addition", first, &Sum { s: 99 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Reply(reply)) = a.next() else {
            panic!("original reply");
        };
        assert_eq!(reply.seq, first);
        assert_eq!(a.decode_reply::<Add>(&reply), Some(Sum { s: 99 }));
    }

    #[test]
    fn pending_table_exhaustion_does_not_emit_or_replace_a_call() {
        let mut a = link("a");
        let mut b = link("b");
        a.rpc_client::<Add>("addition").unwrap();
        b.rpc_server::<Add>("addition").unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);
        for _ in 0..8 {
            a.request("addition", &Add::default()).unwrap();
        }
        let pending_bytes = a.tx_pending();
        assert_eq!(
            a.request("addition", &Add::default()),
            Err(Error::TableFull)
        );
        assert_eq!(a.tx_pending(), pending_bytes);
        assert_eq!(a.pending.iter().flatten().count(), 8);
    }

    #[test]
    fn same_type_on_an_undeclared_source_port_cannot_deliver() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>("position").unwrap();
        a.publishes::<Pos>("position_copy").unwrap();
        b.subscribes::<Pos>("position").unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);
        assert_eq!(a.send("position_copy", &Pos { x: 7 }), Ok(false));
        let len = a.encode_payload(&Pos { x: 7 }).unwrap();
        a.emit_data(wire::endpoint_hash("a", "position_copy"), len)
            .unwrap();
        while pump(&mut a, &mut b) {}
        assert!(b.next().is_none());
        assert_eq!(b.stats().dropped_unwanted, 1);
    }

    #[test]
    fn mismatched_response_metadata_rejects_the_entire_peer_contract() {
        let mut caller = link("a");
        caller.rpc_client::<Add>("addition").unwrap();
        let mut peer = link("b");
        peer.ensure_started();
        peer.tx_len = 0;
        let len = wire::hello_write(
            &mut peer.scratch[HEADER..],
            false,
            false,
            "b",
            [HelloEntry {
                hash: wire::endpoint_hash("b", "addition"),
                flags: flags::SERVE,
                name: Add::TYPE_NAME,
                schema: Add::SCHEMA_FINGERPRINT,
                port: "addition",
                response: Some(wire::HelloType {
                    name: Sum::TYPE_NAME,
                    schema: Sum::SCHEMA_FINGERPRINT ^ 1,
                }),
                sources: wire::Sources::Static(&[]),
            }]
            .into_iter(),
        )
        .unwrap();
        peer.emit(Kind::Hello, 0, 0, len).unwrap();
        while pump(&mut caller, &mut peer) {}
        assert!(caller.next().is_none());
        assert!(!caller.is_connected());
        assert_eq!(caller.peer_types().count(), 0);
    }
}
