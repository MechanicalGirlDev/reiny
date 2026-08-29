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
//! The schema fingerprint (`Topic::SCHEMA`) is checked exactly once, at Hello, and Data of a
//! mismatching type is dropped (the same "only drop when we can actually compare" rule reiny
//! proper uses).

use core::fmt;

use prost::Message;
use reiny_core::{Service, Topic};

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
    schema: Option<u64>,
    flags: u8,
}

/// One type's worth of what the peer declared in its Hello.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteType {
    /// See [`wire::type_hash`].
    pub hash: u32,
    /// The OR of [`wire::flags`].
    pub flags: u8,
    /// The peer's `Topic::SCHEMA`.
    pub schema: Option<u64>,
    /// Both this and our own `SCHEMA` are `Some` and they differ. Data of this type is dropped.
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
    /// The type hash.
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
    /// Two type names hashed to the same value. Change one of their `TYPE`s.
    HashCollision {
        /// The type name that was registered first.
        existing: &'static str,
        /// The colliding type name.
        new: &'static str,
    },
    /// A type was declared after I/O had started. Declarations must precede the Hello, or the peer
    /// never hears about them.
    Started,
    /// `send` of a type that was never passed to `publishes`.
    NotDeclared,
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
                write!(f, "type hash collision between '{existing}' and '{new}'")
            }
            Self::Started => f.write_str("types must be declared before the link starts"),
            Self::NotDeclared => f.write_str("type not declared with publishes()"),
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
/// - `FRAME`: the cap on a raw frame (header + payload + CRC). The payload goes up to `FRAME - 8`.
///
/// Make the send buffer at least [`wire::encoded_max`]`(FRAME)`, and the receive buffer at least
/// that much again, sized to the peer's `FRAME`.
#[allow(clippy::struct_excessive_bools)] // started / connected / bridge / peer_bridge are orthogonal
pub struct Link<B, const N: usize, const FRAME: usize = 256> {
    id: IdBuf,
    peer: Option<IdBuf>,
    local: [Option<LocalType>; N],
    remote: [Option<RemoteType>; N],
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
    pub fn new(id: &str, tx: B, rx: B) -> Result<Self, Error> {
        Ok(Self {
            id: IdBuf::new(id)?,
            peer: None,
            local: [None; N],
            remote: [None; N],
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
    pub fn as_bridge(mut self) -> Self {
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
    pub fn publishes<T: Topic>(&mut self) -> Result<(), Error> {
        self.declare::<T>(flags::PUB)
    }

    /// Publish the type `T` latched — a promise to re-send the most recent value on every
    /// `Connected`. The re-sending itself is the caller's job (`Link` holds no values).
    pub fn publishes_latched<T: Topic>(&mut self) -> Result<(), Error> {
        self.declare::<T>(flags::PUB | flags::LATCHED)
    }

    /// Subscribe to the type `T`.
    pub fn subscribes<T: Topic>(&mut self) -> Result<(), Error> {
        self.declare::<T>(flags::SUB)
    }

    /// Serve the request type `S`.
    pub fn serves<S: Service>(&mut self) -> Result<(), Error> {
        self.declare::<S>(flags::SERVE)
    }

    /// Declare that we call the request type `S`. [`Link::request`] needs no declaration, but when
    /// the peer is a bridge (to zenoh) this is what tells it the type's name, and only then can the
    /// call reach the service on the far side.
    pub fn calls<S: Service>(&mut self) -> Result<(), Error> {
        self.declare::<S>(flags::CALLS)
    }

    fn declare<T: Topic>(&mut self, flag: u8) -> Result<(), Error> {
        if self.started {
            return Err(Error::Started);
        }
        let hash = wire::type_hash(T::TYPE);
        for slot in self.local.iter_mut().flatten() {
            if slot.hash == hash {
                if slot.name != T::TYPE {
                    return Err(Error::HashCollision {
                        existing: slot.name,
                        new: T::TYPE,
                    });
                }
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
            name: T::TYPE,
            schema: T::SCHEMA,
            flags: flag,
        });
        Ok(())
    }

    fn local_type(&self, hash: u32) -> Option<&LocalType> {
        self.local.iter().flatten().find(|t| t.hash == hash)
    }

    fn remote_type(&self, hash: u32) -> Option<&RemoteType> {
        self.remote.iter().flatten().find(|t| t.hash == hash)
    }

    // -----------------------------------------------------------------------
    // I/O
    // -----------------------------------------------------------------------

    /// Hand over the bytes received. Any split is fine. Returns how much was accepted — which is
    /// short only when the receive buffer holds a frame nobody has taken yet, so run [`Link::next`]
    /// and hand over the rest afterwards.
    pub fn feed(&mut self, bytes: &[u8]) -> usize {
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
                // A bridge takes everything the peer publishes (it has no type table, so it checks
                // no fingerprints either).
                let wanted = self.bridge
                    || self
                        .local_type(frame.hash)
                        .is_some_and(|t| t.flags & flags::SUB != 0);
                if !wanted {
                    self.stats.dropped_unwanted = self.stats.dropped_unwanted.wrapping_add(1);
                    return None;
                }
                if self.remote_type(frame.hash).is_some_and(|t| t.mismatch) {
                    self.stats.dropped_schema = self.stats.dropped_schema.wrapping_add(1);
                    return None;
                }
                Some(Event::Data(frame))
            }
            Kind::Request => {
                let served = self.bridge
                    || self
                        .local_type(frame.hash)
                        .is_some_and(|t| t.flags & flags::SERVE != 0);
                if served {
                    Some(Event::Request(frame))
                } else {
                    self.stats.dropped_unwanted = self.stats.dropped_unwanted.wrapping_add(1);
                    let _ = self.emit_error(frame.hash, frame.seq, "no such service");
                    None
                }
            }
            Kind::Reply => Some(Event::Reply(frame)),
            Kind::Error => Some(Event::Error(frame)),
        }
    }

    /// The peer's Hello: rebuild the peer's type table and, unless this was an ack, answer with one.
    fn on_hello(&mut self, frame: Frame) -> Option<Event> {
        let (peer, remote, overflow, ack, bridge) = {
            let payload = &self.rx.as_ref()[HEADER..HEADER + usize::from(frame.len)];
            let hello = wire::hello_parse(payload).ok()?;
            let peer = IdBuf::new(hello.id).ok()?;
            let mut remote = [None; N];
            let mut count = 0usize;
            let mut overflow = 0u32;
            for e in hello.entries() {
                let mismatch = match (self.local_type(e.hash).and_then(|t| t.schema), e.schema) {
                    (Some(mine), Some(theirs)) => mine != theirs,
                    _ => false,
                };
                if count < N {
                    remote[count] = Some(RemoteType {
                        hash: e.hash,
                        flags: e.flags,
                        schema: e.schema,
                        mismatch,
                    });
                    count += 1;
                } else {
                    overflow += 1;
                }
            }
            (peer, remote, overflow, hello.ack, hello.bridge)
        };
        self.peer = Some(peer);
        self.peer_bridge = bridge;
        self.remote = remote;
        self.stats.remote_overflow = self.stats.remote_overflow.wrapping_add(overflow);
        let was_connected = self.connected;
        self.connected = true;
        if !ack {
            let _ = self.queue_hello(true);
        }
        // A Hello without ack means the peer (re)started, so it is always reported. An ack is the
        // completion of the handshake: stay quiet if we were already connected, so that two sides
        // starting simultaneously do not fire Connected twice.
        (!ack || !was_connected).then_some(Event::Connected(frame))
    }

    // -----------------------------------------------------------------------
    // Sending
    // -----------------------------------------------------------------------

    /// Send the type `T`. If the peer does not subscribe to `T` nothing reaches the wire and this
    /// returns `Ok(false)`.
    pub fn send<T: Topic + Message>(&mut self, msg: &T) -> Result<bool, Error> {
        let hash = wire::type_hash(T::TYPE);
        self.check_publishes(hash)?;
        if !self.peer_subscribes_hash(hash) {
            return Ok(false);
        }
        let len = self.encode_payload(msg)?;
        self.emit_data(hash, len)
    }

    /// The pre-encoded form of [`Link::send`]: send by type hash and prost bytes (the door through
    /// which a bridge passes on what it took off zenoh). Unless we are a bridge, the type still has
    /// to have been passed to `publishes`.
    pub fn send_raw(&mut self, hash: u32, payload: &[u8]) -> Result<bool, Error> {
        self.check_publishes(hash)?;
        if !self.peer_subscribes_hash(hash) {
            return Ok(false);
        }
        let len = self.copy_payload(payload)?;
        self.emit_data(hash, len)
    }

    /// Send the request type `S` and return the correlation id (`seq`). The response comes back as
    /// [`Event::Reply`] / [`Event::Error`].
    pub fn request<S: Service>(&mut self, req: &S) -> Result<u8, Error> {
        let hash = wire::type_hash(S::TYPE);
        if !self.peer_serves_hash(hash) {
            return Err(Error::NoPeerService);
        }
        let len = self.encode_payload(req)?;
        self.emit_request(hash, len)
    }

    /// The pre-encoded form of [`Link::request`].
    pub fn request_raw(&mut self, hash: u32, payload: &[u8]) -> Result<u8, Error> {
        if !self.peer_serves_hash(hash) {
            return Err(Error::NoPeerService);
        }
        let len = self.copy_payload(payload)?;
        self.emit_request(hash, len)
    }

    /// Answer an [`Event::Request`]. `seq` is the request frame's.
    pub fn reply<S: Service>(&mut self, seq: u8, resp: &S::Response) -> Result<(), Error> {
        let len = self.encode_payload(resp)?;
        self.emit(Kind::Reply, wire::type_hash(S::TYPE), seq, len)
    }

    /// The pre-encoded form of [`Link::reply`]. `hash` is the *request* type's.
    pub fn reply_raw(&mut self, seq: u8, hash: u32, payload: &[u8]) -> Result<(), Error> {
        let len = self.copy_payload(payload)?;
        self.emit(Kind::Reply, hash, seq, len)
    }

    /// Answer an [`Event::Request`] with an error.
    pub fn reply_err<S: Service>(&mut self, seq: u8, message: &str) -> Result<(), Error> {
        self.emit_error(wire::type_hash(S::TYPE), seq, message)
    }

    /// The type-hash form of [`Link::reply_err`].
    pub fn reply_err_raw(&mut self, seq: u8, hash: u32, message: &str) -> Result<(), Error> {
        self.emit_error(hash, seq, message)
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
        let seq = self.req_seq;
        self.req_seq = seq.wrapping_add(1);
        self.emit(Kind::Request, hash, seq, len)?;
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

    fn queue_hello(&mut self, ack: bool) -> Result<(), Error> {
        let entries = self.local.iter().flatten().map(|t| HelloEntry {
            hash: t.hash,
            flags: t.flags,
            schema: t.schema,
            name: t.name,
        });
        let len = wire::hello_write(
            &mut self.scratch[HEADER..],
            ack,
            self.bridge,
            self.id.as_str(),
            entries,
        )
        .map_err(|_| Error::TooLarge)?;
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

    /// Decode a Data / Request frame as the type `T`. `None` if the type hash differs, if it does
    /// not decode, or if the handle is stale.
    #[must_use]
    pub fn decode<T: Topic + Message + Default>(&self, frame: &Frame) -> Option<T> {
        // Checked before the payload is read: an empty payload is a perfectly valid encoding of a
        // default message, so without this a stale handle would decode to a plausible `T::default()`
        // instead of failing.
        if !self.is_fresh(frame) || frame.hash != wire::type_hash(T::TYPE) {
            return None;
        }
        T::decode(self.payload(frame)).ok()
    }

    /// Decode an [`Event::Reply`] frame as the response to the request type `S`. A Reply carries the
    /// *request* type's hash, so that is what is checked.
    #[must_use]
    pub fn decode_reply<S: Service>(&self, frame: &Frame) -> Option<S::Response> {
        if !self.is_fresh(frame) || frame.hash != wire::type_hash(S::TYPE) {
            return None;
        }
        S::Response::decode(self.payload(frame)).ok()
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

    /// Whether the peer subscribes to the type `T` (false when disconnected, true for a bridge).
    #[must_use]
    pub fn peer_subscribes<T: Topic>(&self) -> bool {
        self.peer_subscribes_hash(wire::type_hash(T::TYPE))
    }

    /// The type-hash form of [`Link::peer_subscribes`].
    #[must_use]
    pub fn peer_subscribes_hash(&self, hash: u32) -> bool {
        self.connected
            && (self.peer_bridge
                || self
                    .remote_type(hash)
                    .is_some_and(|t| t.flags & flags::SUB != 0))
    }

    /// Whether the peer serves the request type `T` (false when disconnected, true for a bridge).
    #[must_use]
    pub fn peer_serves<T: Topic>(&self) -> bool {
        self.peer_serves_hash(wire::type_hash(T::TYPE))
    }

    /// The type-hash form of [`Link::peer_serves`].
    #[must_use]
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
    impl Topic for Pos {
        const TYPE: &'static str = "Pos";
        const SCHEMA: Option<u64> = Some(0xaa);
    }

    /// The same type name as `Pos` with a different fingerprint (a same-named type from another
    /// project).
    #[derive(Clone, PartialEq, prost::Message)]
    struct PosV2 {
        #[prost(int32, tag = "1")]
        x: i32,
    }
    impl Topic for PosV2 {
        const TYPE: &'static str = "Pos";
        const SCHEMA: Option<u64> = Some(0xbb);
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Cmd {
        #[prost(uint32, tag = "1")]
        v: u32,
    }
    impl Topic for Cmd {
        const TYPE: &'static str = "Cmd";
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Add {
        #[prost(int32, tag = "1")]
        a: i32,
        #[prost(int32, tag = "2")]
        b: i32,
    }
    impl Topic for Add {
        const TYPE: &'static str = "Add";
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct Sum {
        #[prost(int32, tag = "1")]
        s: i32,
    }
    impl Topic for Sum {
        const TYPE: &'static str = "Sum";
    }
    impl Service for Add {
        type Response = Sum;
    }

    type L = Link<[u8; 1024], 8>;

    fn link(id: &str) -> L {
        L::new(id, [0; 1024], [0; 1024]).unwrap()
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
        a.subscribes::<Pos>().unwrap();
        a.publishes::<Cmd>().unwrap();
        b.publishes_latched::<Pos>().unwrap();
        b.subscribes::<Cmd>().unwrap();
        b.serves::<Add>().unwrap();
        connect(&mut a, &mut b);

        assert_eq!(a.peer_id(), Some("mcu"));
        assert_eq!(b.peer_id(), Some("host"));
        assert!(a.peer_serves::<Add>());
        assert!(!b.peer_serves::<Add>());
        assert!(a.peer_subscribes::<Cmd>());
        assert!(b.peer_subscribes::<Pos>());
        let pos = a
            .peer_types()
            .find(|t| t.hash == wire::type_hash("Pos"))
            .unwrap();
        assert_eq!(pos.flags, flags::PUB | flags::LATCHED);
        assert_eq!(pos.schema, Some(0xaa));
        assert!(!pos.mismatch);
        // Declarations cannot be added after the Hello.
        assert_eq!(a.subscribes::<Cmd>(), Err(Error::Started));
    }

    /// `calls` is the only reason a request type's *name* ever reaches a bridge, which is what lets
    /// the bridge route the call onward. It must ride in the Hello with the CALLS flag and must not
    /// make us look like a server.
    #[test]
    fn calls_announces_the_request_type_without_serving_it() {
        let mut a = link("a");
        let mut b = link("b");
        a.calls::<Add>().unwrap();
        connect(&mut a, &mut b);

        let add = b
            .peer_types()
            .find(|t| t.hash == wire::type_hash("Add"))
            .expect("Add was announced");
        assert_eq!(add.flags, flags::CALLS);
        assert!(!b.peer_serves::<Add>(), "calls() is not serves()");
    }

    #[test]
    fn data_flows_only_to_subscribers() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>().unwrap();
        a.publishes::<Cmd>().unwrap();
        b.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);

        assert_eq!(a.send(&Cmd { v: 1 }), Ok(false), "peer does not subscribe");
        assert_eq!(a.send(&Pos { x: 42 }), Ok(true));
        assert_eq!(b.send(&Pos { x: 1 }), Err(Error::NotDeclared));
        while pump(&mut a, &mut b) {}
        let Some(Event::Data(f)) = b.next() else {
            panic!("expected data")
        };
        assert_eq!(b.decode::<Pos>(&f), Some(Pos { x: 42 }));
        assert_eq!(b.decode::<Cmd>(&f), None, "wrong type");
        assert!(b.next().is_none());
        // The handle goes stale on the next next().
        assert!(b.payload(&f).is_empty());
    }

    /// A stale handle must decode to `None`, not to a default-valued message. An empty payload is a
    /// legal encoding of `T::default()`, so "stale" and "empty" have to be told apart before the
    /// payload is decoded — otherwise a caller holding an old handle silently reads zeros.
    #[test]
    fn stale_handles_decode_to_none() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>().unwrap();
        a.publishes::<Cmd>().unwrap();
        b.subscribes::<Pos>().unwrap();
        b.subscribes::<Cmd>().unwrap();
        connect(&mut a, &mut b);

        a.send(&Pos { x: 42 }).unwrap();
        a.send(&Cmd { v: 1 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Data(first)) = b.next() else {
            panic!("expected data")
        };
        assert_eq!(b.decode::<Pos>(&first), Some(Pos { x: 42 }));
        // Pulling the next frame invalidates the previous handle.
        assert!(matches!(b.next(), Some(Event::Data(_))));
        assert!(b.payload(&first).is_empty());
        assert_eq!(b.decode::<Pos>(&first), None, "stale handle decoded anyway");
    }

    #[test]
    fn request_reply_roundtrip_and_error() {
        let mut a = link("a");
        let mut b = link("b");
        b.serves::<Add>().unwrap();
        connect(&mut a, &mut b);

        assert_eq!(b.request(&Add { a: 1, b: 2 }), Err(Error::NoPeerService));
        let seq = a.request(&Add { a: 1, b: 2 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Request(req)) = b.next() else {
            panic!("expected request")
        };
        let add = b.decode::<Add>(&req).unwrap();
        b.reply::<Add>(req.seq, &Sum { s: add.a + add.b }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Reply(rep)) = a.next() else {
            panic!("expected reply")
        };
        assert_eq!(rep.seq, seq);
        assert_eq!(a.decode_reply::<Add>(&rep), Some(Sum { s: 3 }));
        assert_eq!(
            a.decode::<Sum>(&rep),
            None,
            "reply carries the request hash"
        );

        let seq = a.request(&Add { a: 0, b: 0 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Request(req)) = b.next() else {
            panic!("expected request")
        };
        b.reply_err::<Add>(req.seq, "busy").unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Error(err)) = a.next() else {
            panic!("expected error")
        };
        assert_eq!(err.seq, seq);
        assert_eq!(a.error_message(&err), "busy");
    }

    /// The correlation id is a u8 that wraps. Two outstanding requests must not be told apart by
    /// anything else, and the sequence has to advance by one per request.
    #[test]
    fn request_seq_advances_and_wraps() {
        let mut a = link("a");
        let mut b = link("b");
        b.serves::<Add>().unwrap();
        connect(&mut a, &mut b);

        let mut sink = [0u8; 4096];
        let mut previous = None;
        for i in 0..300u32 {
            let seq = a.request(&Add { a: 1, b: 1 }).unwrap();
            if let Some(p) = previous {
                assert_eq!(seq, u8::wrapping_add(p, 1), "at request {i}");
            }
            previous = Some(seq);
            let _ = a.drain(&mut sink); // keep the send buffer from filling
        }
    }

    #[test]
    fn unserved_request_gets_error_reply() {
        let mut a = link("a");
        let mut b = link("b");
        // b does not serve Add. Rather than fake a's table, send the frame directly: this recreates
        // the situation where the peer's table is stale (right after a restart).
        b.serves::<Add>().unwrap();
        connect(&mut a, &mut b);
        let seq = a.request(&Add { a: 1, b: 1 }).unwrap();
        // Swap b for an instance that does not serve Add.
        let mut b2 = link("b");
        b2.subscribes::<Cmd>().unwrap();
        b2.tick(0);
        // b2 needs next() to run before it queues the error response.
        loop {
            let moved = pump(&mut a, &mut b2);
            while b2.next().is_some() {}
            if !moved {
                break;
            }
        }
        // a receives b2's Hello (Connected) plus the error response to the request.
        let mut got_error = false;
        while let Some(ev) = a.next() {
            if let Event::Error(f) = ev {
                assert_eq!(f.seq, seq);
                assert_eq!(a.error_message(&f), "no such service");
                got_error = true;
            }
        }
        assert!(got_error);
        assert_eq!(b2.stats().dropped_unwanted, 1);
    }

    #[test]
    fn schema_mismatch_drops_data_once_connected() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<PosV2>().unwrap(); // same name, different fingerprint
        b.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);
        assert!(b.peer_types().any(|t| t.mismatch));
        assert_eq!(a.send(&PosV2 { x: 1 }), Ok(true));
        while pump(&mut a, &mut b) {}
        assert!(b.next().is_none());
        assert_eq!(b.stats().dropped_schema, 1);
    }

    /// A fingerprint is only enforced when both sides have one. A hand-written `impl Topic` (no
    /// `SCHEMA`) has to keep interoperating with a generated type — that is the promise `SCHEMA`'s
    /// default exists for.
    #[test]
    fn missing_fingerprint_passes_through() {
        /// `Pos`'s name and shape without a fingerprint, as a hand-written impl would have it.
        #[derive(Clone, PartialEq, prost::Message)]
        struct PosNoSchema {
            #[prost(int32, tag = "1")]
            x: i32,
        }
        impl Topic for PosNoSchema {
            const TYPE: &'static str = "Pos";
        }

        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<PosNoSchema>().unwrap();
        b.subscribes::<Pos>().unwrap(); // SCHEMA = Some(0xaa)
        connect(&mut a, &mut b);
        assert!(b.peer_types().all(|t| !t.mismatch));
        a.send(&PosNoSchema { x: 3 }).unwrap();
        while pump(&mut a, &mut b) {}
        let Some(Event::Data(f)) = b.next() else {
            panic!("expected data")
        };
        assert_eq!(b.decode::<Pos>(&f), Some(Pos { x: 3 }));
        assert_eq!(b.stats().dropped_schema, 0);
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
        b.publishes_latched::<Pos>().unwrap();
        a.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);
        // b restarts: the new Link sends a Hello with no ack → a sees Connected again.
        let mut b2 = link("b");
        b2.publishes::<Pos>().unwrap();
        b2.tick(0);
        while pump(&mut a, &mut b2) {}
        let Some(Event::Connected(f)) = a.next() else {
            panic!("expected reconnect")
        };
        let hello = wire::hello_parse(a.payload(&f)).unwrap();
        assert_eq!(hello.id, "b");
        let entry = hello.entries().next().unwrap();
        assert_eq!(entry.name, "Pos");
        assert_eq!(entry.flags, flags::PUB);
        assert!(matches!(b2.next(), Some(Event::Connected(_))));
    }

    #[test]
    fn garbage_and_split_frames_resync() {
        let mut a = link("a");
        let mut b = link("b");
        a.publishes::<Pos>().unwrap();
        b.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);
        a.send(&Pos { x: 7 }).unwrap();
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
        a.publishes::<Pos>().unwrap();
        b.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);
        // 1500 bytes with no delimiter (longer than the 1024-byte receive buffer).
        let junk = [0x11u8; 1500];
        assert_eq!(b.feed(&junk), 1500);
        assert_eq!(b.stats().rx_overflow, 1);
        b.feed(&[0x00]);
        a.send(&Pos { x: 9 }).unwrap();
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
        let mut b = Link::<[u8; 64], 4>::new("b", [0; 64], [0; 64]).unwrap();
        a.publishes::<Pos>().unwrap();
        b.subscribes::<Pos>().unwrap();

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
            assert_eq!(a.send(&Pos { x: 1 }), Ok(true));
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
        a.publishes::<Pos>().unwrap();
        b.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);
        settle(&mut a, &mut b);

        a.send(&Pos { x: 1 }).unwrap();
        a.send(&Pos { x: 2 }).unwrap();
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
        a.send(&Pos { x: 3 }).unwrap();
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
        let mut small = Link::<[u8; 64], 2, 32>::new("s", [0; 64], [0; 64]).unwrap();
        small.publishes::<Cmd>().unwrap();
        let mut sink = [0u8; 64];
        let _ = small.drain(&mut sink); // flush the Hello
        // With no peer, send is Ok(false). The emit path is exercised through request instead.
        assert_eq!(small.send(&Cmd { v: 1 }), Ok(false));
        assert_eq!(small.reply_err::<Add>(0, "x"), Ok(()));
        // Packing ~10-byte frames into a 64-byte tx fills it.
        let mut full = false;
        for _ in 0..16 {
            match small.reply_err::<Add>(0, "xxxxxxxxxx") {
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
        // A 40-byte payload does not fit FRAME = 32.
        let long = "y".repeat(40);
        assert_eq!(small.reply_err::<Add>(0, &long), Err(Error::Full));
    }

    #[test]
    fn bridge_mirrors_the_peer_without_declaring_anything() {
        let mut mcu = link("mcu");
        mcu.publishes_latched::<Pos>().unwrap();
        mcu.subscribes::<Cmd>().unwrap();
        mcu.serves::<Add>().unwrap();
        let mut bridge = link("bridge").as_bridge();
        connect(&mut mcu, &mut bridge);
        assert!(mcu.peer_is_bridge() && !bridge.peer_is_bridge());
        // The bridge takes / sends / serves anything.
        assert!(mcu.peer_subscribes::<Pos>() && mcu.peer_serves::<Add>());
        assert!(bridge.peer_subscribes::<Cmd>() && !bridge.peer_subscribes::<Pos>());

        assert_eq!(mcu.send(&Pos { x: 5 }), Ok(true));
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Data(f)) = bridge.next() else {
            panic!("bridge should receive undeclared data")
        };
        assert_eq!(f.hash, wire::type_hash("Pos"));
        assert_eq!(bridge.decode::<Pos>(&f), Some(Pos { x: 5 }));

        let cmd = Cmd { v: 3 }.encode_to_vec();
        assert_eq!(bridge.send_raw(wire::type_hash("Cmd"), &cmd), Ok(true));
        assert_eq!(bridge.send_raw(wire::type_hash("Pos"), &cmd), Ok(false));
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Data(f)) = mcu.next() else {
            panic!("mcu should receive cmd")
        };
        assert_eq!(mcu.decode::<Cmd>(&f), Some(Cmd { v: 3 }));

        // bridge → mcu service call (raw).
        let req = Add { a: 2, b: 3 }.encode_to_vec();
        let seq = bridge.request_raw(wire::type_hash("Add"), &req).unwrap();
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Request(r)) = mcu.next() else {
            panic!("mcu should receive request")
        };
        mcu.reply::<Add>(r.seq, &Sum { s: 5 }).unwrap();
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Reply(rep)) = bridge.next() else {
            panic!("bridge should receive reply")
        };
        assert_eq!(rep.seq, seq);
        assert_eq!(Sum::decode(bridge.payload(&rep)), Ok(Sum { s: 5 }));

        // mcu → bridge service call: a bridge accepts requests for types it does not serve.
        let seq = mcu.request(&Add { a: 1, b: 1 }).unwrap();
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Request(r)) = bridge.next() else {
            panic!("bridge should receive any request")
        };
        assert_eq!(Add::decode(bridge.payload(&r)), Ok(Add { a: 1, b: 1 }));
        bridge
            .reply_raw(r.seq, r.hash, &Sum { s: 2 }.encode_to_vec())
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
        mcu.publishes::<PosV2>().unwrap(); // SCHEMA = Some(0xbb)
        let mut bridge = link("bridge").as_bridge();
        connect(&mut mcu, &mut bridge);

        assert_eq!(mcu.send(&PosV2 { x: 8 }), Ok(true));
        while pump(&mut mcu, &mut bridge) {}
        let Some(Event::Data(f)) = bridge.next() else {
            panic!("a bridge must not drop on fingerprints")
        };
        assert_eq!(f.hash, wire::type_hash("Pos"));
        assert_eq!(bridge.stats().dropped_schema, 0);
        // The peer's fingerprint is still visible, so it can be carried onward.
        assert_eq!(
            bridge
                .peer_types()
                .find(|t| t.hash == wire::type_hash("Pos"))
                .and_then(|t| t.schema),
            Some(0xbb)
        );
    }

    /// `N` caps the peer's table too. Overflow is counted rather than silently truncating the link
    /// into a state where it thinks the peer never declared the type.
    #[test]
    fn peer_table_overflow_is_counted() {
        let mut small = Link::<[u8; 1024], 1>::new("small", [0; 1024], [0; 1024]).unwrap();
        let mut big = link("big");
        big.publishes::<Pos>().unwrap();
        big.publishes::<Cmd>().unwrap();
        big.serves::<Add>().unwrap();
        small.subscribes::<Pos>().unwrap();

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
        assert!(small.is_connected());
        assert_eq!(small.peer_types().count(), 1);
        // The table is rebuilt on every Hello (the peer's first one and its ack), so the two types
        // that did not fit are counted once per Hello rather than once in total.
        assert!(small.stats().remote_overflow >= 2);
    }

    /// The same `TYPE` (same hash, same name) as `Pos`.
    struct Alias;
    impl Topic for Alias {
        const TYPE: &'static str = "Pos";
    }
    struct Other;
    impl Topic for Other {
        const TYPE: &'static str = "Other";
    }

    #[test]
    fn declaration_errors() {
        let mut l = Link::<[u8; 256], 1>::new("l", [0; 256], [0; 256]).unwrap();
        l.publishes::<Pos>().unwrap();
        l.subscribes::<Pos>().unwrap(); // the same type with another flag folds into one slot
        assert_eq!(l.subscribes::<Cmd>(), Err(Error::TableFull));
        assert!(L::new(&"x".repeat(ID_MAX + 1), [0; 1024], [0; 1024]).is_err());
        let mut l = Link::<[u8; 256], 2>::new("l", [0; 256], [0; 256]).unwrap();
        l.publishes::<Pos>().unwrap();
        assert_eq!(
            l.subscribes::<Alias>(),
            Ok(()),
            "same name, same hash: fine"
        );
        assert_eq!(l.subscribes::<Other>(), Ok(()));
    }

    /// Two different names hashing alike is the one failure a 32-bit type hash can produce, and it
    /// has to be caught at declaration time with both names in the message — not left to corrupt
    /// traffic at run time. ("costarring" / "liquid" is a documented FNV-1a 32 collision; the test
    /// asserts that rather than trusting it.)
    #[test]
    fn hash_collision_is_rejected_at_declaration() {
        struct Costarring;
        impl Topic for Costarring {
            const TYPE: &'static str = "costarring";
        }
        struct Liquid;
        impl Topic for Liquid {
            const TYPE: &'static str = "liquid";
        }
        assert_eq!(
            wire::type_hash(Costarring::TYPE),
            wire::type_hash(Liquid::TYPE),
            "the fixture is only meaningful if these actually collide"
        );

        let mut l = Link::<[u8; 256], 4>::new("l", [0; 256], [0; 256]).unwrap();
        l.publishes::<Costarring>().unwrap();
        assert_eq!(
            l.subscribes::<Liquid>(),
            Err(Error::HashCollision {
                existing: "costarring",
                new: "liquid",
            })
        );
    }

    /// The id is bounded by [`ID_MAX`], and exactly `ID_MAX` still has to work — an off-by-one here
    /// would reject a legitimate id or write past the fixed buffer.
    #[test]
    fn id_length_boundary() {
        let exact = "x".repeat(ID_MAX);
        let l = L::new(&exact, [0; 1024], [0; 1024]).unwrap();
        assert_eq!(l.id(), exact);
        assert_eq!(
            L::new(&"x".repeat(ID_MAX + 1), [0; 1024], [0; 1024])
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
        a.publishes::<Pos>().unwrap();
        a.serves::<Add>().unwrap();
        assert!(!a.is_connected());
        assert_eq!(a.peer_id(), None);
        assert!(!a.peer_subscribes::<Pos>());
        assert!(!a.peer_serves::<Add>());
        assert!(!a.peer_is_bridge());
        assert_eq!(a.send(&Pos { x: 1 }), Ok(false));
        assert_eq!(a.request(&Add { a: 1, b: 1 }), Err(Error::NoPeerService));
    }
}
