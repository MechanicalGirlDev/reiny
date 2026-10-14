//! The wire format — one frame = COBS(header + payload + CRC) followed by the `0x00` delimiter.
//!
//! ```text
//! raw    : [RY][epoch 2][kind u8][endpoint_hash u32 LE][seq u8][payload …][crc16 LE]
//! on wire: COBS(raw) 0x00
//! ```
//!
//! - **Exact endpoints travel as a hash** ([`endpoint_hash`]). Complete named-port contracts,
//!   type names, fingerprints, and source pairs travel in [`Kind::Hello`].
//! - **COBS** allows resynchronization on `0x00` (start listening mid-stream and you are aligned
//!   again from the next `0x00`). Datagrams (UDP) use the same shape — one receive path is worth
//!   more than the byte it saves.
//! - **CRC-16/KERMIT** covers header + payload. Serial corruption is caught by the CRC; COBS only
//!   looks after delimiter consistency.
//! - `seq` is a running counter for gap detection on Data, and a correlation id on Request / Reply
//!   / Error.
//!
//! This module is `Link`'s subordinate: it knows how to assemble and take apart a frame and nothing
//! else (it holds no state such as which types are subscribed).

use core::fmt;

/// Header length (magic 2 + epoch 1 + kind 1 + hash 4 + seq 1).
pub const HEADER: usize = 9;
/// Every frame carries this incompatible protocol epoch.
pub const EPOCH: u8 = 2;
/// Trailer length (CRC-16).
pub const TRAILER: usize = 2;
/// The length of a raw frame excluding the payload.
pub const OVERHEAD: usize = HEADER + TRAILER;
/// The frame delimiter. It never appears inside a COBS-encoded frame.
pub const DELIMITER: u8 = 0;

/// The frame kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// Own id plus the list of types being declared. Both sides send it on link up
    /// (§ `hello_write`).
    Hello = 0,
    /// One published message. `hash` identifies the sending output.
    Data = 1,
    /// A service invocation. `hash` identifies the destination server.
    Request = 2,
    /// The response to a [`Kind::Request`]. `hash` / `seq` match the request's.
    Reply = 3,
    /// An error response to a [`Kind::Request`] (the payload is a UTF-8 message).
    Error = 4,
    /// A liveness check sent while the link is silent. No payload.
    Ping = 5,
}

impl Kind {
    fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            0 => Self::Hello,
            1 => Self::Data,
            2 => Self::Request,
            3 => Self::Reply,
            4 => Self::Error,
            5 => Self::Ping,
            _ => return None,
        })
    }
}

/// A raw frame's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The kind.
    pub kind: Kind,
    /// The endpoint hash ([`endpoint_hash`]). Zero for Hello / Ping.
    pub hash: u32,
    /// Running counter / correlation id.
    pub seq: u8,
}

/// A failure while assembling or taking apart a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    /// Does not fit the buffer.
    TooLarge,
    /// Too short to hold a header + CRC.
    Short,
    /// CRC mismatch.
    Crc,
    /// The COBS encoding is broken.
    Cobs,
    /// Unknown kind.
    Kind(u8),
    /// Unsupported protocol magic or epoch.
    Epoch,
    /// The Hello payload is not shaped like one.
    Malformed,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge => f.write_str("frame does not fit the buffer"),
            Self::Short => f.write_str("frame shorter than header + crc"),
            Self::Crc => f.write_str("crc mismatch"),
            Self::Cobs => f.write_str("malformed cobs"),
            Self::Kind(k) => write!(f, "unknown frame kind {k}"),
            Self::Epoch => f.write_str("unsupported link epoch"),
            Self::Malformed => f.write_str("malformed hello"),
        }
    }
}

impl core::error::Error for WireError {}

const CRC: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_KERMIT);

/// `MessageType::TYPE_NAME` → the on-wire type hash (FNV-1a 32-bit). It is a `const fn`, so it is computed at
/// compile time.
///
/// 32 bits keeps the per-frame fixed cost down. A collision is detectable at link up, because Hello
/// carries the names as well (that is `Link`'s job, not this module's).
#[must_use]
pub const fn type_hash(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut h: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        h ^= bytes[i] as u32;
        h = h.wrapping_mul(0x0100_0193);
        i += 1;
    }
    h
}

/// FNV-1a of the two u32-length-prefixed UTF-8 address components.
#[must_use]
pub fn endpoint_hash(namespace: &str, port: &str) -> u32 {
    let mut hash = 0x811c_9dc5u32;
    for component in [namespace, port] {
        // Wire strings are limited to 255 bytes; platform-sized lengths never reach the wire.
        let length = u32::try_from(component.len()).unwrap_or(u32::MAX);
        for byte in length.to_le_bytes().iter().chain(component.as_bytes()) {
            hash = (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193);
        }
    }
    hash
}

/// The **upper bound** on the length of a `raw_len`-byte raw frame once COBS-wrapped and
/// delimited.
#[must_use]
pub const fn encoded_max(raw_len: usize) -> usize {
    cobs::max_encoding_length(raw_len) + 1
}

/// Call this **after** writing the payload into `buf[HEADER..HEADER + payload_len]`; it fills in
/// the header and the CRC and returns the raw frame length.
pub fn seal(buf: &mut [u8], header: Header, payload_len: usize) -> Result<usize, WireError> {
    let raw_len = OVERHEAD + payload_len;
    if raw_len > buf.len() {
        return Err(WireError::TooLarge);
    }
    buf[..3].copy_from_slice(&[b'R', b'Y', EPOCH]);
    buf[3] = header.kind as u8;
    buf[4..8].copy_from_slice(&header.hash.to_le_bytes());
    buf[8] = header.seq;
    let crc = CRC.checksum(&buf[..HEADER + payload_len]);
    buf[HEADER + payload_len..raw_len].copy_from_slice(&crc.to_le_bytes());
    Ok(raw_len)
}

/// Split a raw frame (already COBS-decoded) into its header and payload. A CRC mismatch, a frame
/// that is too short, or an unknown kind are all `Err`.
pub fn parse(frame: &[u8]) -> Result<(Header, &[u8]), WireError> {
    if frame.len() < OVERHEAD {
        return Err(WireError::Short);
    }
    let body_len = frame.len() - TRAILER;
    let expected = u16::from_le_bytes([frame[body_len], frame[body_len + 1]]);
    if CRC.checksum(&frame[..body_len]) != expected {
        return Err(WireError::Crc);
    }
    if frame[..3] != [b'R', b'Y', EPOCH] {
        return Err(WireError::Epoch);
    }
    let kind = Kind::from_u8(frame[3]).ok_or(WireError::Kind(frame[3]))?;
    let hash = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);
    Ok((
        Header {
            kind,
            hash,
            seq: frame[8],
        },
        &frame[HEADER..body_len],
    ))
}

/// COBS-wrap a raw frame, append the delimiter and write it into `out`. Returns how much was
/// written.
pub fn encode(raw: &[u8], out: &mut [u8]) -> Result<usize, WireError> {
    if out.len() < cobs::max_encoding_length(raw.len()) + 1 {
        return Err(WireError::TooLarge);
    }
    let n = cobs::encode(raw, out);
    out[n] = DELIMITER;
    Ok(n + 1)
}

/// Decode a COBS sequence (delimiter already stripped) in place and return the raw frame length.
pub fn decode_in_place(buf: &mut [u8]) -> Result<usize, WireError> {
    cobs::decode_in_place(buf).map_err(|_| WireError::Cobs)
}

// ---------------------------------------------------------------------------
// Hello
// ---------------------------------------------------------------------------

/// Bit in Hello's first byte: this is a reply to the peer's Hello (so replies are not replied to).
pub const HELLO_ACK: u8 = 0x01;
/// Bit in Hello's first byte: the sender is a **bridge** — it subscribes to every type the peer
/// publishes, can publish every type the peer subscribes to, and serves every request type the peer
/// calls. The mark of a side that declares no types of its own and instead mirrors the peer's
/// declarations (the bridge to zenoh).
pub const HELLO_BRIDGE: u8 = 0x02;

/// Per-type flags in a Hello entry.
pub mod flags {
    /// Publishes this type.
    pub const PUB: u8 = 1;
    /// Subscribes to this type.
    pub const SUB: u8 = 2;
    /// Serves this request type.
    pub const SERVE: u8 = 4;
    /// The publication is latched (a promise to re-send the most recent value on link up).
    pub const LATCHED: u8 = 8;
    /// `schema` is present. Required for every declared type.
    pub const SCHEMA_FINGERPRINT: u8 = 16;
    /// Calls this request type (`Link::rpc_client`). When the peer is a bridge, this exists purely to
    /// tell it the type's name.
    pub const CALLS: u8 = 32;
}

/// A borrowed exact endpoint address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointRef<'a> {
    /// Canonical endpoint owner namespace.
    pub namespace: &'a str,
    /// Declared port name.
    pub port: &'a str,
}

/// A static source list or a validated borrowed wire source list.
#[derive(Debug, Clone, Copy)]
pub enum Sources<'a> {
    /// A generated static source list.
    Static(&'a [reiny_core::StaticEndpointAddress]),
    /// Source bytes validated by the Hello parser.
    Wire(&'a [u8]),
}

impl<'a> Sources<'a> {
    /// Iterate exact source endpoint pairs without allocating.
    pub fn iter(self) -> impl Iterator<Item = EndpointRef<'a>> {
        let (mut static_iter, bytes) = match self {
            Self::Static(sources) => (Some(sources.iter()), &[][..]),
            Self::Wire(bytes) => (None, bytes),
        };
        let mut reader = Reader { buf: bytes, pos: 0 };
        core::iter::from_fn(move || {
            if let Some(sources) = &mut static_iter {
                return sources.next().map(|source| EndpointRef {
                    namespace: source.namespace,
                    port: source.port,
                });
            }
            Some(EndpointRef {
                namespace: reader.str()?,
                port: reader.str()?,
            })
        })
    }
}
impl PartialEq for Sources<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for Sources<'_> {}

/// An RPC response contract, carried on its port rather than as a permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloType<'a> {
    /// Fully qualified protobuf message name.
    pub name: &'a str,
    /// Compiled schema fingerprint.
    pub schema: u64,
}

/// One complete named port contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloEntry<'a> {
    /// Hash of the peer namespace and this port name.
    pub hash: u32,
    /// Exactly one direction plus optional publication latching.
    pub flags: u8,
    /// Compiled schema fingerprint.
    pub schema: u64,
    /// Fully qualified protobuf message name.
    pub name: &'a str,
    /// Declared port name.
    pub port: &'a str,
    /// Required RPC response metadata; absent for streams.
    pub response: Option<HelloType<'a>>,
    /// Exact output endpoints selected by this input or caller.
    pub sources: Sources<'a>,
}

/// Write a complete epoch-2 Hello. Fingerprints, response contracts, and exact
/// source references are mandatory; the receiver validates the table atomically.
pub fn hello_write<'a>(
    out: &mut [u8],
    ack: bool,
    bridge: bool,
    id: &str,
    entries: impl Iterator<Item = HelloEntry<'a>>,
) -> Result<usize, WireError> {
    let mut w = Writer { out, pos: 0 };
    w.u8(if ack { HELLO_ACK } else { 0 } | if bridge { HELLO_BRIDGE } else { 0 })?;
    w.bytes(&reiny_core::SCHEMA_FINGERPRINT_VERSION.to_le_bytes())?;
    w.str(id)?;
    let count_at = w.pos;
    w.u8(0)?;
    let mut count: u8 = 0;
    for e in entries {
        w.u32(e.hash)?;
        w.u8(e.flags | flags::SCHEMA_FINGERPRINT)?;
        w.str(e.port)?;
        w.str(e.name)?;
        w.u64(e.schema)?;
        w.u8(u8::from(e.response.is_some()))?;
        if let Some(response) = e.response {
            w.str(response.name)?;
            w.u64(response.schema)?;
        }
        let sources = u8::try_from(e.sources.iter().count()).map_err(|_| WireError::TooLarge)?;
        w.u8(sources)?;
        for source in e.sources.iter() {
            w.str(source.namespace)?;
            w.str(source.port)?;
        }
        count = count.checked_add(1).ok_or(WireError::TooLarge)?;
    }
    w.out[count_at] = count;
    Ok(w.pos)
}

/// A fully validated Hello. Nothing is exposed until its complete table parses.
#[derive(Debug, Clone, Copy)]
pub struct Hello<'a> {
    /// This Hello acknowledges its peer.
    pub ack: bool,
    /// Internal transport tooling, not application permission.
    pub bridge: bool,
    /// Canonical peer namespace.
    pub id: &'a str,
    count: usize,
    entries: &'a [u8],
}
impl<'a> Hello<'a> {
    #[must_use]
    /// Iterate the complete validated port table.
    pub fn entries(&self) -> HelloEntries<'a> {
        HelloEntries {
            r: Reader {
                buf: self.entries,
                pos: 0,
            },
            remaining: self.count,
        }
    }
}
/// Borrowed iterator over a validated Hello table.
pub struct HelloEntries<'a> {
    r: Reader<'a>,
    remaining: usize,
}
impl<'a> Iterator for HelloEntries<'a> {
    type Item = HelloEntry<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        self.r.entry()
    }
}

pub(crate) fn valid_port(port: &str) -> bool {
    !port.is_empty()
        && port
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        && port != "."
        && port != ".."
}
pub(crate) fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty() && namespace.split('/').all(valid_port)
}
fn valid_type(name: &str) -> bool {
    !name.is_empty()
        && name.split('.').all(|part| {
            !part.is_empty()
                && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                && !part.as_bytes()[0].is_ascii_digit()
        })
}

/// Parse and validate a complete epoch-2 Hello atomically.
pub fn hello_parse(payload: &[u8]) -> Result<Hello<'_>, WireError> {
    let mut r = Reader {
        buf: payload,
        pos: 0,
    };
    let hello_flags = r.u8().ok_or(WireError::Malformed)?;
    if hello_flags & !(HELLO_ACK | HELLO_BRIDGE) != 0
        || r.bytes(2)
            != Some(
                reiny_core::SCHEMA_FINGERPRINT_VERSION
                    .to_le_bytes()
                    .as_slice(),
            )
    {
        return Err(WireError::Malformed);
    }
    let id = r
        .str()
        .filter(|id| valid_namespace(id))
        .ok_or(WireError::Malformed)?;
    let count = r.u8().ok_or(WireError::Malformed)?;
    let entries = &payload[r.pos..];
    let hello = Hello {
        ack: hello_flags & HELLO_ACK != 0,
        bridge: hello_flags & HELLO_BRIDGE != 0,
        id,
        count: usize::from(count),
        entries,
    };
    if hello.bridge && count != 0 {
        return Err(WireError::Malformed);
    }
    for index in 0..count {
        let entry = r.entry().ok_or(WireError::Malformed)?;
        let direction = entry.flags & !flags::LATCHED;
        if !matches!(
            direction,
            flags::PUB | flags::SUB | flags::SERVE | flags::CALLS
        ) || (entry.flags & flags::LATCHED != 0 && direction != flags::PUB)
            || !valid_port(entry.port)
            || !valid_type(entry.name)
            || entry.hash != endpoint_hash(id, entry.port)
            || matches!(direction, flags::SERVE | flags::CALLS) != entry.response.is_some()
            || entry
                .response
                .is_some_and(|response| !valid_type(response.name))
        {
            return Err(WireError::Malformed);
        }
        let source_count = entry.sources.iter().count();
        if (matches!(direction, flags::PUB | flags::SERVE) && source_count != 0)
            || (direction == flags::SUB && source_count == 0)
            || (direction == flags::CALLS && source_count != 1)
        {
            return Err(WireError::Malformed);
        }
        for (i, source) in entry.sources.iter().enumerate() {
            if !valid_namespace(source.namespace)
                || !valid_port(source.port)
                || entry
                    .sources
                    .iter()
                    .take(i)
                    .any(|previous| previous == source)
            {
                return Err(WireError::Malformed);
            }
        }
        for previous in hello.entries().take(usize::from(index)) {
            let shared_source = previous
                .sources
                .iter()
                .any(|source| entry.sources.iter().any(|other| other == source));
            if shared_source
                && (previous.name != entry.name
                    || previous.schema != entry.schema
                    || previous.response != entry.response
                    || (previous.flags & flags::CALLS != 0) != (entry.flags & flags::CALLS != 0))
            {
                return Err(WireError::Malformed);
            }
            let same_slot = (previous.flags & (flags::PUB | flags::SERVE) != 0)
                == (entry.flags & (flags::PUB | flags::SERVE) != 0);
            if (previous.port == entry.port && same_slot)
                || (previous.hash == entry.hash && previous.port != entry.port)
            {
                return Err(WireError::Malformed);
            }
        }
    }
    if r.pos != payload.len() {
        return Err(WireError::Malformed);
    }
    validate_address_hashes(&hello)?;
    Ok(hello)
}

// An address hash is never accepted for two different exact endpoint pairs,
// including addresses supplied as source references.
fn validate_address_hashes(hello: &Hello<'_>) -> Result<(), WireError> {
    let addresses = || {
        hello.entries().flat_map(|e| {
            core::iter::once(EndpointRef {
                namespace: hello.id,
                port: e.port,
            })
            .chain(e.sources.iter())
        })
    };
    for (index, address) in addresses().enumerate() {
        if addresses().take(index).any(|previous| {
            previous != address
                && endpoint_hash(previous.namespace, previous.port)
                    == endpoint_hash(address.namespace, address.port)
        }) {
            return Err(WireError::Malformed);
        }
    }
    Ok(())
}

struct Writer<'a> {
    out: &'a mut [u8],
    pos: usize,
}

impl Writer<'_> {
    fn bytes(&mut self, b: &[u8]) -> Result<(), WireError> {
        let end = self.pos + b.len();
        if end > self.out.len() {
            return Err(WireError::TooLarge);
        }
        self.out[self.pos..end].copy_from_slice(b);
        self.pos = end;
        Ok(())
    }
    fn u8(&mut self, v: u8) -> Result<(), WireError> {
        self.bytes(&[v])
    }
    fn u32(&mut self, v: u32) -> Result<(), WireError> {
        self.bytes(&v.to_le_bytes())
    }
    fn u64(&mut self, v: u64) -> Result<(), WireError> {
        self.bytes(&v.to_le_bytes())
    }
    fn str(&mut self, s: &str) -> Result<(), WireError> {
        let len = u8::try_from(s.len()).map_err(|_| WireError::TooLarge)?;
        self.u8(len)?;
        self.bytes(s.as_bytes())
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        self.bytes(1).map(|b| b[0])
    }
    fn u32(&mut self) -> Option<u32> {
        self.bytes(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Option<u64> {
        let b = self.bytes(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Some(u64::from_le_bytes(a))
    }
    fn entry(&mut self) -> Option<HelloEntry<'a>> {
        let hash = self.u32()?;
        let encoded_flags = self.u8()?;
        if encoded_flags & flags::SCHEMA_FINGERPRINT == 0 {
            return None;
        }
        let flags = encoded_flags & !flags::SCHEMA_FINGERPRINT;
        let port = self.str()?;
        let name = self.str()?;
        let schema = self.u64()?;
        let response = match self.u8()? {
            0 => None,
            1 => Some(HelloType {
                name: self.str()?,
                schema: self.u64()?,
            }),
            _ => return None,
        };
        let count = self.u8()?;
        let start = self.pos;
        for _ in 0..count {
            self.str()?;
            self.str()?;
        }
        Some(HelloEntry {
            hash,
            flags,
            schema,
            name,
            port,
            response,
            sources: Sources::Wire(&self.buf[start..self.pos]),
        })
    }
    fn str(&mut self) -> Option<&'a str> {
        let len = usize::from(self.u8()?);
        core::str::from_utf8(self.bytes(len)?).ok()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;

    #[test]
    fn type_hash_is_fnv1a_32() {
        // Known FNV-1a vectors (the empty string and "a").
        assert_eq!(type_hash(""), 0x811c_9dc5);
        assert_eq!(type_hash("a"), 0xe40c_292c);
        assert_ne!(type_hash("Ping"), type_hash("Pong"));
    }

    /// The hash is the whole address, so it must depend on every byte of the name and on their
    /// order — the two ways a weak hash aliases distinct types onto one topic.
    #[test]
    fn type_hash_separates_similar_names() {
        assert_ne!(type_hash("MotorState"), type_hash("MotorStat"));
        assert_ne!(type_hash("MotorState"), type_hash("MotorStates"));
        assert_ne!(type_hash("ab"), type_hash("ba"));
        assert_ne!(type_hash("Ping"), type_hash("ping"));
    }

    #[test]
    fn encoded_max_bounds_real_encodings() {
        // Even the worst case (a run with no zeros) stays inside the bound when actually encoded.
        for n in [0usize, 1, 253, 254, 255, 508, 509, 4096] {
            let raw = vec![0x7fu8; n];
            let mut out = vec![0u8; encoded_max(n)];
            let written = encode(&raw, &mut out).unwrap();
            assert!(written <= encoded_max(n), "n={n}");
        }
    }

    #[test]
    fn seal_encode_decode_parse_roundtrip() {
        let mut raw = [0u8; 64];
        raw[HEADER..HEADER + 4].copy_from_slice(&[0, 1, 0, 255]); // a payload containing zeros
        let header = Header {
            kind: Kind::Data,
            hash: 0xdead_beef,
            seq: 7,
        };
        let raw_len = seal(&mut raw, header, 4).unwrap();
        assert_eq!(raw_len, OVERHEAD + 4);

        let mut wire = [0u8; 80];
        let n = encode(&raw[..raw_len], &mut wire).unwrap();
        assert_eq!(wire[n - 1], DELIMITER);
        assert!(!wire[..n - 1].contains(&DELIMITER), "cobs removes zeros");

        let mut back = [0u8; 80];
        back[..n - 1].copy_from_slice(&wire[..n - 1]);
        let len = decode_in_place(&mut back[..n - 1]).unwrap();
        let (h, payload) = parse(&back[..len]).unwrap();
        assert_eq!(h, header);
        assert_eq!(payload, &[0, 1, 0, 255]);
    }

    /// Every kind survives the round trip. `Kind::from_u8` is a hand-written match, so a variant
    /// added to the enum but forgotten there would decode as `WireError::Kind`.
    #[test]
    fn every_kind_roundtrips() {
        for kind in [
            Kind::Hello,
            Kind::Data,
            Kind::Request,
            Kind::Reply,
            Kind::Error,
            Kind::Ping,
        ] {
            let mut raw = [0u8; 16];
            let n = seal(
                &mut raw,
                Header {
                    kind,
                    hash: endpoint_hash("x", "stream"),
                    seq: 2,
                },
                0,
            )
            .unwrap();
            let (h, payload) = parse(&raw[..n]).unwrap();
            assert_eq!(h.kind, kind);
            assert_eq!(payload, b"");
        }
    }

    #[test]
    fn parse_rejects_corruption() {
        let mut raw = [0u8; 16];
        let raw_len = seal(
            &mut raw,
            Header {
                kind: Kind::Ping,
                hash: 0,
                seq: 0,
            },
            0,
        )
        .unwrap();
        assert!(parse(&raw[..raw_len]).is_ok());
        raw[5] ^= 1; // corrupt seq
        assert_eq!(parse(&raw[..raw_len]), Err(WireError::Crc));
        assert_eq!(parse(&raw[..OVERHEAD - 1]), Err(WireError::Short));

        raw[5] ^= 1;
        raw[3] = 9;
        let n = seal(
            &mut raw,
            Header {
                kind: Kind::Ping,
                hash: 0,
                seq: 0,
            },
            0,
        )
        .unwrap();
        raw[3] = 9;
        let crc = CRC.checksum(&raw[..HEADER]);
        raw[HEADER..n].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(parse(&raw[..n]), Err(WireError::Kind(9)));
    }

    /// The CRC covers the payload too, not just the header — a single flipped payload bit has to
    /// take the frame down rather than reaching the decoder as a plausible message.
    #[test]
    fn crc_covers_the_payload() {
        let mut raw = [0u8; 32];
        raw[HEADER..HEADER + 4].copy_from_slice(b"abcd");
        let n = seal(
            &mut raw,
            Header {
                kind: Kind::Data,
                hash: 7,
                seq: 1,
            },
            4,
        )
        .unwrap();
        assert!(parse(&raw[..n]).is_ok());
        raw[HEADER + 2] ^= 0x01;
        assert_eq!(parse(&raw[..n]), Err(WireError::Crc));
    }

    /// Both directions refuse to run off the end of their buffer instead of panicking on a slice.
    #[test]
    fn buffers_that_are_too_small_are_reported() {
        let mut tiny = [0u8; OVERHEAD - 1];
        assert_eq!(
            seal(
                &mut tiny,
                Header {
                    kind: Kind::Ping,
                    hash: 0,
                    seq: 0
                },
                0
            ),
            Err(WireError::TooLarge)
        );

        let mut small = [0u8; 16];
        let payload_len = small.len(); // a payload as long as the whole buffer cannot fit + OVERHEAD
        assert_eq!(
            seal(
                &mut small,
                Header {
                    kind: Kind::Data,
                    hash: 0,
                    seq: 0
                },
                payload_len
            ),
            Err(WireError::TooLarge)
        );

        let mut out = [0u8; 4];
        assert_eq!(encode(&[1, 2, 3, 4, 5], &mut out), Err(WireError::TooLarge));
    }

    /// A COBS sequence whose length byte points past the end is corruption, not a panic.
    #[test]
    fn decode_in_place_rejects_broken_cobs() {
        let mut broken = [0xffu8, 1, 2];
        assert_eq!(decode_in_place(&mut broken), Err(WireError::Cobs));
    }

    #[test]
    fn hello_roundtrip() {
        let entries = [
            HelloEntry {
                hash: endpoint_hash("motor", "state"),
                flags: flags::PUB | flags::LATCHED,
                schema: 0x1234,
                name: "State",
                port: "state",
                response: None,
                sources: Sources::Static(&[]),
            },
            HelloEntry {
                hash: endpoint_hash("motor", "command"),
                flags: flags::SUB,
                schema: 0x5678,
                name: "Cmd",
                port: "command",
                response: None,
                sources: Sources::Static(&[reiny_core::StaticEndpointAddress {
                    namespace: "source",
                    port: "command",
                }]),
            },
        ];
        let mut buf = [0u8; 128];
        let n = hello_write(&mut buf, true, false, "motor", entries.iter().copied()).unwrap();
        let hello = hello_parse(&buf[..n]).unwrap();
        assert!(hello.ack && !hello.bridge);
        assert_eq!(hello.id, "motor");
        let got: Vec<HelloEntry<'_>> = hello.entries().collect();
        assert_eq!(got, entries);
    }

    /// `ack` and `bridge` are independent bits in the same byte; the four combinations have to stay
    /// distinguishable, because `ack` decides whether to answer and `bridge` decides whether the
    /// peer's declarations get mirrored.
    #[test]
    fn hello_flag_bits_are_independent() {
        for (ack, bridge) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut buf = [0u8; 32];
            let n = hello_write(&mut buf, ack, bridge, "x", core::iter::empty()).unwrap();
            let hello = hello_parse(&buf[..n]).unwrap();
            assert_eq!(hello.ack, ack);
            assert_eq!(hello.bridge, bridge);
            assert_eq!(hello.entries().count(), 0);
        }
    }

    /// The `SCHEMA_FINGERPRINT` bit is purely an encoding device: it says whether the `schema` field is
    /// meaningful and must never leak into the flags the caller sees.
    #[test]
    fn schema_bit_does_not_leak_into_flags() {
        let mut buf = [0u8; 64];
        let n = hello_write(
            &mut buf,
            false,
            false,
            "x",
            [HelloEntry {
                hash: endpoint_hash("x", "stream"),
                flags: flags::PUB,
                schema: 0,
                name: "S",
                port: "stream",
                response: None,
                sources: Sources::Static(&[]),
            }]
            .into_iter(),
        )
        .unwrap();
        let e = hello_parse(&buf[..n]).unwrap().entries().next().unwrap();
        assert_eq!(
            e.flags,
            flags::PUB,
            "SCHEMA_FINGERPRINT bit leaked into the flags"
        );
        // Zero is a real fingerprint, not "absent" — the bit is what distinguishes them.
        assert_eq!(e.schema, 0);
    }

    #[test]
    fn hello_rejects_missing_fingerprint_bit() {
        let mut buf = [0u8; 64];
        let n = hello_write(
            &mut buf,
            false,
            false,
            "x",
            [HelloEntry {
                hash: endpoint_hash("x", "stream"),
                flags: flags::PUB,
                schema: 0,
                name: "S",
                port: "stream",
                response: None,
                sources: Sources::Static(&[]),
            }]
            .into_iter(),
        )
        .unwrap();
        buf[10] &= !flags::SCHEMA_FINGERPRINT;
        assert!(hello_parse(&buf[..n]).is_err());
    }

    #[test]
    fn hello_rejects_truncation() {
        let mut buf = [0u8; 128];
        let n = hello_write(
            &mut buf,
            false,
            false,
            "deployment/robot/controller",
            [HelloEntry {
                hash: endpoint_hash("deployment/robot/controller", "stream"),
                flags: flags::PUB,
                schema: 1,
                name: "Long",
                port: "stream",
                response: None,
                sources: Sources::Static(&[]),
            }]
            .into_iter(),
        )
        .unwrap();
        // No partial descriptor is exposed, even when its id was readable.
        for len in 0..n {
            assert!(
                hello_parse(&buf[..len]).is_err(),
                "truncated to {len} bytes"
            );
        }
        assert_eq!(hello_parse(&buf[..n]).unwrap().entries().count(), 1);
        buf[n] = 0;
        assert!(
            hello_parse(&buf[..=n]).is_err(),
            "trailing descriptor bytes"
        );
        assert!(hello_parse(&[]).is_err());
    }

    /// Invalid type names reject the whole descriptor rather than exposing a partial table.
    #[test]
    fn hello_rejects_invalid_utf8_name() {
        let mut buf = [0u8; 64];
        let n = hello_write(
            &mut buf,
            false,
            false,
            "x",
            [HelloEntry {
                hash: endpoint_hash("x", "stream"),
                flags: flags::PUB,
                schema: 1,
                name: "AB",
                port: "stream",
                response: None,
                sources: Sources::Static(&[]),
            }]
            .into_iter(),
        )
        .unwrap();
        let name_at = buf[..n]
            .windows(2)
            .position(|bytes| bytes == b"AB")
            .unwrap();
        buf[name_at] = 0xff; // invalid UTF-8 inside the type name
        assert!(hello_parse(&buf[..n]).is_err());
    }

    /// The writer refuses what the format cannot express, instead of writing a truncated length.
    #[test]
    fn hello_write_rejects_what_it_cannot_encode() {
        let long = "n".repeat(256);
        let mut buf = [0u8; 512];
        // An id longer than 255 bytes: the length prefix is a u8.
        assert_eq!(
            hello_write(&mut buf, false, false, &long, core::iter::empty()),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            hello_write(
                &mut buf,
                false,
                false,
                "deployment/robot",
                [HelloEntry {
                    hash: endpoint_hash("x", "stream"),
                    flags: flags::PUB,
                    schema: 1,
                    name: &long,
                    port: "stream",
                    response: None,
                    sources: Sources::Static(&[]),
                }]
                .into_iter()
            ),
            Err(WireError::TooLarge)
        );
        // More than 255 entries: the count is a u8.
        let entries = core::iter::repeat_n(
            HelloEntry {
                hash: endpoint_hash("x", "stream"),
                flags: flags::PUB,
                schema: 1,
                name: "",
                port: "stream",
                response: None,
                sources: Sources::Static(&[]),
            },
            256,
        );
        let mut big = [0u8; 8192];
        assert_eq!(
            hello_write(&mut big, false, false, "x", entries),
            Err(WireError::TooLarge)
        );
        // An output buffer that cannot even hold the header.
        let mut tiny = [0u8; 2];
        assert_eq!(
            hello_write(&mut tiny, false, false, "toolong", core::iter::empty()),
            Err(WireError::TooLarge)
        );
    }

    #[test]
    fn old_epoch_is_rejected_even_with_a_valid_crc() {
        // Given an otherwise valid current frame with a legacy epoch.
        let mut raw = [0; 32];
        let n = seal(
            &mut raw,
            Header {
                kind: Kind::Data,
                hash: 7,
                seq: 2,
            },
            0,
        )
        .unwrap();
        raw[2] = EPOCH - 1;
        let crc = CRC.checksum(&raw[..n - TRAILER]);
        raw[n - TRAILER..n].copy_from_slice(&crc.to_le_bytes());
        // When parsed, it cannot enter the current protocol.
        assert_eq!(parse(&raw[..n]), Err(WireError::Epoch));
    }

    #[test]
    fn hello_rejects_duplicate_colliding_and_incomplete_contracts() {
        let output = HelloEntry {
            hash: endpoint_hash("l", "p0edd4c2"),
            flags: flags::PUB,
            schema: 1,
            name: "State",
            port: "p0edd4c2",
            response: None,
            sources: Sources::Static(&[]),
        };
        let collision = HelloEntry {
            port: "p0lzv8oe",
            ..output
        };
        assert_eq!(output.hash, endpoint_hash("l", collision.port));
        let missing_response = HelloEntry {
            flags: flags::SERVE,
            ..output
        };
        let missing_source = HelloEntry {
            flags: flags::SUB,
            ..output
        };
        let corrupt_hash = HelloEntry {
            hash: output.hash ^ 1,
            ..output
        };
        // Each malformed contract is rejected as a whole, never exposed as a partial iterator.
        for entries in [
            vec![output, output],
            vec![output, collision],
            vec![missing_response],
            vec![missing_source],
            vec![corrupt_hash],
        ] {
            let mut buf = [0; 512];
            let n = hello_write(&mut buf, false, false, "l", entries.into_iter()).unwrap();
            assert!(hello_parse(&buf[..n]).is_err());
        }
    }

    #[test]
    fn fingerprint_version_and_duplicate_sources_are_rejected() {
        let sources = [reiny_core::StaticEndpointAddress {
            namespace: "source",
            port: "value",
        }; 2];
        let input = HelloEntry {
            hash: endpoint_hash("l", "input"),
            flags: flags::SUB,
            schema: 1,
            name: "State",
            port: "input",
            response: None,
            sources: Sources::Static(&sources),
        };
        let mut buf = [0; 512];
        let n = hello_write(&mut buf, false, false, "l", [input].into_iter()).unwrap();
        assert!(hello_parse(&buf[..n]).is_err());
        let n = hello_write(&mut buf, false, false, "l", core::iter::empty()).unwrap();
        buf[1] = 1;
        assert!(hello_parse(&buf[..n]).is_err());
    }
}
