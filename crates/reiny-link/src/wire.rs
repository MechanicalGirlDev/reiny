//! The wire format — one frame = COBS(header + payload + CRC) followed by the `0x00` delimiter.
//!
//! ```text
//! raw    : [kind u8][type_hash u32 LE][seq u8][payload …][crc16 LE]
//! on wire: COBS(raw) 0x00
//! ```
//!
//! - **The type travels as a hash** ([`type_hash`] = FNV-1a 32-bit of `Topic::TYPE`). The type name
//!   as a string rides only in [`Kind::Hello`]. type = topic in four bytes.
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

/// Header length (kind 1 + hash 4 + seq 1).
pub const HEADER: usize = 6;
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
    /// One published message. `hash` = the type, `seq` = the running counter.
    Data = 1,
    /// A service invocation. `hash` = the request type, `seq` = the correlation id.
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
    /// The type hash ([`type_hash`]). Zero for Hello / Ping.
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
            Self::Malformed => f.write_str("malformed hello"),
        }
    }
}

impl core::error::Error for WireError {}

const CRC: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_KERMIT);

/// `Topic::TYPE` → the on-wire type hash (FNV-1a 32-bit). It is a `const fn`, so it is computed at
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
    buf[0] = header.kind as u8;
    buf[1..5].copy_from_slice(&header.hash.to_le_bytes());
    buf[5] = header.seq;
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
    let kind = Kind::from_u8(frame[0]).ok_or(WireError::Kind(frame[0]))?;
    let hash = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]);
    Ok((
        Header {
            kind,
            hash,
            seq: frame[5],
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
    /// `schema` is present (`Topic::SCHEMA` is `Some`).
    pub const SCHEMA: u8 = 16;
    /// Calls this request type (`Link::calls`). When the peer is a bridge, this exists purely to
    /// tell it the type's name.
    pub const CALLS: u8 = 32;
}

/// One type's worth of a Hello.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloEntry<'a> {
    /// See [`type_hash`].
    pub hash: u32,
    /// The OR of [`flags`].
    pub flags: u8,
    /// `Topic::SCHEMA`.
    pub schema: Option<u64>,
    /// `Topic::TYPE`. It rides here and nowhere else, to recover the hash and to detect collisions.
    pub name: &'a str,
}

/// Write a Hello payload into `out` and return its length.
///
/// ```text
/// [flags u8][id_len u8][id …][count u8] { [hash u32][flags u8][schema u64][name_len u8][name …] } × count
/// ```
pub fn hello_write<'a>(
    out: &mut [u8],
    ack: bool,
    bridge: bool,
    id: &str,
    entries: impl Iterator<Item = HelloEntry<'a>>,
) -> Result<usize, WireError> {
    let mut w = Writer { out, pos: 0 };
    w.u8(if ack { HELLO_ACK } else { 0 } | if bridge { HELLO_BRIDGE } else { 0 })?;
    w.str(id)?;
    let count_at = w.pos;
    w.u8(0)?;
    let mut count: u8 = 0;
    for e in entries {
        w.u32(e.hash)?;
        w.u8(e.flags | if e.schema.is_some() { flags::SCHEMA } else { 0 })?;
        w.u64(e.schema.unwrap_or(0))?;
        w.str(e.name)?;
        count = count.checked_add(1).ok_or(WireError::TooLarge)?;
    }
    w.out[count_at] = count;
    Ok(w.pos)
}

/// A parsed Hello.
#[derive(Debug, Clone, Copy)]
pub struct Hello<'a> {
    /// Whether this is a reply to the peer's Hello.
    pub ack: bool,
    /// Whether the peer is a bridge ([`HELLO_BRIDGE`]).
    pub bridge: bool,
    /// The peer's id.
    pub id: &'a str,
    count: usize,
    entries: &'a [u8],
}

impl<'a> Hello<'a> {
    /// The sequence of type entries. It stops wherever the shape breaks down.
    #[must_use]
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

/// The iterator behind [`Hello::entries`].
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
        let hash = self.r.u32()?;
        let flags = self.r.u8()?;
        let schema = self.r.u64()?;
        let name = self.r.str()?;
        Some(HelloEntry {
            hash,
            // The SCHEMA bit is an encoding detail (whether `schema` is present). Do not surface it.
            flags: flags & !flags::SCHEMA,
            schema: (flags & flags::SCHEMA != 0).then_some(schema),
            name,
        })
    }
}

/// Take a Hello payload apart.
pub fn hello_parse(payload: &[u8]) -> Result<Hello<'_>, WireError> {
    let mut r = Reader {
        buf: payload,
        pos: 0,
    };
    let flags = r.u8().ok_or(WireError::Malformed)?;
    let id = r.str().ok_or(WireError::Malformed)?;
    let count = r.u8().ok_or(WireError::Malformed)?;
    Ok(Hello {
        ack: flags & HELLO_ACK != 0,
        bridge: flags & HELLO_BRIDGE != 0,
        id,
        count: usize::from(count),
        entries: &payload[r.pos..],
    })
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
                    hash: 1,
                    seq: 2,
                },
                0,
            )
            .unwrap();
            let (h, payload) = parse(&raw[..n]).unwrap();
            assert_eq!(h.kind, kind);
            assert!(payload.is_empty());
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
        raw[0] = 9;
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
        raw[0] = 9;
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
                hash: 1,
                flags: flags::PUB | flags::LATCHED,
                schema: Some(0x1234),
                name: "State",
            },
            HelloEntry {
                hash: 2,
                flags: flags::SUB,
                schema: None,
                name: "Cmd",
            },
        ];
        let mut buf = [0u8; 128];
        let n = hello_write(&mut buf, true, true, "motor", entries.iter().copied()).unwrap();
        let hello = hello_parse(&buf[..n]).unwrap();
        assert!(hello.ack && hello.bridge);
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

    /// The SCHEMA bit is purely an encoding device: it says whether the `schema` field is
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
                hash: 9,
                flags: flags::PUB,
                schema: Some(0),
                name: "S",
            }]
            .into_iter(),
        )
        .unwrap();
        let e = hello_parse(&buf[..n]).unwrap().entries().next().unwrap();
        assert_eq!(e.flags, flags::PUB, "SCHEMA bit leaked into the flags");
        // `Some(0)` is a real fingerprint, not "absent" — the bit is what distinguishes them.
        assert_eq!(e.schema, Some(0));
    }

    #[test]
    fn hello_stops_at_truncation() {
        let mut buf = [0u8; 128];
        let n = hello_write(
            &mut buf,
            false,
            false,
            "x",
            [HelloEntry {
                hash: 1,
                flags: flags::PUB,
                schema: None,
                name: "Long",
            }]
            .into_iter(),
        )
        .unwrap();
        // A Hello cut short mid-entry: the id still reads, and the entries stop at zero.
        let hello = hello_parse(&buf[..n - 3]).unwrap();
        assert_eq!(hello.id, "x");
        assert_eq!(hello.entries().count(), 0);
        assert!(hello_parse(&[]).is_err());
    }

    /// A name whose bytes are not UTF-8 stops the iteration rather than surfacing as a bad `&str`.
    #[test]
    fn hello_stops_at_invalid_utf8_name() {
        let mut buf = [0u8; 64];
        let n = hello_write(
            &mut buf,
            false,
            false,
            "x",
            [HelloEntry {
                hash: 1,
                flags: flags::PUB,
                schema: None,
                name: "AB",
            }]
            .into_iter(),
        )
        .unwrap();
        buf[n - 1] = 0xff; // wreck the last byte of the name
        assert_eq!(hello_parse(&buf[..n]).unwrap().entries().count(), 0);
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
        // More than 255 entries: the count is a u8.
        let entries = core::iter::repeat_n(
            HelloEntry {
                hash: 1,
                flags: flags::PUB,
                schema: None,
                name: "",
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
}
