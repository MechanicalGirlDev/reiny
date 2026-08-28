//! ワイヤ形式 —— 1 フレーム = COBS(header + payload + CRC) + 区切り `0x00`。
//!
//! ```text
//! raw    : [kind u8][type_hash u32 LE][seq u8][payload …][crc16 LE]
//! on wire: COBS(raw) 0x00
//! ```
//!
//! - **型はハッシュで運ぶ**([`type_hash`] = FNV-1a 32 bit of `Topic::TYPE`)。型名の文字列は
//!   [`Kind::Hello`] にだけ載る。type = topic を 4 バイトで表す。
//! - **COBS** は `0x00` を区切りとして再同期できる(途中から聞き始めても次の `0x00` から
//!   揃う)。datagram(UDP)でも同じ形を使う —— 受信経路が 1 本で済む方が、1 バイトの節約より
//!   価値がある。
//! - **CRC-16/KERMIT** は header + payload に掛かる。シリアルの化けは CRC で落とし、
//!   COBS は区切りの整合だけを見る。
//! - `seq` は Data では欠落検出用の通し番号、Request / Reply / Error では相関 id。
//!
//! この module は `Link` の下請けで、フレームの組み立てと分解しか知らない(どの型を
//! 購読しているか等の状態は持たない)。

use core::fmt;

/// header 長(kind 1 + hash 4 + seq 1)。
pub const HEADER: usize = 6;
/// trailer 長(CRC-16)。
pub const TRAILER: usize = 2;
/// payload 以外の raw frame 長。
pub const OVERHEAD: usize = HEADER + TRAILER;
/// フレーム区切り。COBS 符号化後のフレームには現れない。
pub const DELIMITER: u8 = 0;

/// フレームの種別。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// 自分の id と名乗る型の一覧。link up 時に双方が送る(§ `hello_write`)。
    Hello = 0,
    /// publish された 1 メッセージ。`hash` = 型、`seq` = 通し番号。
    Data = 1,
    /// service の呼び出し。`hash` = request 型、`seq` = 相関 id。
    Request = 2,
    /// [`Kind::Request`] への応答。`hash` / `seq` は request と同じ。
    Reply = 3,
    /// [`Kind::Request`] へのエラー応答(payload は UTF-8 のメッセージ)。
    Error = 4,
    /// 無音時の生存確認。payload 無し。
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

/// raw frame の header。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// 種別。
    pub kind: Kind,
    /// 型ハッシュ([`type_hash`])。Hello / Ping では 0。
    pub hash: u32,
    /// 通し番号 / 相関 id。
    pub seq: u8,
}

/// フレームの組み立て・分解の失敗。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    /// バッファに収まらない。
    TooLarge,
    /// 短すぎて header + CRC が無い。
    Short,
    /// CRC 不一致。
    Crc,
    /// COBS が壊れている。
    Cobs,
    /// 未知の種別。
    Kind(u8),
    /// Hello の payload が形になっていない。
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

/// `Topic::TYPE` → ワイヤ上の型ハッシュ(FNV-1a 32 bit)。`const fn` なのでコンパイル時に出る。
///
/// 32 bit なのはフレームごとの固定費を抑えるため。衝突は Hello で名前ごと届くので link up 時に
/// 検出できる(`Link` 側の仕事)。
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

/// raw frame `raw_len` バイトを COBS で包み区切りを付けたときの**上限**長。
#[must_use]
pub const fn encoded_max(raw_len: usize) -> usize {
    cobs::max_encoding_length(raw_len) + 1
}

/// `buf[HEADER..HEADER + payload_len]` に payload を**書き終えた後**に呼び、header と CRC を
/// 付けて raw frame 長を返す。
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

/// raw frame(COBS 復号済み)を header と payload に分ける。CRC 不一致・短すぎ・未知の
/// kind は `Err`。
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

/// raw frame を COBS で包み、区切りを付けて `out` に書く。書いた長さを返す。
pub fn encode(raw: &[u8], out: &mut [u8]) -> Result<usize, WireError> {
    if out.len() < cobs::max_encoding_length(raw.len()) + 1 {
        return Err(WireError::TooLarge);
    }
    let n = cobs::encode(raw, out);
    out[n] = DELIMITER;
    Ok(n + 1)
}

/// 区切りを除いた COBS 列をその場で復号し、raw frame 長を返す。
pub fn decode_in_place(buf: &mut [u8]) -> Result<usize, WireError> {
    cobs::decode_in_place(buf).map_err(|_| WireError::Cobs)
}

// ---------------------------------------------------------------------------
// Hello
// ---------------------------------------------------------------------------

/// Hello の先頭バイトのビット: 相手の Hello への応答である(応答に応答しないため)。
pub const HELLO_ACK: u8 = 0x01;
/// Hello の先頭バイトのビット: 送り手は **bridge** —— 相手が publish する型は全部 subscribe し、
/// 相手が subscribe する型は全部 publish でき、相手が呼ぶ request 型は全部 serve する。型を
/// 名乗らずに相手の宣言を鏡写しにする側(zenoh への橋)のための印。
pub const HELLO_BRIDGE: u8 = 0x02;

/// Hello の各型エントリのフラグ。
pub mod flags {
    /// この型を publish する。
    pub const PUB: u8 = 1;
    /// この型を subscribe する。
    pub const SUB: u8 = 2;
    /// この request 型を serve する。
    pub const SERVE: u8 = 4;
    /// publish は latched(link up 時に直近値を再送する約束)。
    pub const LATCHED: u8 = 8;
    /// `schema` が有効(`Topic::SCHEMA` が `Some`)。
    pub const SCHEMA: u8 = 16;
    /// この request 型を呼ぶ(`Link::calls`)。相手が bridge のとき、型名を知らせるためだけの印。
    pub const CALLS: u8 = 32;
}

/// Hello に載る型 1 つ分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloEntry<'a> {
    /// [`type_hash`]。
    pub hash: u32,
    /// [`flags`] の OR。
    pub flags: u8,
    /// `Topic::SCHEMA`。
    pub schema: Option<u64>,
    /// `Topic::TYPE`。ハッシュの復元と衝突検出のためにここにだけ載る。
    pub name: &'a str,
}

/// Hello の payload を `out` に書き、長さを返す。
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

/// 分解済みの Hello。
#[derive(Debug, Clone, Copy)]
pub struct Hello<'a> {
    /// 相手の Hello への応答か。
    pub ack: bool,
    /// 相手は bridge か([`HELLO_BRIDGE`])。
    pub bridge: bool,
    /// 相手の id。
    pub id: &'a str,
    count: usize,
    entries: &'a [u8],
}

impl<'a> Hello<'a> {
    /// 型エントリの列。途中で形が崩れていたらそこで止まる。
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

/// [`Hello::entries`] のイテレータ。
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
            // SCHEMA ビットはワイヤ上の符号化の都合(`schema` の有無)。表には出さない。
            flags: flags & !flags::SCHEMA,
            schema: (flags & flags::SCHEMA != 0).then_some(schema),
            name,
        })
    }
}

/// Hello の payload を分解する。
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
#[allow(clippy::unwrap_used)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;

    #[test]
    fn type_hash_is_fnv1a_32() {
        // FNV-1a の既知ベクタ(空文字列と "a")。
        assert_eq!(type_hash(""), 0x811c_9dc5);
        assert_eq!(type_hash("a"), 0xe40c_292c);
        assert_ne!(type_hash("Ping"), type_hash("Pong"));
    }

    #[test]
    fn encoded_max_bounds_real_encodings() {
        // 最悪ケース(0 が無い列)を実際に符号化しても上限を超えない。
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
        raw[HEADER..HEADER + 4].copy_from_slice(&[0, 1, 0, 255]); // 0 を含む payload
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
        raw[5] ^= 1; // seq を化かす
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
        // エントリの途中で切れた Hello: id までは読め、エントリは 0 個で止まる。
        let hello = hello_parse(&buf[..n - 3]).unwrap();
        assert_eq!(hello.id, "x");
        assert_eq!(hello.entries().count(), 0);
        assert!(hello_parse(&[]).is_err());
    }
}
