//! [`Link`] —— 点対点リンクの sans-I/O 状態機械。
//!
//! I/O を持たない。受け取ったバイトを [`Link::feed`] で食わせ、送るべきバイトを
//! [`Link::drain`](stream)/ [`Link::drain_frame`](datagram)で取り出す。時計も持たないので、
//! 生存確認は [`Link::tick`] に現在時刻(ms)を渡して進める。UART / USB CDC / UDP /
//! embassy / RTIC / tokio のどれに繋ぐかは呼び出し側の数行で決まる。
//!
//! 接続の流儀:
//!
//! 1. 双方が起動時に Hello(id + 名乗る型の一覧)を送る。相手の Hello を受けたら ack 付きの
//!    Hello を返す(ack には返さない)。
//! 2. Hello を受けた時点で `Connected`。相手が名乗った型の表(hash → flags / schema)を持ち、
//!    相手が subscribe していない型の `send` はワイヤに出さない。
//! 3. 無音が `ping_interval_ms` 続けば Ping、受信が `timeout_ms` 途絶えれば `Disconnected`。
//!    切れている間は `ping_interval_ms` ごとに Hello を送り直す(相手が後から起動しても、
//!    ケーブルを抜き差ししても、どちらかの Hello が届いた時点で繋がる)。
//!
//! スキーマ指紋(`Topic::SCHEMA`)は Hello で 1 回だけ照合し、不一致の型の Data は捨てる
//! (reiny 本体と同じ「照合できるときだけ落とす」規則)。

use core::fmt;

use prost::Message;
use reiny_core::{Service, Topic};

use crate::wire::{self, DELIMITER, HEADER, HelloEntry, Kind, OVERHEAD, flags};

/// 生存確認のノブ。物理層ごとに遅延が違うので、既定値は目安。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkConfig {
    /// これだけ送信が無ければ Ping を出す(切れている間は Hello を出し直す間隔)。
    pub ping_interval_ms: u32,
    /// これだけ受信が無ければ `Disconnected`。`ping_interval_ms` の 2〜3 倍が目安。
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

/// id の上限(バイト)。Hello の `id_len` が u8 なのでそれ以下、表の固定長としてこの値。
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
        // `&str` から丸ごとコピーしたので必ず UTF-8。
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

/// 相手が Hello で名乗った型 1 つ分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteType {
    /// [`wire::type_hash`]。
    pub hash: u32,
    /// [`wire::flags`] の OR。
    pub flags: u8,
    /// 相手の `Topic::SCHEMA`。
    pub schema: Option<u64>,
    /// 自分の `SCHEMA` と両方 `Some` で不一致。この型の Data は捨てられる。
    pub mismatch: bool,
}

/// 受信したフレームの取っ手。中身は [`Link::payload`] / [`Link::decode`] で読む。
///
/// `Copy` なので持ち回れるが、指しているバイトは**次の [`Link::next`] / [`Link::feed`] まで**
/// しか有効でない。古い取っ手で `payload` を読むと空スライスが返る。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    /// 種別。
    pub kind: Kind,
    /// 型ハッシュ。
    pub hash: u32,
    /// 通し番号 / 相関 id。
    pub seq: u8,
    len: u16,
    generation: u16,
}

/// [`Link::next`] / [`Link::tick`] が返す出来事。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// 相手の Hello が届いた(初回、または相手が再起動した)。payload は Hello そのもの
    /// (`wire::hello_parse` で型名まで読める)。latched な型はここで再送すること。
    Connected(Frame),
    /// `timeout_ms` のあいだ受信が無かった。
    Disconnected,
    /// 購読している型の Data。
    Data(Frame),
    /// serve している request 型の Request。[`Link::reply`] / [`Link::reply_err`] で返す。
    Request(Frame),
    /// 自分の [`Link::request`] への応答。`hash` / `seq` で突き合わせる。
    Reply(Frame),
    /// 自分の [`Link::request`] へのエラー応答([`Link::error_message`])。
    Error(Frame),
}

/// 捨てたものの数。デバッグ用。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// COBS / CRC / 種別が壊れていたフレーム。
    pub bad_frames: u32,
    /// スキーマ指紋の不一致で捨てた Data。
    pub dropped_schema: u32,
    /// 購読していない型の Data、serve していない型の Request。
    pub dropped_unwanted: u32,
    /// 受信バッファより長いフレーム(区切りが来る前に満杯)。
    pub rx_overflow: u32,
    /// 相手が名乗った型のうち表(`N`)に入らなかった数。
    pub remote_overflow: u32,
    /// 送信バッファ満杯で送れなかったフレーム。
    pub tx_full: u32,
}

/// [`Link`] の失敗。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// id が [`ID_MAX`] を超える。
    IdTooLong,
    /// 名乗る型が `N` を超えた。
    TableFull,
    /// 2 つの型名が同じハッシュになった。どちらかの `TYPE` を変えること。
    HashCollision {
        /// 先に登録されていた型名。
        existing: &'static str,
        /// 衝突した型名。
        new: &'static str,
    },
    /// 送受信を始めた後に型を宣言した。宣言は Hello より前でなければ相手に届かない。
    Started,
    /// `publishes` していない型を `send` した。
    NotDeclared,
    /// 相手がその request 型を serve していない(未接続を含む)。
    NoPeerService,
    /// フレームが `FRAME` に収まらない。
    TooLarge,
    /// 送信バッファに空きが無い。`drain` してから再試行。
    Full,
    /// prost の encode が失敗した。
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

/// 点対点リンクの状態機械。
///
/// - `B`: 送受信バッファの型。`[u8; N]` / `&mut [u8]` / `Vec<u8>` のどれでも。
/// - `N`: 自分が名乗る型・相手が名乗る型それぞれの上限。
/// - `FRAME`: raw frame(header + payload + CRC)の上限。payload は `FRAME - 8` まで。
///
/// 送信バッファは [`wire::encoded_max`]`(FRAME)` 以上、受信バッファは相手の `FRAME` に
/// 合わせて同じ以上を用意する。
#[allow(clippy::struct_excessive_bools)] // started / connected / bridge / peer_bridge は直交する旗
pub struct Link<B, const N: usize, const FRAME: usize = 256> {
    id: IdBuf,
    peer: Option<IdBuf>,
    local: [Option<LocalType>; N],
    remote: [Option<RemoteType>; N],
    tx: B,
    tx_len: usize,
    rx: B,
    rx_len: usize,
    /// 直前の `next` が返したフレームのバイト数。次の `next` / `feed` で捨てる(遅延消費)。
    rx_consumed: usize,
    /// 溢れの後、次の区切りまで捨てている最中。
    rx_skip: bool,
    rx_generation: u16,
    scratch: [u8; FRAME],
    config: LinkConfig,
    started: bool,
    connected: bool,
    /// 自分は bridge(相手の宣言を鏡写しにする側。[`wire::HELLO_BRIDGE`])。
    bridge: bool,
    /// 相手が bridge と名乗った。
    peer_bridge: bool,
    data_seq: u8,
    req_seq: u8,
    now_ms: u32,
    last_rx_ms: u32,
    last_tx_ms: u32,
    stats: Stats,
}

impl<B: AsRef<[u8]> + AsMut<[u8]>, const N: usize, const FRAME: usize> Link<B, N, FRAME> {
    /// COBS + 区切りを含めた 1 フレームの上限。`drain_frame` の `out` はこれ以上にする。
    pub const MAX_ENCODED: usize = wire::encoded_max(FRAME);

    /// 自分の id と送受信バッファを渡して作る。型の宣言([`Link::publishes`] 等)を済ませて
    /// から I/O を始めること。
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

    /// 生存確認のノブを差し替える。
    #[must_use]
    pub fn with_config(mut self, config: LinkConfig) -> Self {
        self.config = config;
        self
    }

    /// **bridge** として振る舞う: 型を名乗らず、相手が publish する型は全部受け、相手が
    /// subscribe する型は全部送れ、相手が呼ぶ request 型は全部 serve する(相手の宣言の鏡)。
    /// zenoh への橋(`reiny bridge serial …`)がこれ。宣言と同じく I/O を始める前に。
    /// bridge は自分の型表を持たないので指紋は照合しない —— 相手の Hello の指紋を、橋の先
    /// (zenoh の attachment)へそのまま運ぶ。
    #[must_use]
    pub fn as_bridge(mut self) -> Self {
        self.bridge = true;
        self
    }

    /// 自分は bridge か。
    #[must_use]
    pub fn is_bridge(&self) -> bool {
        self.bridge
    }

    /// 自分の id。
    #[must_use]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    // -----------------------------------------------------------------------
    // 宣言
    // -----------------------------------------------------------------------

    /// 型 `T` を publish する。
    pub fn publishes<T: Topic>(&mut self) -> Result<(), Error> {
        self.declare::<T>(flags::PUB)
    }

    /// 型 `T` を latched で publish する —— `Connected` のたびに直近値を再送する約束。
    /// 再送そのものは呼び出し側がやる(`Link` は値を保持しない)。
    pub fn publishes_latched<T: Topic>(&mut self) -> Result<(), Error> {
        self.declare::<T>(flags::PUB | flags::LATCHED)
    }

    /// 型 `T` を subscribe する。
    pub fn subscribes<T: Topic>(&mut self) -> Result<(), Error> {
        self.declare::<T>(flags::SUB)
    }

    /// request 型 `S` を serve する。
    pub fn serves<S: Service>(&mut self) -> Result<(), Error> {
        self.declare::<S>(flags::SERVE)
    }

    /// request 型 `S` を呼ぶ、と名乗る。[`Link::request`] に宣言は要らないが、相手が bridge
    /// (zenoh への橋)のときは、これで型名が伝わって初めて橋の先の service に届く。
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

    /// 受信したバイトを渡す。どんな切れ方でもよい。受け入れた長さを返す —— 受信バッファに
    /// 取り出し待ちのフレームがあって入り切らないときだけ短くなるので、[`Link::next`] を
    /// 回してから残りを渡す。
    pub fn feed(&mut self, bytes: &[u8]) -> usize {
        self.rx_compact();
        let mut rest = bytes;
        while !rest.is_empty() {
            if self.rx_skip {
                // 溢れの後: 次の区切りまで捨てて再同期する。
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
                    // 完成したフレームが取り出されていないだけ。呼び出し側が next() を回す。
                    return bytes.len() - rest.len();
                }
                // 区切りが来る前に満杯 = フレームがバッファより長い。捨てて再同期。
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

    /// 送るべきバイトを `out` に写して長さを返す(stream 向け: 切れ方は問わない)。
    /// 0 なら送るものが無い。
    pub fn drain(&mut self, out: &mut [u8]) -> usize {
        self.ensure_started();
        let n = self.tx_len.min(out.len());
        out[..n].copy_from_slice(&self.tx.as_ref()[..n]);
        self.tx_consume(n);
        n
    }

    /// 送るべきフレームを**丸ごと 1 つ** `out` に写して長さを返す(datagram 向け: 1 フレーム =
    /// 1 datagram)。`None` なら送るものが無い。`out` が [`Link::MAX_ENCODED`] より短くて
    /// 入らないフレームは捨てる(詰まらせない)。
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

    /// 送信待ちのバイト数。
    #[must_use]
    pub fn tx_pending(&self) -> usize {
        self.tx_len
    }

    /// 時計を進める。`now_ms` は単調な ms(wrap してよい)。Ping / Hello の再送と
    /// `Disconnected` の判定はここでしか起きないので、定期的に呼ぶこと。
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

    /// 受信バッファから次の出来事を取り出す。無ければ `None`。
    ///
    /// Hello / Ping は内部で処理する(Hello は `Connected` として見える)。購読していない型の
    /// Data・指紋不一致の Data・serve していない型の Request は捨てる(後者にはエラー応答を
    /// 返す)。返ってきた [`Frame`] は次の `next` / `feed` まで有効。
    ///
    /// `Iterator` にしないのは、返す [`Frame`] の中身を読むのに `&self` が要る(取っ手方式、
    /// 借用の都合)ためで、`for` で回せる形にはならない。名前だけ揃えている。
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
            // ここから先はフレームが有効。遅延消費にしておき、返すなら次回に捨てる。
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

    /// `rx[..end]` を COBS 復号して header を読む。壊れていれば `None`。
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

    /// 復号済みフレームを種別ごとに捌く。呼び出し側へ返すなら `Some`。
    fn dispatch(&mut self, frame: Frame) -> Option<Event> {
        match frame.kind {
            Kind::Hello => self.on_hello(frame),
            Kind::Ping => None,
            Kind::Data => {
                // bridge は相手が publish するものを全部受ける(型表を持たないので指紋も見ない)。
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

    /// 相手の Hello: 相手の型表を作り直し、ack でなければ ack を返す。
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
        // ack でない Hello は相手の(再)起動なので常に通知する。ack は握手の完了で、既に
        // 繋がっていれば黙る(双方同時起動で Connected が 2 度鳴らないように)。
        (!ack || !was_connected).then_some(Event::Connected(frame))
    }

    // -----------------------------------------------------------------------
    // 送信
    // -----------------------------------------------------------------------

    /// 型 `T` を送る。相手が `T` を subscribe していなければワイヤに出さず `Ok(false)`。
    pub fn send<T: Topic + Message>(&mut self, msg: &T) -> Result<bool, Error> {
        let hash = wire::type_hash(T::TYPE);
        self.check_publishes(hash)?;
        if !self.peer_subscribes_hash(hash) {
            return Ok(false);
        }
        let len = self.encode_payload(msg)?;
        self.emit_data(hash, len)
    }

    /// [`Link::send`] の encode 済み版: 型ハッシュと prost の bytes で送る(bridge が zenoh から
    /// 受けたものをそのまま流す口)。bridge でなければ `publishes` 済みの型に限る。
    pub fn send_raw(&mut self, hash: u32, payload: &[u8]) -> Result<bool, Error> {
        self.check_publishes(hash)?;
        if !self.peer_subscribes_hash(hash) {
            return Ok(false);
        }
        let len = self.copy_payload(payload)?;
        self.emit_data(hash, len)
    }

    /// request 型 `S` を送り、相関 id(`seq`)を返す。応答は [`Event::Reply`] / [`Event::Error`]。
    pub fn request<S: Service>(&mut self, req: &S) -> Result<u8, Error> {
        let hash = wire::type_hash(S::TYPE);
        if !self.peer_serves_hash(hash) {
            return Err(Error::NoPeerService);
        }
        let len = self.encode_payload(req)?;
        self.emit_request(hash, len)
    }

    /// [`Link::request`] の encode 済み版。
    pub fn request_raw(&mut self, hash: u32, payload: &[u8]) -> Result<u8, Error> {
        if !self.peer_serves_hash(hash) {
            return Err(Error::NoPeerService);
        }
        let len = self.copy_payload(payload)?;
        self.emit_request(hash, len)
    }

    /// [`Event::Request`] に応答する。`seq` は request フレームのもの。
    pub fn reply<S: Service>(&mut self, seq: u8, resp: &S::Response) -> Result<(), Error> {
        let len = self.encode_payload(resp)?;
        self.emit(Kind::Reply, wire::type_hash(S::TYPE), seq, len)
    }

    /// [`Link::reply`] の encode 済み版。`hash` は request 型のもの。
    pub fn reply_raw(&mut self, seq: u8, hash: u32, payload: &[u8]) -> Result<(), Error> {
        let len = self.copy_payload(payload)?;
        self.emit(Kind::Reply, hash, seq, len)
    }

    /// [`Event::Request`] にエラーで応答する。
    pub fn reply_err<S: Service>(&mut self, seq: u8, message: &str) -> Result<(), Error> {
        self.emit_error(wire::type_hash(S::TYPE), seq, message)
    }

    /// [`Link::reply_err`] の型ハッシュ版。
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

    /// prost で `scratch[HEADER..]` に encode し、payload 長を返す。
    fn encode_payload<M: Message>(&mut self, msg: &M) -> Result<usize, Error> {
        let len = msg.encoded_len();
        if len > FRAME.saturating_sub(OVERHEAD) {
            return Err(Error::TooLarge);
        }
        let mut slot: &mut [u8] = &mut self.scratch[HEADER..HEADER + len];
        msg.encode(&mut slot).map_err(|_| Error::Encode)?;
        Ok(len)
    }

    /// encode 済みの bytes を `scratch[HEADER..]` に写し、payload 長を返す。
    fn copy_payload(&mut self, payload: &[u8]) -> Result<usize, Error> {
        if payload.len() > FRAME.saturating_sub(OVERHEAD) {
            return Err(Error::TooLarge);
        }
        self.scratch[HEADER..HEADER + payload.len()].copy_from_slice(payload);
        Ok(payload.len())
    }

    /// `scratch` の payload を header + CRC で封じ、COBS で包んで送信バッファへ積む。
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

    /// 最初の I/O で Hello を積む。宣言はこれより前に済んでいる(以後は `Started`)。
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
    // 読み出し
    // -----------------------------------------------------------------------

    /// フレームの payload。取っ手が古ければ空。
    #[must_use]
    pub fn payload(&self, frame: &Frame) -> &[u8] {
        if frame.generation != self.rx_generation {
            return &[];
        }
        &self.rx.as_ref()[HEADER..HEADER + usize::from(frame.len)]
    }

    /// Data / Request のフレームを型 `T` として decode する。型ハッシュが違う / decode
    /// できない / 取っ手が古いなら `None`。
    #[must_use]
    pub fn decode<T: Topic + Message + Default>(&self, frame: &Frame) -> Option<T> {
        if frame.hash != wire::type_hash(T::TYPE) {
            return None;
        }
        T::decode(self.payload(frame)).ok()
    }

    /// [`Event::Reply`] のフレームを request 型 `S` の応答として decode する。Reply は
    /// request 型のハッシュを運ぶので、照合も `S` で行う。
    #[must_use]
    pub fn decode_reply<S: Service>(&self, frame: &Frame) -> Option<S::Response> {
        if frame.hash != wire::type_hash(S::TYPE) {
            return None;
        }
        S::Response::decode(self.payload(frame)).ok()
    }

    /// [`Event::Error`] のメッセージ。
    #[must_use]
    pub fn error_message(&self, frame: &Frame) -> &str {
        core::str::from_utf8(self.payload(frame)).unwrap_or("")
    }

    // -----------------------------------------------------------------------
    // 相手
    // -----------------------------------------------------------------------

    /// 相手の Hello を受けてから `timeout_ms` 以内に受信があるか。
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// 相手の id(接続中のみ)。
    #[must_use]
    pub fn peer_id(&self) -> Option<&str> {
        self.peer.as_ref().map(IdBuf::as_str)
    }

    /// 相手が名乗った型。
    pub fn peer_types(&self) -> impl Iterator<Item = &RemoteType> {
        self.remote.iter().flatten()
    }

    /// 相手は bridge か(未接続なら false)。bridge は何でも subscribe / serve する。
    #[must_use]
    pub fn peer_is_bridge(&self) -> bool {
        self.connected && self.peer_bridge
    }

    /// 相手が型 `T` を subscribe しているか(未接続なら false、bridge なら true)。
    #[must_use]
    pub fn peer_subscribes<T: Topic>(&self) -> bool {
        self.peer_subscribes_hash(wire::type_hash(T::TYPE))
    }

    /// [`Link::peer_subscribes`] の型ハッシュ版。
    #[must_use]
    pub fn peer_subscribes_hash(&self, hash: u32) -> bool {
        self.connected
            && (self.peer_bridge
                || self
                    .remote_type(hash)
                    .is_some_and(|t| t.flags & flags::SUB != 0))
    }

    /// 相手が request 型 `T` を serve しているか(未接続なら false、bridge なら true)。
    #[must_use]
    pub fn peer_serves<T: Topic>(&self) -> bool {
        self.peer_serves_hash(wire::type_hash(T::TYPE))
    }

    /// [`Link::peer_serves`] の型ハッシュ版。
    #[must_use]
    pub fn peer_serves_hash(&self, hash: u32) -> bool {
        self.connected
            && (self.peer_bridge
                || self
                    .remote_type(hash)
                    .is_some_and(|t| t.flags & flags::SERVE != 0))
    }

    /// 捨てたものの数。
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.stats
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // テストは panic で失敗を表現してよい
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

    /// `Pos` と同じ型名・別指紋(別プロジェクトの同名型)。
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

    /// 双方の送信を相手に流し込む(stream 風: 1 バイトずつ)。何か流れたら true。
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
        // 宣言は Hello の後には足せない。
        assert_eq!(a.subscribes::<Cmd>(), Err(Error::Started));
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
        // 取っ手は次の next で古くなる。
        assert!(b.payload(&f).is_empty());
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

    #[test]
    fn unserved_request_gets_error_reply() {
        let mut a = link("a");
        let mut b = link("b");
        // b は Add を serve していないが、a には偽の表を持たせず直接フレームを送る:
        // 相手の表が古い(再起動直後)状況の再現。
        b.serves::<Add>().unwrap();
        connect(&mut a, &mut b);
        let seq = a.request(&Add { a: 1, b: 1 }).unwrap();
        // b を「Add を serve しない」個体に差し替える。
        let mut b2 = link("b");
        b2.subscribes::<Cmd>().unwrap();
        b2.tick(0);
        // b2 が request を処理(エラー応答を積む)するには next() が要る。
        loop {
            let moved = pump(&mut a, &mut b2);
            while b2.next().is_some() {}
            if !moved {
                break;
            }
        }
        // a には b2 の Hello(Connected)と、request へのエラー応答が届く。
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
        a.publishes::<PosV2>().unwrap(); // 同名・別指紋
        b.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);
        assert!(b.peer_types().any(|t| t.mismatch));
        assert_eq!(a.send(&PosV2 { x: 1 }), Ok(true));
        while pump(&mut a, &mut b) {}
        assert!(b.next().is_none());
        assert_eq!(b.stats().dropped_schema, 1);
    }

    #[test]
    fn ping_keeps_alive_and_silence_disconnects() {
        let mut a = link("a");
        let mut b = link("b");
        connect(&mut a, &mut b);
        // 1.5 s: a は Ping を出し、b はそれで生存を知る。
        for t in (100..=1500).step_by(100) {
            assert!(a.tick(t).is_none());
            assert!(b.tick(t).is_none());
            while pump(&mut a, &mut b) {}
            assert!(a.next().is_none() && b.next().is_none());
        }
        assert!(a.is_connected() && b.is_connected());
        // b が黙る(a の送信は b に届くが b の送信を捨てる)。
        for t in (1600..=6000).step_by(100) {
            let ev = a.tick(t);
            let mut sink = [0u8; 2048];
            let _ = b.drain(&mut sink);
            let mut buf = [0u8; 2048];
            let n = a.drain(&mut buf);
            b.feed(&buf[..n]);
            while b.next().is_some() {}
            if ev == Some(Event::Disconnected) {
                // b の最後の送信は t=1000 の Ping。timeout 3000 を超えた最初の tick で切れる。
                assert_eq!(t, 4100, "disconnect at {t}");
                break;
            }
            assert!(t < 6000, "never disconnected");
        }
        assert!(!a.is_connected());
        assert_eq!(a.peer_id(), None);
        // 切れている間は Hello を出し直し、b が(next() で)答えれば繋がり直す。
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

    #[test]
    fn peer_restart_reconnects_and_reannounces() {
        let mut a = link("a");
        let mut b = link("b");
        b.publishes_latched::<Pos>().unwrap();
        a.subscribes::<Pos>().unwrap();
        connect(&mut a, &mut b);
        // b が再起動: 新しい Link が Hello(no ack)を送る → a は Connected を再度受ける。
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
        // 前にゴミ、途中で分割、後ろに壊れたフレーム。
        b.feed(&[0x55, 0xaa, 0x01]);
        b.feed(&buf[..n / 2]);
        assert!(b.next().is_none(), "half a frame is not a frame");
        b.feed(&buf[n / 2..n]);
        b.feed(&[0x03, 0x01, 0x02, 0x00]); // COBS としては通るが短すぎる
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
        // 区切りの無い 1500 バイト(受信バッファ 1024 より長い)。
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

    #[test]
    fn tx_full_and_too_large_are_reported() {
        let mut small = Link::<[u8; 64], 2, 32>::new("s", [0; 64], [0; 64]).unwrap();
        small.publishes::<Cmd>().unwrap();
        let mut sink = [0u8; 64];
        let _ = small.drain(&mut sink); // Hello を吐いて空にする
        // 相手が居ないので send は Ok(false)。emit の経路は request で確かめる。
        assert_eq!(small.send(&Cmd { v: 1 }), Ok(false));
        assert_eq!(small.reply_err::<Add>(0, "x"), Ok(()));
        // 64 バイトの tx に 10 バイト級のフレームを詰めていくと満杯になる。
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
        // FRAME = 32 に 40 バイトの payload は入らない。
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
        // bridge は何でも受ける / 送れる / serve する。
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

        // bridge → mcu の service 呼び出し(raw)。
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

        // mcu → bridge の service 呼び出し: bridge は serve していない型の request も受ける。
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

    /// `Pos` と同じ `TYPE`(同じハッシュ・同じ名前)。
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
        l.subscribes::<Pos>().unwrap(); // 同じ型の別フラグは 1 枠に畳まれる
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
}
