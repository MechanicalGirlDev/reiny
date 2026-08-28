//! [`Host`] —— tokio で [`Link`] を [`Transport`] の上で回すドライバ。
//!
//! `Link` は sans-I/O なので、ホスト側ではこれが I/O と時計を供給する: 受信 → `feed` →
//! `next`、送信要求 → `drain` / `drain_frame` → `transport.send`、100 ms ごとの `tick`。
//! 利用側は型で `send` / `call` し、届いたものを `recv` で受ける。

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use prost::Message;
use reiny_core::{Service, Topic};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::link::{Error, Event, Link};
use crate::transport::Transport;
use crate::wire;

/// ホストで使う `Link`: ヒープのバッファ、型 64 個まで、フレーム 4 KiB。
pub type HostLink = Link<Vec<u8>, 64, 4096>;

impl HostLink {
    /// ホスト用の既定サイズで作る(送受信バッファ各 16 KiB)。
    pub fn host(id: &str) -> Result<Self, Error> {
        Self::new(id, vec![0; 16 * 1024], vec![0; 16 * 1024])
    }
}

/// 相手が Hello で名乗った型(名前付き)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerType {
    /// [`wire::type_hash`]。
    pub hash: u32,
    /// `Topic::TYPE`。
    pub name: String,
    /// [`wire::flags`] の OR。
    pub flags: u8,
    /// 相手の `Topic::SCHEMA`。
    pub schema: Option<u64>,
}

/// [`Host::recv`] が返す出来事。Reply / Error は [`Host::call`] が内部で消費する。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostEvent {
    /// 相手の Hello(初回、または相手の再起動)。latched な型はここで送り直す。
    Connected {
        /// 相手の id。
        id: String,
        /// 相手が名乗った型。
        types: Vec<PeerType>,
    },
    /// 受信が途絶えた。
    Disconnected,
    /// 購読している型の Data。[`HostEvent::decode`] で型にする。
    Data {
        /// 型ハッシュ。
        hash: u32,
        /// 通し番号。
        seq: u8,
        /// prost の bytes。
        payload: Vec<u8>,
    },
    /// serve している型の Request。[`Host::reply`] / [`Host::reply_err`] に `seq` を渡す。
    Request {
        /// request 型のハッシュ。
        hash: u32,
        /// 相関 id。
        seq: u8,
        /// prost の bytes。
        payload: Vec<u8>,
    },
}

impl HostEvent {
    /// Data / Request を型 `T` として decode する。型ハッシュが違えば `None`。
    #[must_use]
    pub fn decode<T: Topic + Message + Default>(&self) -> Option<T> {
        match self {
            Self::Data { hash, payload, .. } | Self::Request { hash, payload, .. }
                if *hash == wire::type_hash(T::TYPE) =>
            {
                T::decode(payload.as_slice()).ok()
            }
            _ => None,
        }
    }
}

/// [`Host::call`] の失敗。
#[derive(Debug)]
pub enum CallError {
    /// 相手がその request 型を serve していない(未接続を含む)。
    NoPeerService,
    /// 期限までに応答が無かった。
    Timeout,
    /// 相手が `reply_err` した。
    Remote(String),
    /// 応答が `Response` として decode できない。
    Decode(prost::DecodeError),
    /// link 層の失敗(送信バッファ満杯など)。
    Link(Error),
    /// ドライバが終了した(transport の EOF / エラー)。
    Closed,
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPeerService => f.write_str("peer does not serve this request type"),
            Self::Timeout => f.write_str("timed out waiting for a reply"),
            Self::Remote(m) => write!(f, "peer replied with error: {m}"),
            Self::Decode(e) => write!(f, "undecodable reply: {e}"),
            Self::Link(e) => write!(f, "link: {e}"),
            Self::Closed => f.write_str("link driver has stopped"),
        }
    }
}

impl std::error::Error for CallError {}

type Pending = Arc<Mutex<HashMap<(u32, u8), oneshot::Sender<Result<Vec<u8>, String>>>>>;
type Peer = Arc<Mutex<Option<(String, Vec<PeerType>)>>>;

/// 1 本のリンクのホスト側。drop するとドライバも止まる。
///
/// 全メソッドが `&self` なので `Arc` で共有できる(`recv` の受け手は tokio の mutex で 1 つ)。
pub struct Host {
    link: Arc<Mutex<HostLink>>,
    wake: Arc<Notify>,
    events: tokio::sync::Mutex<mpsc::Receiver<HostEvent>>,
    pending: Pending,
    peer: Peer,
    task: JoinHandle<io::Result<()>>,
}

impl Host {
    /// `link` を `transport` の上で回し始める。
    pub fn spawn<T: Transport + 'static>(link: HostLink, transport: T) -> Self {
        let link = Arc::new(Mutex::new(link));
        let wake = Arc::new(Notify::new());
        let (tx, events) = mpsc::channel(256);
        let pending: Pending = Arc::default();
        let peer: Peer = Arc::default();
        let task = tokio::spawn(drive(
            Arc::clone(&link),
            transport,
            Arc::clone(&wake),
            tx,
            Arc::clone(&pending),
            Arc::clone(&peer),
        ));
        Self {
            link,
            wake,
            events: tokio::sync::Mutex::new(events),
            pending,
            peer,
            task,
        }
    }

    /// 次の出来事。ドライバが止まったら `None`。cancel-safe(取り出した出来事を await 地点に
    /// 抱えない)。
    pub async fn recv(&self) -> Option<HostEvent> {
        self.events.lock().await.recv().await
    }

    /// 型 `T` を送る。相手が購読していなければ `Ok(false)`。
    pub fn send<T: Topic + Message>(&self, msg: &T) -> Result<bool, Error> {
        let sent = lock(&self.link).send(msg)?;
        self.wake.notify_one();
        Ok(sent)
    }

    /// [`Host::send`] の encode 済み版(型ハッシュ + prost の bytes)。
    pub fn send_raw(&self, hash: u32, payload: &[u8]) -> Result<bool, Error> {
        let sent = lock(&self.link).send_raw(hash, payload)?;
        self.wake.notify_one();
        Ok(sent)
    }

    /// request 型 `S` を送って応答を待つ。
    pub async fn call<S: Service>(
        &self,
        req: &S,
        timeout: Duration,
    ) -> Result<S::Response, CallError> {
        let bytes = self
            .call_raw(wire::type_hash(S::TYPE), req.encode_to_vec(), timeout)
            .await?;
        S::Response::decode(bytes.as_slice()).map_err(CallError::Decode)
    }

    /// [`Host::call`] の encode 済み版。応答も prost の bytes のまま。
    pub async fn call_raw(
        &self,
        hash: u32,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, CallError> {
        let (tx, rx) = oneshot::channel();
        let seq = {
            // request を積むのと pending に登録するのを同じ lock の中でやる —— 間にドライバが
            // 送って応答まで受け取ると、登録前の応答が捨てられる。
            let mut link = lock(&self.link);
            let seq = link.request_raw(hash, &payload).map_err(|e| match e {
                Error::NoPeerService => CallError::NoPeerService,
                e => CallError::Link(e),
            })?;
            lock(&self.pending).insert((hash, seq), tx);
            seq
        };
        self.wake.notify_one();
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(bytes))) => Ok(bytes),
            Ok(Ok(Err(message))) => Err(CallError::Remote(message)),
            Ok(Err(_)) => Err(CallError::Closed),
            Err(_) => {
                lock(&self.pending).remove(&(hash, seq));
                Err(CallError::Timeout)
            }
        }
    }

    /// [`HostEvent::Request`] に応答する。
    pub fn reply<S: Service>(&self, seq: u8, resp: &S::Response) -> Result<(), Error> {
        lock(&self.link).reply::<S>(seq, resp)?;
        self.wake.notify_one();
        Ok(())
    }

    /// [`Host::reply`] の encode 済み版。`hash` は request 型のもの。
    pub fn reply_raw(&self, seq: u8, hash: u32, payload: &[u8]) -> Result<(), Error> {
        lock(&self.link).reply_raw(seq, hash, payload)?;
        self.wake.notify_one();
        Ok(())
    }

    /// [`HostEvent::Request`] にエラーで応答する。
    pub fn reply_err<S: Service>(&self, seq: u8, message: &str) -> Result<(), Error> {
        self.reply_err_raw(seq, wire::type_hash(S::TYPE), message)
    }

    /// [`Host::reply_err`] の型ハッシュ版。
    pub fn reply_err_raw(&self, seq: u8, hash: u32, message: &str) -> Result<(), Error> {
        lock(&self.link).reply_err_raw(seq, hash, message)?;
        self.wake.notify_one();
        Ok(())
    }

    /// 相手と繋がっているか。
    #[must_use]
    pub fn is_connected(&self) -> bool {
        lock(&self.link).is_connected()
    }

    /// 繋がっている相手の id と型(直近の Hello から)。
    #[must_use]
    pub fn peer(&self) -> Option<(String, Vec<PeerType>)> {
        lock(&self.peer).clone()
    }

    /// 中の `Link`。統計([`Link::stats`])や、ここに無い操作への逃げ道。
    #[must_use]
    pub fn link(&self) -> &Mutex<HostLink> {
        &self.link
    }

    /// ドライバを止める。drop でも止まる。
    pub fn abort(&self) {
        self.task.abort();
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[allow(clippy::cast_possible_truncation)] // Link の時刻は u32 ms で wrap する前提
fn now_ms(start: Instant) -> u32 {
    start.elapsed().as_millis() as u32
}

/// ドライバ本体。transport の EOF / エラー、または `Host` の drop(events の受け手消失)で戻る。
async fn drive<T: Transport>(
    link: Arc<Mutex<HostLink>>,
    mut transport: T,
    wake: Arc<Notify>,
    events: mpsc::Sender<HostEvent>,
    pending: Pending,
    peer: Peer,
) -> io::Result<()> {
    let start = Instant::now();
    let mut rx = vec![0u8; 16 * 1024];
    let mut tx = vec![0u8; HostLink::MAX_ENCODED];
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let out = {
            let mut l = lock(&link);
            let mut out = Vec::new();
            if let Some(ev) = l.tick(now_ms(start)) {
                out.extend(convert(&l, ev, &pending, &peer));
            }
            collect(&mut l, &mut out, &pending, &peer);
            out
        };
        for ev in out {
            if events.send(ev).await.is_err() {
                return Ok(()); // Host が drop された
            }
        }
        flush::<T>(&link, &mut transport, &mut tx).await?;

        tokio::select! {
            r = transport.recv(&mut rx) => {
                let n = r?;
                if n == 0 && !T::DATAGRAM {
                    return Ok(()); // EOF
                }
                let out = {
                    let mut l = lock(&link);
                    let mut out = Vec::new();
                    let mut rest = &rx[..n];
                    while !rest.is_empty() {
                        let accepted = l.feed(rest);
                        rest = &rest[accepted..];
                        collect(&mut l, &mut out, &pending, &peer);
                        if accepted == 0 {
                            break; // next() で空かなかった: フレームがバッファより長い
                        }
                    }
                    out
                };
                for ev in out {
                    if events.send(ev).await.is_err() {
                        return Ok(());
                    }
                }
            }
            () = wake.notified() => {}
            _ = ticker.tick() => {}
        }
    }
}

/// 送信バッファを空になるまで transport へ流す。
async fn flush<T: Transport>(
    link: &Mutex<HostLink>,
    transport: &mut T,
    buf: &mut [u8],
) -> io::Result<()> {
    loop {
        let n = {
            let mut l = lock(link);
            if T::DATAGRAM {
                l.drain_frame(buf)
            } else {
                Some(l.drain(buf)).filter(|&n| n > 0)
            }
        };
        match n {
            Some(n) => transport.send(&buf[..n]).await?,
            None => return Ok(()),
        }
    }
}

fn collect(l: &mut HostLink, out: &mut Vec<HostEvent>, pending: &Pending, peer: &Peer) {
    while let Some(ev) = l.next() {
        out.extend(convert(l, ev, pending, peer));
    }
}

/// `Link` の出来事を持ち出せる形に写す。Reply / Error は `call` の待ち手に渡して消える。
fn convert(l: &HostLink, ev: Event, pending: &Pending, peer: &Peer) -> Option<HostEvent> {
    match ev {
        Event::Connected(f) => {
            let hello = wire::hello_parse(l.payload(&f)).ok()?;
            let types: Vec<PeerType> = hello
                .entries()
                .map(|e| PeerType {
                    hash: e.hash,
                    name: e.name.to_string(),
                    flags: e.flags,
                    schema: e.schema,
                })
                .collect();
            let id = hello.id.to_string();
            *lock(peer) = Some((id.clone(), types.clone()));
            Some(HostEvent::Connected { id, types })
        }
        Event::Disconnected => {
            *lock(peer) = None;
            Some(HostEvent::Disconnected)
        }
        Event::Data(f) => Some(HostEvent::Data {
            hash: f.hash,
            seq: f.seq,
            payload: l.payload(&f).to_vec(),
        }),
        Event::Request(f) => Some(HostEvent::Request {
            hash: f.hash,
            seq: f.seq,
            payload: l.payload(&f).to_vec(),
        }),
        Event::Reply(f) => {
            if let Some(tx) = lock(pending).remove(&(f.hash, f.seq)) {
                let _ = tx.send(Ok(l.payload(&f).to_vec()));
            }
            None
        }
        Event::Error(f) => {
            if let Some(tx) = lock(pending).remove(&(f.hash, f.seq)) {
                let _ = tx.send(Err(l.error_message(&f).to_string()));
            }
            None
        }
    }
}
