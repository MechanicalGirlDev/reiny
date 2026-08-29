//! [`Host`] — the driver that runs a [`Link`] over a [`Transport`] on tokio.
//!
//! `Link` is sans-I/O, so on the host side this is what supplies the I/O and the clock: receive →
//! `feed` → `next`, a send request → `drain` / `drain_frame` → `transport.send`, and a `tick` every
//! 100 ms. Callers `send` / `call` by type and take what arrives out of `recv`.

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

/// The `Link` used on a host: heap buffers, up to 64 types, 4 KiB frames.
pub type HostLink = Link<Vec<u8>, 64, 4096>;

impl HostLink {
    /// Build one at the host's default sizes (16 KiB for each of the two buffers).
    pub fn host(id: &str) -> Result<Self, Error> {
        Self::new(id, vec![0; 16 * 1024], vec![0; 16 * 1024])
    }
}

/// A type the peer declared in its Hello, with its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerType {
    /// See [`wire::type_hash`].
    pub hash: u32,
    /// `Topic::TYPE`.
    pub name: String,
    /// The OR of [`wire::flags`].
    pub flags: u8,
    /// The peer's `Topic::SCHEMA`.
    pub schema: Option<u64>,
}

/// What [`Host::recv`] reports. Reply / Error are consumed internally by [`Host::call`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostEvent {
    /// The peer's Hello (the first one, or the peer restarting). Re-send latched types here.
    Connected {
        /// The peer's id.
        id: String,
        /// The types the peer declared.
        types: Vec<PeerType>,
    },
    /// Nothing has been received for too long.
    Disconnected,
    /// Data of a subscribed type. Turn it into a type with [`HostEvent::decode`].
    Data {
        /// The type hash.
        hash: u32,
        /// The running counter.
        seq: u8,
        /// The prost bytes.
        payload: Vec<u8>,
    },
    /// A Request for a served type. Pass `seq` to [`Host::reply`] / [`Host::reply_err`].
    Request {
        /// The request type's hash.
        hash: u32,
        /// The correlation id.
        seq: u8,
        /// The prost bytes.
        payload: Vec<u8>,
    },
}

impl HostEvent {
    /// Decode a Data / Request as the type `T`. `None` if the type hash differs.
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

/// A [`Host::call`] failure.
#[derive(Debug)]
pub enum CallError {
    /// The peer does not serve that request type (which includes not being connected).
    NoPeerService,
    /// No response arrived before the deadline.
    Timeout,
    /// The peer answered with `reply_err`.
    Remote(String),
    /// The response does not decode as `Response`.
    Decode(prost::DecodeError),
    /// A link-layer failure (a full send buffer, say).
    Link(Error),
    /// The driver stopped (the transport hit EOF or an error).
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

/// The host side of one link. Dropping it stops the driver too.
///
/// Every method takes `&self`, so it can be shared through an `Arc` (the one `recv` consumer is
/// serialized by a tokio mutex).
pub struct Host {
    link: Arc<Mutex<HostLink>>,
    wake: Arc<Notify>,
    events: tokio::sync::Mutex<mpsc::Receiver<HostEvent>>,
    pending: Pending,
    peer: Peer,
    task: JoinHandle<io::Result<()>>,
}

impl Host {
    /// Start running `link` over `transport`.
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

    /// The next event, or `None` once the driver has stopped. Cancel-safe (no event pulled from the
    /// channel is ever held across an await point).
    pub async fn recv(&self) -> Option<HostEvent> {
        self.events.lock().await.recv().await
    }

    /// Send the type `T`. `Ok(false)` if the peer does not subscribe to it.
    pub fn send<T: Topic + Message>(&self, msg: &T) -> Result<bool, Error> {
        let sent = lock(&self.link).send(msg)?;
        self.wake.notify_one();
        Ok(sent)
    }

    /// The pre-encoded form of [`Host::send`] (type hash + prost bytes).
    pub fn send_raw(&self, hash: u32, payload: &[u8]) -> Result<bool, Error> {
        let sent = lock(&self.link).send_raw(hash, payload)?;
        self.wake.notify_one();
        Ok(sent)
    }

    /// Send the request type `S` and wait for the response.
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

    /// The pre-encoded form of [`Host::call`]. The response stays prost bytes too.
    pub async fn call_raw(
        &self,
        hash: u32,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, CallError> {
        let (tx, rx) = oneshot::channel();
        let seq = {
            // Queuing the request and registering it in `pending` happen under the same lock — if
            // the driver got to send it and take the reply in between, the reply would arrive
            // before the registration and be thrown away.
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

    /// Answer a [`HostEvent::Request`].
    pub fn reply<S: Service>(&self, seq: u8, resp: &S::Response) -> Result<(), Error> {
        lock(&self.link).reply::<S>(seq, resp)?;
        self.wake.notify_one();
        Ok(())
    }

    /// The pre-encoded form of [`Host::reply`]. `hash` is the *request* type's.
    pub fn reply_raw(&self, seq: u8, hash: u32, payload: &[u8]) -> Result<(), Error> {
        lock(&self.link).reply_raw(seq, hash, payload)?;
        self.wake.notify_one();
        Ok(())
    }

    /// Answer a [`HostEvent::Request`] with an error.
    pub fn reply_err<S: Service>(&self, seq: u8, message: &str) -> Result<(), Error> {
        self.reply_err_raw(seq, wire::type_hash(S::TYPE), message)
    }

    /// The type-hash form of [`Host::reply_err`].
    pub fn reply_err_raw(&self, seq: u8, hash: u32, message: &str) -> Result<(), Error> {
        lock(&self.link).reply_err_raw(seq, hash, message)?;
        self.wake.notify_one();
        Ok(())
    }

    /// Whether the peer is connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        lock(&self.link).is_connected()
    }

    /// The connected peer's id and types (from its most recent Hello).
    #[must_use]
    pub fn peer(&self) -> Option<(String, Vec<PeerType>)> {
        lock(&self.peer).clone()
    }

    /// The `Link` inside. For statistics ([`Link::stats`]) and as an escape hatch to operations that
    /// are not mirrored here.
    #[must_use]
    pub fn link(&self) -> &Mutex<HostLink> {
        &self.link
    }

    /// Stop the driver. Dropping the `Host` does the same.
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

#[allow(clippy::cast_possible_truncation)] // Link's clock is u32 ms and is expected to wrap
fn now_ms(start: Instant) -> u32 {
    start.elapsed().as_millis() as u32
}

/// The driver itself. Returns on transport EOF / error, or when the `Host` is dropped (the event
/// receiver disappears).
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
                return Ok(()); // the Host was dropped
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
                            break; // next() did not free anything: the frame is longer than the buffer
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

/// Push the send buffer out to the transport until it is empty.
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

/// Copy a `Link` event into a shape that can be handed out. Reply / Error go to whoever is waiting
/// in `call` and disappear here.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct A {
        #[prost(int32, tag = "1")]
        v: i32,
    }
    impl Topic for A {
        const TYPE: &'static str = "HostA";
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct B {
        #[prost(int32, tag = "1")]
        v: i32,
    }
    impl Topic for B {
        const TYPE: &'static str = "HostB";
    }

    fn data(hash: u32, payload: Vec<u8>) -> HostEvent {
        HostEvent::Data {
            hash,
            seq: 0,
            payload,
        }
    }

    /// `decode` is keyed on the type hash, not on whether the bytes happen to parse. `A` and `B`
    /// have identical wire shapes, so without the hash check the wrong type would decode cleanly —
    /// exactly the silent corruption the type-as-address rule exists to prevent.
    #[test]
    fn decode_matches_on_the_type_hash() {
        let ev = data(wire::type_hash(A::TYPE), A { v: 7 }.encode_to_vec());
        assert_eq!(ev.decode::<A>(), Some(A { v: 7 }));
        assert_eq!(ev.decode::<B>(), None, "decoded as the wrong type");

        // A Request decodes the same way; the other two events carry no payload at all.
        let req = HostEvent::Request {
            hash: wire::type_hash(A::TYPE),
            seq: 3,
            payload: A { v: 1 }.encode_to_vec(),
        };
        assert_eq!(req.decode::<A>(), Some(A { v: 1 }));
        assert_eq!(HostEvent::Disconnected.decode::<A>(), None);
        assert_eq!(
            HostEvent::Connected {
                id: "x".into(),
                types: vec![],
            }
            .decode::<A>(),
            None
        );
    }

    /// Undecodable bytes are `None`, not a panic — the payload comes off a wire that anything can
    /// write to.
    #[test]
    fn decode_rejects_garbage_payloads() {
        let ev = data(wire::type_hash(A::TYPE), vec![0xff, 0xff, 0xff]);
        assert_eq!(ev.decode::<A>(), None);
    }

    /// Every `CallError` says which of the failures it is; `Timeout` and `NoPeerService` in
    /// particular are what callers branch on, and both are reported by different code paths.
    #[test]
    fn call_errors_describe_themselves() {
        assert!(CallError::Timeout.to_string().contains("timed out"));
        assert!(
            CallError::NoPeerService
                .to_string()
                .contains("does not serve")
        );
        assert_eq!(
            CallError::Remote("busy".into()).to_string(),
            "peer replied with error: busy"
        );
        assert!(CallError::Closed.to_string().contains("stopped"));
        assert!(
            CallError::Link(Error::Full)
                .to_string()
                .contains("tx buffer full")
        );
    }
}
