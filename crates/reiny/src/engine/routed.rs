//! Placement routes, not endpoint authority. Each host must use one shared engine and the
//! same immutable placement. Unknown namespaces fail rather than falling back to a bus.
//! Owned declarations and publications are visible on both transports, without atomicity:
//! a failed publication may already have reached one transport. Never retry it as a transaction.

use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use super::{
    BoxFuture, Callback, Caps, Delivery, DeliveryResult, Engine, Guard, Key, Local, Payload,
    Presence, QueryCallback, QueryParams, RawPublisher, RawReplies, ReplyResult, Sample, Zenoh,
};
use crate::{Qos, Result};

/// One host's Local fast path plus its Zenoh transport.
///
/// Placement only selects transport; existing named bindings still grant endpoint authority.
/// Wildcard reads cover exactly the supplied namespaces, never an open-ended network census.
/// Dropping returned handles releases their declarations on both transports.
pub struct Routed {
    host: String,
    placement: BTreeMap<String, String>,
    local: Local,
    remote: Zenoh,
}

impl Routed {
    /// Own a Zenoh engine and create the single Local bus for this host.
    ///
    /// # Errors
    /// Rejects malformed namespaces, empty host names, or a host with no placement.
    pub fn new(host: &str, placement: BTreeMap<String, String>, remote: Zenoh) -> Result<Self> {
        anyhow::ensure!(!host.trim().is_empty(), "routed: empty host");
        for (namespace, owner) in &placement {
            Key::launch("placement", Some(namespace)).validate()?;
            anyhow::ensure!(!owner.trim().is_empty(), "routed: empty placement host");
        }
        anyhow::ensure!(
            placement.values().any(|owner| owner == host),
            "routed: host has no assigned namespaces"
        );
        Ok(Self {
            host: host.into(),
            placement,
            local: Local::new(),
            remote,
        })
    }

    fn routes(&self, key: &Key, query: bool) -> Result<Vec<(&dyn Engine, Key)>> {
        key.validate()?;
        anyhow::ensure!(
            query
                || key.endpoint.is_some()
                || key
                    .chunk
                    .as_ref()
                    .is_none_or(|chunk| !chunk.starts_with("@schema/")),
            "routed: wildcard-direction schema streams are unsupported; use query"
        );
        let namespaces = match &key.source {
            Some(source) => vec![(
                source,
                self.placement
                    .get(source)
                    .ok_or_else(|| anyhow::anyhow!("routed: unplaced namespace '{source}'"))?,
            )],
            None => self.placement.iter().collect(),
        };
        Ok(namespaces
            .into_iter()
            .map(|(source, host)| {
                let engine: &dyn Engine = if host == &self.host {
                    &self.local
                } else {
                    &self.remote
                };
                (
                    engine,
                    Key {
                        source: Some(source.clone()),
                        ..key.clone()
                    },
                )
            })
            .collect())
    }

    fn owned(&self, key: &Key) -> Result<()> {
        key.validate()?;
        anyhow::ensure!(
            key.source
                .as_ref()
                .is_some_and(|source| self.placement.get(source) == Some(&self.host)),
            "routed: declaration requires a namespace assigned to this host"
        );
        anyhow::ensure!(
            !key.to_string().contains('*'),
            "routed: declarations require a concrete named address"
        );
        Ok(())
    }
}

impl Engine for Routed {
    fn caps(&self) -> Caps {
        let a = self.local.caps();
        let b = self.remote.caps();
        Caps {
            wildcard_source: a.wildcard_source && b.wildcard_source,
            liveliness: a.liveliness && b.liveliness,
            query: a.query && b.query,
            attachment: a.attachment && b.attachment,
            congestion_control: a.congestion_control && b.congestion_control,
        }
    }

    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        self.owned(key)?;
        Ok(Box::new(Publisher {
            local: self.local.publisher(key, qos)?,
            remote: self.remote.publisher(key, qos)?,
        }))
    }

    fn subscribe(&self, key: &Key, callback: Callback<Sample>) -> Result<Guard> {
        self.subscribe_delivery(key, Box::new(move |sample| callback(sample.into())))
    }

    fn subscribe_delivery(&self, key: &Key, callback: Callback<Delivery>) -> Result<Guard> {
        let callback = Arc::from(callback);
        let mut guards = Vec::new();
        for (engine, key) in self.routes(key, false)? {
            let callback = Arc::<dyn Fn(Delivery) + Send + Sync>::clone(&callback);
            guards.push(engine.subscribe_delivery(&key, Box::new(move |sample| callback(sample)))?);
        }
        Ok(Box::new(guards))
    }

    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        self.owned(key)?;
        Ok(Box::new((
            self.local.declare_alive(key)?,
            self.remote.declare_alive(key)?,
        )))
    }

    fn alive(&self, key: &Key, timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        let routes = self.routes(key, false);
        Box::pin(async move {
            let routes = routes?;
            let mut pending: Vec<_> = routes
                .iter()
                .map(|(engine, key)| Some(engine.alive(key, timeout)))
                .collect();
            let mut keys = HashSet::new();
            poll_fn(|cx| {
                for future in &mut pending {
                    if let Some(f) = future {
                        match f.as_mut().poll(cx) {
                            Poll::Ready(Ok(found)) => {
                                keys.extend(found);
                                *future = None;
                            }
                            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                            Poll::Pending => {}
                        }
                    }
                }
                if pending.iter().all(Option::is_none) {
                    Poll::Ready(Ok(keys.drain().collect()))
                } else {
                    Poll::Pending
                }
            })
            .await
        })
    }

    fn watch_alive(&self, key: &Key, callback: Callback<Presence>) -> Result<Guard> {
        let callback = Arc::from(callback);
        let mut guards = Vec::new();
        for (engine, key) in self.routes(key, false)? {
            let callback = Arc::<dyn Fn(Presence) + Send + Sync>::clone(&callback);
            guards.push(engine.watch_alive(&key, Box::new(move |event| callback(event)))?);
        }
        Ok(Box::new(guards))
    }

    fn respond(&self, key: &Key, callback: QueryCallback) -> Result<Guard> {
        self.owned(key)?;
        let callback: Arc<dyn Fn(Box<dyn super::RawQuery>) + Send + Sync> = Arc::from(callback);
        let local = Arc::clone(&callback);
        Ok(Box::new((
            self.local
                .respond(key, Box::new(move |query| local(query)))?,
            self.remote
                .respond(key, Box::new(move |query| callback(query)))?,
        )))
    }

    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        let streams = self
            .routes(key, true)?
            .into_iter()
            .map(|(engine, key)| {
                engine
                    .query(&key, params.clone())
                    .map(|replies| (key, replies))
            })
            .collect::<Result<_>>()?;
        Ok(Box::new(Replies(streams)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct Publisher {
    local: Box<dyn RawPublisher>,
    remote: Box<dyn RawPublisher>,
}

impl RawPublisher for Publisher {
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()> {
        self.put_payload(payload.into(), attachment)
    }

    fn put_payload(&self, payload: Payload, attachment: Option<Vec<u8>>) -> Result<()> {
        let local = self.local.put_payload(payload.clone(), attachment.clone());
        let remote = self.remote.put_payload(payload, attachment);
        match (local, remote) {
            (Ok(()), Ok(())) => Ok(()),
            (local, remote) => anyhow::bail!(
                "routed: non-atomic publication; Local={local:?}; Zenoh={remote:?}; delivery may have occurred"
            ),
        }
    }
}

struct Replies(Vec<(Key, Box<dyn RawReplies>)>);

impl RawReplies for Replies {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(async move { Some(self.next_delivery().await?.map(Sample::from)) })
    }

    fn next_delivery(&mut self) -> BoxFuture<'_, Option<DeliveryResult>> {
        Box::pin(async move {
            while !self.0.is_empty() {
                let (index, reply) = {
                    let mut futures: Vec<_> = self
                        .0
                        .iter_mut()
                        .map(|(_, stream)| stream.next_delivery())
                        .collect();
                    poll_fn(|cx| {
                        for (index, future) in futures.iter_mut().enumerate() {
                            if let Poll::Ready(reply) = future.as_mut().poll(cx) {
                                return Poll::Ready((index, reply));
                            }
                        }
                        Poll::Pending
                    })
                    .await
                };
                match reply {
                    Some(Ok(delivery)) if !self.0[index].0.matches(&delivery.key) => {}
                    Some(reply) => return Some(reply),
                    None => {
                        self.0.remove(index);
                    }
                }
            }
            None
        })
    }
}

#[cfg(test)]
mod tests;
