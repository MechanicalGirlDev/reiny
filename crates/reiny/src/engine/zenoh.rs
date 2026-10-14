//! The zenoh engine — what `ModuleContext` used to do directly up to 0.4, moved into [`Engine`]'s shape.
//! Named v3 keys preserve endpoint identity; old epochs and malformed selectors are rejected.

use std::any::Any;
use std::collections::VecDeque;
use std::time::{Duration, UNIX_EPOCH};

use zenoh::handlers::FifoChannelHandler;
use zenoh::qos::{CongestionControl as ZCongestionControl, Priority as ZPriority};
use zenoh::query::{ConsolidationMode, Query, Reply};
use zenoh::sample::{Sample as ZSample, SampleKind};
use zenoh::{Session, Wait};

use super::{
    BoxFuture, Callback, Caps, Engine, Guard, Key, Presence, QueryCallback, QueryParams,
    RawPublisher, RawQuery, RawReplies, ReplyResult, Sample,
};
use crate::{CongestionControl, Priority, Qos, Result};

/// A zenoh session as an [`Engine`].
pub struct Zenoh {
    session: Session,
}

impl Zenoh {
    /// Open a session from a configuration.
    pub async fn open(config: zenoh::Config) -> Result<Self> {
        let session = zenoh::open(config).await.map_err(anyhow::Error::msg)?;
        Ok(Self { session })
    }

    /// Wrap a session that is already open.
    #[must_use]
    pub fn from_session(session: Session) -> Self {
        Self { session }
    }

    /// The session inside. The escape hatch to zenoh features reiny does not wrap.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }
}

impl Engine for Zenoh {
    fn caps(&self) -> Caps {
        Caps::ALL
    }

    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        key.validate()?;
        // zenoh's `#[internal_trait]` also grows the QoS setters as inherent methods, so they are
        // callable without importing `QoSBuilderTrait` (= without opening the `internal` feature).
        let mut publisher = self
            .session
            .declare_publisher(key.to_string())
            .priority(zenoh_priority(qos.priority))
            .express(qos.express);
        if let Some(congestion) = zenoh_congestion(qos.congestion_control) {
            publisher = publisher.congestion_control(congestion);
        }
        let publisher = publisher.wait().map_err(anyhow::Error::msg)?;
        Ok(Box::new(ZenohPublisher { publisher }))
    }

    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard> {
        key.validate()?;
        let pattern = key.clone();
        let subscriber = self
            .session
            .declare_subscriber(key.to_string())
            .callback(move |sample: ZSample| {
                if sample.kind() != SampleKind::Put {
                    return;
                }
                if let Some(sample) = convert(&sample)
                    && pattern.matches(&sample.key)
                {
                    on_sample(sample);
                }
            })
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Box::new(subscriber))
    }

    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        key.validate()?;
        let token = self
            .session
            .liveliness()
            .declare_token(key.to_string())
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Box::new(token))
    }

    fn alive(&self, key: &Key, timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        let pattern = key.clone();
        Box::pin(async move {
            pattern.validate()?;
            let replies = self
                .session
                .liveliness()
                .get(pattern.to_string())
                .timeout(timeout)
                .await
                .map_err(anyhow::Error::msg)?;
            let mut keys = Vec::new();
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result()
                    && let Some(key) = Key::parse(sample.key_expr().as_str())
                    && pattern.matches(&key)
                {
                    keys.push(key);
                }
            }
            Ok(keys)
        })
    }

    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard> {
        key.validate()?;
        let pattern = key.clone();
        let subscriber = self
            .session
            .liveliness()
            .declare_subscriber(key.to_string())
            .history(true)
            .callback(move |sample: ZSample| {
                let Some(key) = Key::parse(sample.key_expr().as_str()) else {
                    return;
                };
                if !pattern.matches(&key) {
                    return;
                }
                on_event(match sample.kind() {
                    SampleKind::Put => Presence::Joined(key),
                    SampleKind::Delete => Presence::Left(key),
                });
            })
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Box::new(subscriber))
    }

    fn respond(&self, key: &Key, on_query: QueryCallback) -> Result<Guard> {
        key.validate()?;
        let responder = key.clone();
        let queryable = self
            .session
            .declare_queryable(key.to_string())
            .callback(move |query: Query| {
                let Some(key) = Key::parse(query.key_expr().as_str()) else {
                    return;
                };
                if !key.matches(&responder) {
                    return; // **/* may also intersect an ordinary segment inside a chunk.
                }
                let payload = query.payload().map(|p| p.to_bytes().into_owned());
                let attachment = query.attachment().map(|a| a.to_bytes().into_owned());
                on_query(Box::new(ZenohQuery {
                    query,
                    key,
                    payload,
                    attachment,
                }));
            })
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Box::new(queryable))
    }

    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        key.validate()?;
        // Consolidation off: reiny's replies differ per key, and two replies on the same key are not
        // "duplicates" but "different answers" (services).
        let mut selectors = vec![key.to_string()];
        // Schema introspection spans both named input and output identities. Verbatim
        // direction slots are deliberately not covered by Zenoh's ordinary wildcards.
        if key.endpoint.is_none()
            && key
                .chunk
                .as_deref()
                .is_some_and(|chunk| chunk.starts_with("@schema/"))
        {
            selectors.push(key.to_string().replace("/@out/", "/@in/"));
        }
        let mut replies = VecDeque::new();
        for selector in selectors {
            let mut get = self
                .session
                .get(selector)
                .timeout(params.timeout)
                .consolidation(ConsolidationMode::None);
            if let Some(payload) = &params.payload {
                get = get.payload(payload.clone());
            }
            if let Some(attachment) = &params.attachment {
                get = get.attachment(attachment.clone());
            }
            replies.push_back(get.wait().map_err(anyhow::Error::msg)?);
        }
        Ok(Box::new(ZenohReplies {
            replies,
            pattern: key.clone(),
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct ZenohPublisher {
    publisher: zenoh::pubsub::Publisher<'static>,
}

impl RawPublisher for ZenohPublisher {
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()> {
        // The attachment setter likewise uses the inherent `#[internal_trait]` method (no trait import).
        let mut put = self.publisher.put(payload);
        if let Some(attachment) = attachment {
            put = put.attachment(attachment);
        }
        put.wait().map_err(anyhow::Error::msg)
    }
}

struct ZenohQuery {
    query: Query,
    key: Key,
    payload: Option<Vec<u8>>,
    attachment: Option<Vec<u8>>,
}

impl RawQuery for ZenohQuery {
    fn key(&self) -> &Key {
        &self.key
    }

    fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    fn attachment(&self) -> Option<&[u8]> {
        self.attachment.as_deref()
    }

    fn reply(
        self: Box<Self>,
        key: &Key,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> Result<()> {
        key.validate()?;
        let mut reply = self.query.reply(key.to_string(), payload);
        if let Some(attachment) = attachment {
            reply = reply.attachment(attachment);
        }
        reply.wait().map_err(anyhow::Error::msg)
    }

    fn reply_err(self: Box<Self>, message: Vec<u8>) -> Result<()> {
        self.query
            .reply_err(message)
            .wait()
            .map_err(anyhow::Error::msg)
    }
}

struct ZenohReplies {
    replies: VecDeque<FifoChannelHandler<Reply>>,
    pattern: Key,
}

impl RawReplies for ZenohReplies {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(async move {
            loop {
                // flume's `recv_async` is cancel-safe (no taken value is held across an await point).
                let replies = self.replies.front()?;
                let Ok(reply) = replies.recv_async().await else {
                    self.replies.pop_front();
                    continue;
                };
                match reply.result() {
                    Ok(sample) => {
                        if let Some(sample) = convert(sample)
                            && self.pattern.matches(&sample.key)
                        {
                            return Some(Ok(sample));
                        }
                    }
                    Err(e) => return Some(Err(e.payload().to_bytes().into_owned())),
                }
            }
        })
    }
}

/// A zenoh sample in the engine's shape. `None` when the key is not reiny's (somebody else's sample).
fn convert(sample: &ZSample) -> Option<Sample> {
    Some(Sample {
        key: Key::parse(sample.key_expr().as_str())?,
        payload: sample.payload().to_bytes().into_owned(),
        attachment: sample.attachment().map(|a| a.to_bytes().into_owned()),
        timestamp: sample.timestamp().and_then(unix_ns),
    })
}

fn unix_ns(timestamp: &zenoh::time::Timestamp) -> Option<u64> {
    timestamp
        .get_time()
        .to_system_time()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_nanos()).ok())
}

/// reiny's five levels onto zenoh's seven. `Normal` = zenoh's default (`Data`).
fn zenoh_priority(priority: Priority) -> ZPriority {
    match priority {
        Priority::RealTime => ZPriority::RealTime,
        Priority::High => ZPriority::InteractiveHigh,
        Priority::Normal => ZPriority::Data,
        Priority::Low => ZPriority::DataLow,
        Priority::Background => ZPriority::Background,
    }
}

/// Preserve backend defaults (including session configuration) unless explicitly selected.
fn zenoh_congestion(congestion: CongestionControl) -> Option<ZCongestionControl> {
    match congestion {
        CongestionControl::BackendDefault => None,
        CongestionControl::Drop => Some(ZCongestionControl::Drop),
        CongestionControl::Block => Some(ZCongestionControl::Block),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn congestion_maps_explicit_policies_and_delegates_default() {
        assert_eq!(zenoh_priority(Priority::Normal), ZPriority::DEFAULT);
        assert_eq!(
            zenoh_congestion(CongestionControl::Block),
            Some(ZCongestionControl::Block)
        );
        assert_eq!(
            zenoh_congestion(CongestionControl::Drop),
            Some(ZCongestionControl::Drop)
        );
        assert_eq!(zenoh_congestion(CongestionControl::BackendDefault), None);
    }
}
