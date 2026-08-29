//! The zenoh engine — what `Cloudy` used to do directly up to 0.4, moved into [`Engine`]'s shape.
//! The wire (key shape, attachments, verbatim chunks) is unchanged from 0.4.

use std::any::Any;
use std::time::{Duration, UNIX_EPOCH};

use zenoh::handlers::FifoChannelHandler;
use zenoh::qos::{CongestionControl, Priority as ZPriority};
use zenoh::query::{ConsolidationMode, Query, Reply};
use zenoh::sample::{Sample as ZSample, SampleKind};
use zenoh::{Session, Wait};

use super::{
    BoxFuture, Callback, Caps, Engine, Guard, Key, Presence, QueryCallback, QueryParams,
    RawPublisher, RawQuery, RawReplies, ReplyResult, Sample,
};
use crate::{Priority, Qos, Reliability, Result};

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
        // zenoh's `#[internal_trait]` also grows the QoS setters as inherent methods, so they are
        // callable without importing `QoSBuilderTrait` (= without opening the `internal` feature).
        let publisher = self
            .session
            .declare_publisher(key.to_string())
            .priority(zenoh_priority(qos.priority))
            .congestion_control(zenoh_congestion(qos.reliability))
            .express(qos.express)
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Box::new(ZenohPublisher { publisher }))
    }

    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard> {
        let subscriber = self
            .session
            .declare_subscriber(key.to_string())
            .callback(move |sample: ZSample| {
                if sample.kind() != SampleKind::Put {
                    return;
                }
                if let Some(sample) = convert(&sample) {
                    on_sample(sample);
                }
            })
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Box::new(subscriber))
    }

    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        let token = self
            .session
            .liveliness()
            .declare_token(key.to_string())
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Box::new(token))
    }

    fn alive(&self, key: &Key, timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        let key = key.to_string();
        Box::pin(async move {
            let replies = self
                .session
                .liveliness()
                .get(key)
                .timeout(timeout)
                .await
                .map_err(anyhow::Error::msg)?;
            let mut keys = Vec::new();
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result()
                    && let Some(key) = Key::parse(sample.key_expr().as_str())
                {
                    keys.push(key);
                }
            }
            Ok(keys)
        })
    }

    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard> {
        let subscriber = self
            .session
            .liveliness()
            .declare_subscriber(key.to_string())
            .history(true)
            .callback(move |sample: ZSample| {
                let Some(key) = Key::parse(sample.key_expr().as_str()) else {
                    return;
                };
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
        let queryable = self
            .session
            .declare_queryable(key.to_string())
            .callback(move |query: Query| {
                let Some(key) = Key::parse(query.key_expr().as_str()) else {
                    return; // drop = finalize
                };
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
        // Consolidation off: reiny's replies differ per key, and two replies on the same key are not
        // "duplicates" but "different answers" (services).
        let mut get = self
            .session
            .get(key.to_string())
            .timeout(params.timeout)
            .consolidation(ConsolidationMode::None);
        if let Some(payload) = params.payload {
            get = get.payload(payload);
        }
        if let Some(attachment) = params.attachment {
            get = get.attachment(attachment);
        }
        let replies = get.wait().map_err(anyhow::Error::msg)?;
        Ok(Box::new(ZenohReplies { replies }))
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
    replies: FifoChannelHandler<Reply>,
}

impl RawReplies for ZenohReplies {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(async move {
            loop {
                // flume's `recv_async` is cancel-safe (no taken value is held across an await point).
                let reply = self.replies.recv_async().await.ok()?;
                match reply.result() {
                    Ok(sample) => {
                        if let Some(sample) = convert(sample) {
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

/// `Reliability` lowers to zenoh's `congestion_control` — "drop or wait" under congestion is the only
/// knob that actually takes effect. zenoh's own `reliability()` is a marker that retransmits nothing,
/// and is still `unstable` in 1.10 (`docs/design/0.5.0.md` §2.3).
fn zenoh_congestion(reliability: Reliability) -> CongestionControl {
    match reliability {
        Reliability::BestEffort => CongestionControl::Drop,
        Reliability::Reliable => CongestionControl::Block,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_is_zenoh_default_and_reliable_blocks() {
        assert_eq!(zenoh_priority(Priority::Normal), ZPriority::DEFAULT);
        assert_eq!(
            zenoh_congestion(Reliability::Reliable),
            CongestionControl::Block
        );
        assert_eq!(
            zenoh_congestion(Reliability::BestEffort),
            CongestionControl::Drop
        );
    }
}
