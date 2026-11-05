//! Real Local/Zenoh regression fixtures. Every wait is an event or a bounded deadline.

use super::*;
use anyhow::Context;
use prost::Message;
use tokio::time::timeout;
use zenoh::Wait;

mod delivery;
mod presence;
mod queries;
mod sdk;

const WAIT: Duration = Duration::from_secs(5);
const DOMAIN: &str = "routed";
const A: &str = "robot/arm";
const B: &str = "robot/controller";
const C: &str = "robot/monitor";

#[derive(Clone, PartialEq, Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

impl crate::MessageType for Probe {
    const TYPE_NAME: &'static str = "Probe";
    const SCHEMA_FINGERPRINT: u64 = 0x1717;
    const DESCRIPTOR: Option<crate::Descriptor> = None;
}

struct Pair {
    a: Arc<Routed>,
    b: Arc<Routed>,
    left: zenoh::Session,
    right: zenoh::Session,
}

impl Pair {
    async fn open(port: u16) -> Result<Self> {
        let mut sessions = Vec::new();
        for path in ["listen/endpoints", "connect/endpoints"] {
            let mut config = zenoh::Config::default();
            config
                .insert_json5("scouting/multicast/enabled", "false")
                .map_err(anyhow::Error::msg)?;
            config
                .insert_json5(path, &format!(r#"["tcp/127.0.0.1:{port}"]"#))
                .map_err(anyhow::Error::msg)?;
            sessions.push(
                timeout(WAIT, zenoh::open(config))
                    .await?
                    .map_err(anyhow::Error::msg)?,
            );
        }
        let right = sessions.pop().context("right session")?;
        let left = sessions.pop().context("left session")?;
        let placement = BTreeMap::from([
            (A.into(), "left".into()),
            (B.into(), "right".into()),
            (C.into(), "left".into()),
        ]);
        let a = Arc::new(Routed::new(
            "left",
            placement.clone(),
            Zenoh::from_session(left.clone()),
        )?);
        let b = Arc::new(Routed::new(
            "right",
            placement,
            Zenoh::from_session(right.clone()),
        )?);
        Ok(Self { a, b, left, right })
    }

    async fn close(self) -> Result<()> {
        drop((self.a, self.b));
        timeout(WAIT, self.right.close())
            .await?
            .map_err(anyhow::Error::msg)?;
        timeout(WAIT, self.left.close())
            .await?
            .map_err(anyhow::Error::msg)?;
        Ok(())
    }
}

fn key(source: &str) -> Key {
    Key::output(DOMAIN, source, "state", "Probe")
}

fn collector<T: Send + 'static>() -> (Callback<T>, flume::Receiver<T>) {
    let (tx, rx) = flume::unbounded();
    (
        Box::new(move |value| {
            forward(&tx, value);
        }),
        rx,
    )
}

fn forward<T>(sender: &flume::Sender<T>, value: T) {
    // An in-flight callback may finish after its receiver is dropped during teardown.
    match sender.send(value) {
        Ok(()) => {}
        Err(flume::SendError(value)) => drop(value),
    }
}

async fn recv<T>(rx: &flume::Receiver<T>) -> Result<T> {
    Ok(timeout(WAIT, rx.recv_async()).await??)
}

fn subscription_signal(
    session: &zenoh::Session,
    key: &Key,
) -> Result<(Guard, flume::Receiver<bool>)> {
    let publisher = session
        .declare_publisher(key.to_string())
        .wait()
        .map_err(anyhow::Error::msg)?;
    let (tx, rx) = flume::unbounded();
    let listener = publisher
        .matching_listener()
        .callback(move |status| {
            forward(&tx, status.matching());
        })
        .wait()
        .map_err(anyhow::Error::msg)?;
    Ok((Box::new((publisher, listener)), rx))
}

fn query_signal(session: &zenoh::Session, key: &Key) -> Result<(Guard, flume::Receiver<bool>)> {
    let querier = session
        .declare_querier(key.to_string())
        .wait()
        .map_err(anyhow::Error::msg)?;
    let (tx, rx) = flume::unbounded();
    let listener = querier
        .matching_listener()
        .callback(move |status| {
            forward(&tx, status.matching());
        })
        .wait()
        .map_err(anyhow::Error::msg)?;
    Ok((Box::new((querier, listener)), rx))
}

async fn matched(rx: &flume::Receiver<bool>, expected: bool) -> Result<()> {
    timeout(WAIT, async {
        while rx.recv_async().await? != expected {}
        Ok::<(), flume::RecvError>(())
    })
    .await??;
    Ok(())
}

fn params(payload: Option<Vec<u8>>) -> QueryParams {
    QueryParams {
        payload,
        attachment: Some(vec![17; 8]),
        timeout: WAIT,
    }
}

async fn replies(mut stream: Box<dyn RawReplies>) -> Result<Vec<DeliveryResult>> {
    Ok(timeout(WAIT, async {
        let mut result = Vec::new();
        while let Some(reply) = stream.next_delivery().await {
            result.push(reply);
        }
        result
    })
    .await?)
}

fn success(reply: &DeliveryResult) -> Result<&Delivery> {
    reply
        .as_ref()
        .map_err(|bytes| anyhow::anyhow!("unexpected reply error: {bytes:?}"))
}
