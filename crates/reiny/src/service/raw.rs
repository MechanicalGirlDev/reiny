//! Shared encoded query machinery for declared raw and typed services.

use super::{CallError, DEFAULT_TIMEOUT, ENGINE_TIMEOUT_MARGIN, QUERY_CAPACITY, reply_err};
use crate::engine::{Engine, Guard, Key, QueryParams, RawQuery, SERVICE_CHUNK, Sample};
use crate::managed::ManagedPort;
use crate::pubsub::{attachment_fingerprint, fingerprint};
use crate::shutdown::Shutdown;
use crate::{ModuleContext, Result};
use std::sync::Arc;
use std::time::Duration;

pub(super) struct ClientCore {
    pub(super) engine: Arc<dyn Engine>,
    pub(super) key: Key,
    pub(super) request_schema: u64,
    pub(super) response_schema: u64,
}

impl ClientCore {
    pub(super) async fn call(
        &self,
        payload: Vec<u8>,
        timeout: Duration,
        exact_target: bool,
    ) -> std::result::Result<Sample, CallError> {
        // The engine's expiry looks like NoReply, so our deadline must fire first.
        let engine_timeout = timeout.checked_add(ENGINE_TIMEOUT_MARGIN).ok_or_else(|| {
            CallError::Engine(anyhow::anyhow!(
                "RPC timeout exceeds the supported duration"
            ))
        })?;
        let params = QueryParams {
            payload: Some(payload),
            attachment: fingerprint(Some(self.request_schema)),
            timeout: engine_timeout,
        };
        let mut replies = self
            .engine
            .query(&self.key, params)
            .map_err(CallError::Engine)?;
        let Some(reply) = tokio::time::timeout(timeout, replies.next())
            .await
            .map_err(|_| CallError::Timeout)?
        else {
            return Err(CallError::NoReply);
        };
        let sample = reply
            .map_err(|message| CallError::Remote(String::from_utf8_lossy(&message).into_owned()))?;
        if exact_target && self.key != sample.key {
            return Err(CallError::Engine(anyhow::anyhow!(
                "response came from an undeclared target"
            )));
        }
        let Some(received) = attachment_fingerprint(sample.attachment.as_deref()) else {
            return Err(CallError::MissingSchema);
        };
        if self.response_schema != received {
            return Err(CallError::Schema {
                expected: self.response_schema,
                received,
            });
        }
        Ok(sample)
    }
}

pub(super) struct ServerCore {
    rx: flume::Receiver<Box<dyn RawQuery>>,
    _guard: Guard,
    _token: Guard,
    pub(super) key: Key,
    shutdown: Shutdown,
    request_schema: u64,
    response_schema: u64,
}

impl ServerCore {
    pub(super) fn declare(
        context: &ModuleContext,
        name: &str,
        request_type: &str,
        request_schema: u64,
        response_schema: u64,
    ) -> Result<Self> {
        let engine = &context.engine;
        let caps = engine.caps();
        anyhow::ensure!(
            caps.query && caps.liveliness,
            "server of {request_type}: this engine has no query / liveliness, which services need"
        );
        let key = Key::output(
            context.domain(),
            context.endpoint_namespace(),
            name,
            request_type,
        );
        let (tx, rx) = flume::bounded(QUERY_CAPACITY);
        let guard = engine.respond(
            &key,
            Box::new(move |query| {
                let _ = tx.send(query);
            }),
        )?;
        let token = engine.declare_alive(&key.with_chunk(SERVICE_CHUNK))?;
        Ok(Self {
            rx,
            _guard: guard,
            _token: token,
            key,
            shutdown: context.shutdown_handle(),
            request_schema,
            response_schema,
        })
    }

    pub(super) async fn recv(&mut self) -> Option<DeclaredRequest> {
        loop {
            let query = tokio::select! {
                biased;
                () = self.shutdown.wait() => return None,
                query = self.rx.recv_async() => query.ok()?,
            };
            if query.payload().is_none() {
                continue;
            }
            if attachment_fingerprint(query.attachment()) != Some(self.request_schema) {
                reply_err(
                    query,
                    "missing or mismatching request schema fingerprint".into(),
                );
                continue;
            }
            return Some(DeclaredRequest {
                query,
                key: self.key.clone(),
                response_schema: self.response_schema,
            });
        }
    }
}

/// A declared encoded RPC caller with one exact configured server.
pub struct DeclaredRpcClient {
    pub(super) core: ClientCore,
    pub(crate) managed: Option<ManagedPort>,
}

impl DeclaredRpcClient {
    pub(crate) fn declare(
        context: &ModuleContext,
        target: &crate::bindings::EndpointAddress,
        request: &crate::encoded::EncodedType,
        response: &crate::encoded::EncodedType,
    ) -> Self {
        Self {
            core: ClientCore {
                engine: Arc::clone(&context.engine),
                key: Key::output(
                    context.domain(),
                    &target.namespace,
                    &target.port,
                    request.type_name(),
                ),
                request_schema: request.fingerprint(),
                response_schema: response.fingerprint(),
            },
            managed: None,
        }
    }

    /// Send opaque protobuf bytes and return a schema-checked reply from the declared source.
    pub async fn call(&self, payload: Vec<u8>) -> std::result::Result<Vec<u8>, CallError> {
        self.call_with_timeout(payload, DEFAULT_TIMEOUT).await
    }

    /// Call once with an explicit wait budget. No automatic retry is performed.
    ///
    /// Dropping this future before its first poll sends nothing. After dispatch,
    /// dropping it or reaching the deadline abandons observation, not server execution.
    pub async fn call_with_timeout(
        &self,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> std::result::Result<Vec<u8>, CallError> {
        self.core
            .call(payload, timeout, true)
            .await
            .map(|sample| sample.payload)
    }
}

/// A declared RPC server receiving opaque protobuf request bytes.
pub struct DeclaredRpcServer {
    core: ServerCore,
    pub(crate) managed: Option<ManagedPort>,
}

impl DeclaredRpcServer {
    pub(crate) fn declare(
        context: &ModuleContext,
        name: &str,
        request: &crate::encoded::EncodedType,
        response: &crate::encoded::EncodedType,
    ) -> Result<Self> {
        Ok(Self {
            core: ServerCore::declare(
                context,
                name,
                request.type_name(),
                request.fingerprint(),
                response.fingerprint(),
            )?,
            managed: None,
        })
    }

    /// Receive a fingerprint-checked request, or `None` after shutdown.
    /// Payload-less retention queries are finalized without becoming requests.
    pub async fn recv(&mut self) -> Option<DeclaredRequest> {
        self.core.recv().await
    }
}

/// One received encoded request. Dropping it unanswered finalizes the query.
pub struct DeclaredRequest {
    query: Box<dyn RawQuery>,
    key: Key,
    response_schema: u64,
}

impl DeclaredRequest {
    /// The opaque request snapshot. An empty protobuf message is a valid payload.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.query.payload().unwrap_or_default()
    }

    /// Consume this request with one reply carrying the declared response fingerprint.
    pub async fn reply(self, payload: Vec<u8>) -> Result<()> {
        std::future::ready(self.query.reply(
            &self.key,
            payload,
            fingerprint(Some(self.response_schema)),
        ))
        .await
    }

    /// Consume this request with an application error, distinct from no reply.
    pub async fn reply_err(self, message: impl Into<String>) -> Result<()> {
        std::future::ready(self.query.reply_err(message.into().into_bytes())).await
    }
}
