//! Public managed-port coverage for native delivery, not a private-channel mock.

use super::*;
use crate::bindings::{ReceiveBuffer, Replay, Retention};
use crate::engine::Payload;
use std::sync::atomic::AtomicU32;

mod compatibility;
mod live;
mod replay;

/// A real protobuf implementation with per-value instrumentation, no global
/// counters or Clone implementation. Flavor changes Rust identity, not schema.
#[derive(Debug, Default)]
struct Counted<const FLAVOR: u8 = 0> {
    value: Arc<AtomicU32>,
    encodes: Arc<AtomicU64>,
    decoded_fields: u64,
}

impl<const FLAVOR: u8> Counted<FLAVOR> {
    fn new(value: u32) -> Self {
        Self {
            value: Arc::new(AtomicU32::new(value)),
            ..Self::default()
        }
    }

    fn value(&self) -> u32 {
        self.value.load(Ordering::Relaxed)
    }

    fn encodes(&self) -> u64 {
        self.encodes.load(Ordering::Relaxed)
    }
}

impl<const FLAVOR: u8> MessageType for Counted<FLAVOR> {
    const TYPE_NAME: &'static str = Probe::TYPE_NAME;
    const SCHEMA_FINGERPRINT: u64 = Probe::SCHEMA_FINGERPRINT;
    const DESCRIPTOR: Option<Descriptor> = None;
}

impl<const FLAVOR: u8> Message for Counted<FLAVOR> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        self.encodes.fetch_add(1, Ordering::Relaxed);
        Probe {
            value: self.value(),
        }
        .encode_raw(buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> std::result::Result<(), prost::DecodeError> {
        self.decoded_fields += 1;
        let mut wire = Probe {
            value: self.value(),
        };
        wire.merge_field(tag, wire_type, buf, ctx)?;
        self.value.store(wire.value, Ordering::Relaxed);
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        Probe {
            value: self.value(),
        }
        .encoded_len()
    }

    fn clear(&mut self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

async fn sender(bus: Arc<dyn Engine>, retained: bool) -> ModuleContext {
    let mut binding = bindings(SENDER);
    for name in ["outgoing", "other"] {
        binding.outputs.insert(
            name.into(),
            OutputBinding {
                contract: EndpointContract {
                    retention: if retained {
                        Retention::Last
                    } else {
                        Retention::Volatile
                    },
                    ..contract(Counted::<0>::TYPE_NAME)
                },
            },
        );
    }
    ModuleContext::open(options(bus, binding))
        .await
        .expect("sender")
}

async fn receiver(bus: Arc<dyn Engine>, replay: bool, buffer: ReceiveBuffer) -> ModuleContext {
    let mut binding = bindings(RECEIVER);
    for name in ["first", "second", "raw", "owned", "twin"] {
        let mut incoming = input(SENDER);
        incoming.contract.replay = if replay { Replay::Last } else { Replay::Live };
        incoming.contract.buffer = buffer;
        binding.inputs.insert(name.into(), incoming);
    }
    ModuleContext::open(options(bus, binding))
        .await
        .expect("receiver")
}

async fn barrier(bus: &dyn Engine) {
    timeout(PATIENCE, bus.alive(&Key::launch(DOMAIN, None), PATIENCE))
        .await
        .expect("dispatcher barrier")
        .expect("alive");
}

fn key() -> Key {
    Key::output(DOMAIN, SENDER, "outgoing", Counted::<0>::TYPE_NAME)
}

fn stamp() -> Vec<u8> {
    Counted::<0>::SCHEMA_FINGERPRINT.to_le_bytes().to_vec()
}
