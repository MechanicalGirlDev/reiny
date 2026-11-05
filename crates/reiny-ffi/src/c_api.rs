//! Handle-based C bridge. Pointer contracts are documented in `include/reiny.h`.
use crate::{LocalBus, Message, Publisher, Session, Subscription};
use std::{ffi::c_char, sync::Arc};

mod arguments;
mod objects;
use arguments::{ERROR, boundary, bytes, optional_text, output, text};
pub(crate) use objects::{Object, get, insert, remove, replace_reserved};

type Result<T> = std::result::Result<T, String>;

macro_rules! typed {
    ($name:ident, $variant:ident, $ty:ty) => {
        fn $name(id: u64) -> Result<Arc<$ty>> {
            match get(id)? {
                Object::$variant(value) => Ok(value),
                _ => Err(concat!("wrong handle type: expected ", stringify!($variant)).into()),
            }
        }
    };
}
typed!(bus, Bus, LocalBus);
typed!(session, Session, Session);
typed!(publisher, Publisher, Publisher);
typed!(subscription, Subscription, Subscription);
typed!(message, Message, Message);
typed!(buffer, Buffer, Vec<u8>);

/// Read the thread-local UTF-8 error until the next bridge call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn reiny_last_error() -> *const c_char {
    // No facade operation or allocation occurs here.
    ERROR.with(|slot| slot.borrow().as_ptr())
}

/// Release one owned handle. Released and unknown handles are errors.
#[unsafe(no_mangle)]
pub extern "C" fn reiny_release(handle: u64) -> i32 {
    boundary(|| {
        let object = remove(handle, false)?;
        // Drop outside the registry lock: destructors may block or panic.
        drop(object);
        Ok(())
    })
}

// All exported pointer-taking functions are unsafe in Rust: callers must obey
// the header's readability, alignment, lifetime, and writable-output contracts.
macro_rules! export {
    ($name:ident($($arg:ident: $ty:ty),*) $body:block) => {
        /// # Safety
        /// Callers must obey the input and output pointer contracts in reiny.h.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name($($arg: $ty),*) -> i32 {
            boundary(|| {
                // SAFETY: the C caller guarantees the header pointer contracts;
                // the body checks NULL before reading/writing any pointer.
                unsafe { $body }
            })
        }
    };
}

export!(reiny_local_bus_new(out: *mut u64) {
    output(out)?;
    *out = insert(Object::Bus(LocalBus::new()))?;
    Ok(())
});
export!(reiny_local_bus_connect(handle: u64, id: *const c_char, domain: *const c_char, bindings: *const c_char, out: *mut u64) {
    output(out)?;
    let value = bus(handle)?.connect(text(id)?, text(domain)?, text(bindings)?).map_err(|e| e.to_string())?;
    *out = insert(Object::Session(value))?;
    Ok(())
});
export!(reiny_session_open(id: *const c_char, domain: *const c_char, config: *const c_char, bindings: *const c_char, out: *mut u64) {
    output(out)?;
    let value = Session::open(text(id)?, text(domain)?, optional_text(config)?, text(bindings)?).map_err(|e| e.to_string())?;
    *out = insert(Object::Session(value))?;
    Ok(())
});
export!(reiny_session_publisher(handle: u64, name: *const c_char, type_name: *const c_char, schema: u64, out: *mut u64) {
    output(out)?;
    let value = session(handle)?.publisher(&text(name)?, &text(type_name)?, schema).map_err(|e| e.to_string())?;
    *out = insert(Object::Publisher(value))?;
    Ok(())
});
export!(reiny_session_subscriber(handle: u64, name: *const c_char, type_name: *const c_char, schema: u64, out: *mut u64) {
    output(out)?;
    let value = session(handle)?.subscriber(&text(name)?, &text(type_name)?, schema).map_err(|e| e.to_string())?;
    *out = insert(Object::Subscription(value))?;
    Ok(())
});
/// Validate declared ports and announce module readiness.
#[unsafe(no_mangle)]
pub extern "C" fn reiny_session_ready(handle: u64) -> i32 {
    boundary(|| session(handle)?.ready().map_err(|e| e.to_string()))
}

/// Request cooperative shutdown on a session handle.
#[unsafe(no_mangle)]
pub extern "C" fn reiny_session_shutdown(handle: u64) -> i32 {
    boundary(|| {
        session(handle)?.shutdown();
        Ok(())
    })
}

export!(reiny_publisher_send(handle: u64, data: *const u8, len: usize) {
    publisher(handle)?.send(bytes(data, len)?).map_err(|e| e.to_string())
});
export!(reiny_subscription_receive(handle: u64, timeout: u64, out: *mut u64) {
    output(out)?;
    let value = subscription(handle)?.receive(timeout).map_err(|e| e.to_string())?;
    *out = match value {
        Some(value) => insert(Object::Message(Arc::new(value)))?,
        None => 0,
    };
    Ok(())
});
export!(reiny_message_payload(handle: u64, out: *mut u64) {
    output(out)?;
    *out = insert(Object::Buffer(Arc::new(message(handle)?.payload.clone())))?;
    Ok(())
});
export!(reiny_message_source(handle: u64, out: *mut u64) {
    output(out)?;
    *out = insert(Object::Buffer(Arc::new(message(handle)?.source.as_bytes().to_vec())))?;
    Ok(())
});
export!(reiny_message_source_port(handle: u64, out: *mut u64) {
    output(out)?;
    *out = insert(Object::Buffer(Arc::new(message(handle)?.source_port.as_bytes().to_vec())))?;
    Ok(())
});
export!(reiny_message_schema(handle: u64, has: *mut u8, value: *mut u64) {
    output(has)?; output(value)?;
    let schema = message(handle)?.schema;
    *has = u8::from(schema.is_some()); *value = schema.unwrap_or(0);
    Ok(())
});
export!(reiny_message_timestamp(handle: u64, has: *mut u8, value: *mut u64) {
    output(has)?; output(value)?;
    let timestamp = message(handle)?.timestamp;
    *has = u8::from(timestamp.is_some()); *value = timestamp.unwrap_or(0);
    Ok(())
});
export!(reiny_buffer_len(handle: u64, out: *mut usize) {
    output(out)?;
    *out = buffer(handle)?.len();
    Ok(())
});
export!(reiny_buffer_data(handle: u64, out: *mut *const u8) {
    output(out)?;
    *out = buffer(handle)?.as_ptr();
    Ok(())
});

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests;
