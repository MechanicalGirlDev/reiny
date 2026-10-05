//! Handle-based C bridge. Pointer contracts are documented in `include/reiny.h`.
use crate::{LocalBus, Message, Publisher, Session, Subscription};
use std::{
    cell::RefCell,
    collections::HashMap,
    ffi::{CStr, CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Clone)]
enum Object {
    Bus(Arc<LocalBus>),
    Session(Arc<Session>),
    Publisher(Arc<Publisher>),
    Subscription(Arc<Subscription>),
    Message(Arc<Message>),
    List(Arc<Vec<String>>),
    Buffer(Arc<Vec<u8>>),
}

struct Registry {
    next: u64,
    objects: HashMap<u64, Object>,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        Mutex::new(Registry {
            next: 1,
            objects: HashMap::new(),
        })
    })
}

type Result<T> = std::result::Result<T, String>;

fn insert(object: Object) -> Result<u64> {
    let mut registry = registry().lock().map_err(|_| "handle registry poisoned")?;
    let id = registry.next;
    registry.next = id.checked_add(1).ok_or("handle space exhausted")?;
    registry.objects.insert(id, object);
    Ok(id)
}

fn get(id: u64) -> Result<Object> {
    registry()
        .lock()
        .map_err(|_| "handle registry poisoned")?
        .objects
        .get(&id)
        .cloned()
        .ok_or_else(|| "invalid or released handle".into())
}

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
typed!(list, List, Vec<String>);
typed!(buffer, Buffer, Vec<u8>);

thread_local! {
    static ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn boundary(f: impl FnOnce() -> Result<()>) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(f));
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(_) => Some("Rust panic caught at C boundary".into()),
    };
    ERROR.with(|slot| {
        #[expect(
            clippy::expect_used,
            reason = "embedded NULs were replaced before CString construction"
        )]
        let message = CString::new(error.as_deref().unwrap_or("").replace('\0', "\\0"))
            .expect("embedded NUL removed");
        *slot.borrow_mut() = message;
    });
    if error.is_some() { -1 } else { 0 }
}

fn output<T>(ptr: *mut T) -> Result<()> {
    if ptr.is_null() {
        Err("NULL output pointer".into())
    } else {
        Ok(())
    }
}

unsafe fn text(ptr: *const c_char) -> Result<String> {
    if ptr.is_null() {
        return Err("NULL required string".into());
    }
    // SAFETY: caller supplies a readable NUL-terminated string.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(str::to_owned)
        .map_err(|e| e.to_string())
}

unsafe fn optional_text(ptr: *const c_char) -> Result<Option<String>> {
    if ptr.is_null() {
        Ok(None)
    } else {
        // SAFETY: same caller contract as text.
        unsafe { text(ptr) }.map(Some)
    }
}

unsafe fn bytes(ptr: *const u8, len: usize) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err("NULL nonempty byte input".into());
    }
    if isize::try_from(len).is_err() {
        return Err("byte input exceeds isize::MAX".into());
    }
    // SAFETY: caller supplies len readable bytes in one allocation.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec())
}

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
        let object = registry()
            .lock()
            .map_err(|_| "handle registry poisoned")?
            .objects
            .remove(&handle)
            .ok_or("invalid or released handle")?;
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
export!(reiny_local_bus_connect(handle: u64, id: *const c_char, domain: *const c_char, out: *mut u64) {
    output(out)?;
    let value = bus(handle)?.connect(text(id)?, text(domain)?).map_err(|e| e.to_string())?;
    *out = insert(Object::Session(value))?;
    Ok(())
});
export!(reiny_session_open(id: *const c_char, domain: *const c_char, config: *const c_char, out: *mut u64) {
    output(out)?;
    let value = Session::open(text(id)?, text(domain)?, optional_text(config)?).map_err(|e| e.to_string())?;
    *out = insert(Object::Session(value))?;
    Ok(())
});
export!(reiny_session_publisher(handle: u64, topic: *const c_char, has_schema: u8, schema: u64, out: *mut u64) {
    output(out)?;
    if has_schema > 1 { return Err("has_schema must be 0 or 1".into()); }
    let value = session(handle)?.publisher(text(topic)?, (has_schema == 1).then_some(schema)).map_err(|e| e.to_string())?;
    *out = insert(Object::Publisher(value))?;
    Ok(())
});
export!(reiny_session_subscriber(handle: u64, topic: *const c_char, source: *const c_char, out: *mut u64) {
    output(out)?;
    let value = session(handle)?.subscriber(text(topic)?, optional_text(source)?).map_err(|e| e.to_string())?;
    *out = insert(Object::Subscription(value))?;
    Ok(())
});
export!(reiny_session_publishers(handle: u64, topic: *const c_char, timeout: u64, out: *mut u64) {
    output(out)?;
    let value = session(handle)?.publishers(text(topic)?, timeout).map_err(|e| e.to_string())?;
    *out = insert(Object::List(Arc::new(value)))?;
    Ok(())
});

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
export!(reiny_list_len(handle: u64, out: *mut usize) {
    output(out)?;
    *out = list(handle)?.len();
    Ok(())
});
export!(reiny_list_get(handle: u64, index: usize, out: *mut u64) {
    output(out)?;
    let values = list(handle)?;
    let value = values.get(index).ok_or("list index out of bounds")?;
    *out = insert(Object::Buffer(Arc::new(value.as_bytes().to_vec())))?;
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
mod tests {
    use super::*;

    #[test]
    fn registry_ownership_and_pointer_errors() {
        // SAFETY: all non-NULL pointers below reference live aligned test values;
        // dangling input is rejected by its length before dereferencing.
        unsafe {
            let mut bus = 0;
            assert_eq!(reiny_local_bus_new(&raw mut bus), 0);
            let mut len = 99;
            assert_eq!(reiny_buffer_len(bus, &raw mut len), -1);
            assert_eq!(len, 99);
            assert_eq!(reiny_release(bus), 0);
            assert_eq!(reiny_release(bus), -1);
            assert_eq!(reiny_release(0), -1);
            assert_eq!(reiny_local_bus_new(std::ptr::null_mut()), -1);
            assert!(text(std::ptr::null()).is_err());
            assert!(bytes(std::ptr::null(), 1).is_err());
            assert!(bytes(std::ptr::dangling(), isize::MAX.unsigned_abs() + 1).is_err());
            assert!(text([255u8, 0].as_ptr().cast()).is_err());
            assert_eq!(bytes(std::ptr::null(), 0).unwrap(), Vec::<u8>::new());
            let id = insert(Object::Buffer(Arc::new(vec![0, 255, 0]))).unwrap();
            let mut data = std::ptr::null();
            assert_eq!(reiny_buffer_len(id, &raw mut len), 0);
            assert_eq!(reiny_buffer_data(id, &raw mut data), 0);
            assert_eq!(std::slice::from_raw_parts(data, len), &[0, 255, 0]);
            assert_eq!(reiny_release(id), 0);
            let next = insert(Object::Buffer(Arc::new(vec![]))).unwrap();
            assert!(next > id);
            assert_eq!(reiny_release(next), 0);
        }
    }

    #[test]
    fn getters_return_independent_owned_buffers() {
        // SAFETY: output pointers refer to distinct live aligned local values.
        unsafe {
            let message = insert(Object::Message(Arc::new(Message {
                payload: vec![0, 255, 0],
                source: "sender".into(),
                schema: Some(42),
                timestamp: None,
            })))
            .unwrap();
            let mut payload = 0;
            let mut source = 0;
            let mut has = 99;
            let mut value = 99;
            assert_eq!(
                reiny_message_schema(message, &raw mut has, &raw mut value),
                0
            );
            assert_eq!((has, value), (1, 42));
            assert_eq!(
                reiny_message_timestamp(message, &raw mut has, &raw mut value),
                0
            );
            assert_eq!((has, value), (0, 0));
            assert_eq!(reiny_message_payload(message, &raw mut payload), 0);
            assert_eq!(reiny_message_source(message, &raw mut source), 0);
            assert_eq!(reiny_release(message), 0);
            assert_eq!(*buffer(payload).unwrap(), [0, 255, 0]);
            assert_eq!(*buffer(source).unwrap(), b"sender");
            assert_eq!(reiny_release(payload), 0);
            assert_eq!(reiny_release(source), 0);
            let list = insert(Object::List(Arc::new(vec!["sender".into()]))).unwrap();
            assert_eq!(reiny_list_get(list, 1, &raw mut source), -1);
            assert_eq!(reiny_list_get(list, 0, &raw mut source), 0);
            assert_eq!(reiny_release(list), 0);
            assert_eq!(*buffer(source).unwrap(), b"sender");
            assert_eq!(reiny_release(source), 0);
        }
    }

    #[test]
    fn panic_is_an_error() {
        assert_eq!(boundary(|| panic!("test panic")), -1);
        // SAFETY: the thread-local error remains valid until the next bridge call.
        unsafe {
            assert!(
                CStr::from_ptr(reiny_last_error())
                    .to_str()
                    .unwrap()
                    .contains("panic")
            );
        }
        assert_eq!(boundary(|| Ok(())), 0);
        // SAFETY: the error pointer is consumed before any further bridge call.
        unsafe {
            assert_eq!(CStr::from_ptr(reiny_last_error()).to_bytes(), b"");
        }
    }
}
