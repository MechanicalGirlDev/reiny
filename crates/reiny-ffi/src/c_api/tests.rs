use super::*;
use std::ffi::{CStr, CString};

#[test]
fn registry_ownership_and_pointer_errors() {
    // SAFETY: all non-NULL pointers below reference live aligned test values;
    // dangling input is rejected by its length before dereferencing.
    unsafe {
        let mut bus = 0;
        assert_eq!(reiny_local_bus_new(&raw mut bus), 0);
        let mut session = 99;
        let id = CString::new("sender").unwrap();
        let domain = CString::new("test").unwrap();
        assert_eq!(
            reiny_local_bus_connect(
                bus,
                id.as_ptr(),
                domain.as_ptr(),
                std::ptr::null(),
                &raw mut session,
            ),
            -1
        );
        assert_eq!(session, 99);
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
            source_port: "out".into(),
            schema: Some(42),
            timestamp: None,
        })))
        .unwrap();
        let mut payload = 0;
        let mut source = 0;
        let mut source_port = 0;
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
        assert_eq!(reiny_message_source_port(message, &raw mut source_port), 0);
        assert_eq!(reiny_release(message), 0);
        assert_eq!(*buffer(payload).unwrap(), [0, 255, 0]);
        assert_eq!(*buffer(source).unwrap(), b"sender");
        assert_eq!(*buffer(source_port).unwrap(), b"out");
        assert_eq!(reiny_release(payload), 0);
        assert_eq!(reiny_release(source), 0);
        assert_eq!(reiny_release(source_port), 0);
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
