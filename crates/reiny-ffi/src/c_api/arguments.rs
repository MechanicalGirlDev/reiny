//! Legacy pointer inputs and thread-local error boundary.

use super::Result;
use std::{
    cell::RefCell,
    ffi::{CStr, CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
};

thread_local! {
    pub(super) static ERROR: RefCell<CString> = RefCell::new(CString::default());
}

pub(super) fn boundary(f: impl FnOnce() -> Result<()>) -> i32 {
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

pub(super) fn output<T>(ptr: *mut T) -> Result<()> {
    if ptr.is_null() {
        Err("NULL output pointer".into())
    } else {
        Ok(())
    }
}

pub(super) unsafe fn text(ptr: *const c_char) -> Result<String> {
    if ptr.is_null() {
        return Err("NULL required string".into());
    }
    // SAFETY: caller supplies a readable NUL-terminated string.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(str::to_owned)
        .map_err(|e| e.to_string())
}

pub(super) unsafe fn optional_text(ptr: *const c_char) -> Result<Option<String>> {
    if ptr.is_null() {
        Ok(None)
    } else {
        // SAFETY: same caller contract as text.
        unsafe { text(ptr) }.map(Some)
    }
}

pub(super) unsafe fn bytes(ptr: *const u8, len: usize) -> Result<Vec<u8>> {
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
