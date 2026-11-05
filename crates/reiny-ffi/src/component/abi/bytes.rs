//! Validated foreign byte slices and initialized copy outputs.

use super::{BUFFER, Error, INVALID, Result};

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Bytes {
    pub(crate) data: *const u8,
    pub(crate) len: usize,
}

impl Bytes {
    unsafe fn copy(self) -> Result<Vec<u8>> {
        if self.len == 0 {
            return Ok(Vec::new());
        }
        if self.data.is_null() || self.len > isize::MAX.unsigned_abs() {
            return Err(Error::new(INVALID, "invalid byte slice"));
        }
        // SAFETY: [bounds/lifetime] the C caller promises len readable bytes in
        // one allocation through this call; null and isize overflow were checked.
        Ok(unsafe { std::slice::from_raw_parts(self.data, self.len) }.to_vec())
    }

    pub(in crate::component) unsafe fn text(self) -> Result<String> {
        // SAFETY: [bounds/lifetime] inherits the documented input slice contract.
        let bytes = unsafe { self.copy()? };
        let text = String::from_utf8(bytes).map_err(|e| Error::new(INVALID, e.to_string()))?;
        if text.contains('\0') {
            return Err(Error::new(INVALID, "embedded NUL in text"));
        }
        Ok(text)
    }

    pub(in crate::component) unsafe fn bytes(self) -> Result<Vec<u8>> {
        // SAFETY: [bounds/lifetime] inherits the documented input slice contract.
        unsafe { self.copy() }
    }
}

pub(in crate::component) fn output<T>(pointer: *mut T) -> Result<()> {
    if pointer.is_null() || !pointer.is_aligned() {
        return Err(Error::new(INVALID, "null or misaligned output"));
    }
    Ok(())
}

pub(in crate::component) unsafe fn copy_out(
    bytes: &[u8],
    dest: *mut u8,
    capacity: usize,
    required: *mut usize,
) -> Result<()> {
    output(required)?;
    if capacity > isize::MAX.unsigned_abs() || (dest.is_null() && capacity != 0) {
        return Err(Error::new(INVALID, "invalid output buffer"));
    }
    if capacity < bytes.len() {
        // SAFETY: [alignment/bounds] output verified alignment; caller owns the
        // writable required field and guarantees it does not alias dest.
        unsafe {
            required.write(bytes.len());
        }
        return if dest.is_null() && capacity == 0 {
            Ok(())
        } else {
            Err(Error::new(BUFFER, "output buffer is too small"))
        };
    }
    if !bytes.is_empty() {
        if dest.is_null() {
            return Err(Error::new(INVALID, "null output buffer"));
        }
        // SAFETY: [bounds/aliasing] caller promises capacity writable bytes
        // disjoint from SDK memory; capacity covers this immutable snapshot.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dest, bytes.len());
        }
    }
    // SAFETY: [alignment/bounds] caller supplies an aligned writable scalar.
    unsafe {
        required.write(bytes.len());
    }
    Ok(())
}
