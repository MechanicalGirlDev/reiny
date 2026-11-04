use core::fmt;

/// Maximum JSON payload size, excluding the four-byte length prefix.
pub const MAX_FRAME_BYTES: u32 = 8 * 1024 * 1024;

/// Reject empty/oversized payloads before reading or allocating their body.
///
/// This function interprets only the header, not the JSON payload.
pub fn payload_length(header: [u8; 4]) -> Result<usize, FrameError> {
    let length = u32::from_be_bytes(header);
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(FrameError::Length(length));
    }
    usize::try_from(length).map_err(|_| FrameError::Length(length))
}

/// Build a bounded frame header for already serialized JSON.
///
/// The caller sends this header followed by exactly `payload.len()` bytes.
/// Serialization must itself be bounded; this helper cannot undo an allocation.
/// It does not check that the bytes contain valid JSON.
pub fn frame_header(payload: &[u8]) -> Result<[u8; 4], FrameError> {
    let length = u32::try_from(payload.len()).map_err(|_| FrameError::Length(u32::MAX))?;
    let header = length.to_be_bytes();
    payload_length(header)?;
    Ok(header)
}

/// Borrow one complete bounded frame's JSON bytes without allocating.
///
/// Truncation and trailing bytes (including a second frame) are errors. Stream
/// readers split frames themselves after calling [`payload_length`]. The caller
/// must then parse exactly one JSON envelope; these bytes are not yet trusted.
pub fn frame_payload(frame: &[u8]) -> Result<&[u8], FrameError> {
    let (header, body) = frame
        .split_first_chunk::<4>()
        .ok_or(FrameError::Truncated)?;
    let length = payload_length(*header)?;
    match body.len().cmp(&length) {
        core::cmp::Ordering::Less => Err(FrameError::Truncated),
        core::cmp::Ordering::Greater => Err(FrameError::TrailingBytes),
        core::cmp::Ordering::Equal => Ok(body),
    }
}

/// Framing rejection with no payload or secret included in diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// Empty or oversized declared payload, including unrepresentable lengths.
    Length(u32),
    /// Missing header bytes or an incomplete body.
    Truncated,
    /// More bytes than the one declared frame.
    TrailingBytes,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length(length) => write!(f, "invalid host frame payload length {length}"),
            Self::Truncated => f.write_str("truncated host frame"),
            Self::TrailingBytes => f.write_str("trailing host frame bytes"),
        }
    }
}

impl core::error::Error for FrameError {}
