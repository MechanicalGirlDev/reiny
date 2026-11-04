use super::protocol::{FailureCode, FrameError};

/// Secret-free rejection of the connected process session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessError {
    /// Stable wire failure category. Raw JSON and nonce bytes are never retained.
    pub code: FailureCode,
}

impl ProcessError {
    pub(super) const fn new(code: FailureCode) -> Self {
        Self { code }
    }

    pub(super) fn io(_: std::io::Error) -> Self {
        Self::new(FailureCode::Disconnected)
    }

    pub(super) fn frame(_: FrameError) -> Self {
        Self::new(FailureCode::MalformedMessage)
    }

    pub(super) fn json(_: serde_json::Error) -> Self {
        Self::new(FailureCode::MalformedMessage)
    }
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(output, "component host session: {:?}", self.code)
    }
}

impl std::error::Error for ProcessError {}
