//! Executable status, runtime admission and thread-local error boundary.

use std::{
    cell::RefCell,
    sync::{Mutex, OnceLock},
    time::Duration,
};
use tokio::runtime::{Builder, Runtime};

pub(super) const INVALID: i32 = -1;
pub(super) const HANDLE: i32 = -2;
pub(super) const KIND: i32 = -3;
pub(super) const STATE: i32 = -4;
pub(super) const TIMEOUT: i32 = -5;
pub(super) const CLOSED: i32 = -6;
pub(super) const REMOTE: i32 = -7;
pub(super) const NO_REPLY: i32 = -8;
pub(super) const SCHEMA: i32 = -9;
pub(super) const CALLBACK: i32 = -10;
pub(super) const BUFFER: i32 = -12;
pub(super) const PANIC: i32 = -13;
pub(super) const UNSUPPORTED: i32 = -14;
pub(super) const COMPLETED: i32 = -15;
pub(super) const FAILURE: i32 = -16;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(super) struct Error {
    pub(super) code: i32,
    pub(super) message: String,
}

impl Error {
    pub(super) fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub(super) type Result<T> = std::result::Result<T, Error>;

pub(super) fn failure(error: impl std::fmt::Display) -> Error {
    Error::new(FAILURE, error.to_string())
}

pub(super) fn lock<T>(value: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    value
        .lock()
        .map_err(|_| Error::new(PANIC, "object lock poisoned"))
}

pub(super) fn runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<std::result::Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| Error::new(FAILURE, e.clone()))
}

pub(super) fn ordinary() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::new(
            STATE,
            "blocking ABI call requires an ordinary thread",
        ));
    }
    Ok(())
}

pub(super) fn budget(ns: u64) -> Result<Duration> {
    let duration = Duration::from_nanos(ns);
    if duration.is_zero() || std::time::Instant::now().checked_add(duration).is_none() {
        return Err(Error::new(INVALID, "invalid timeout"));
    }
    Ok(duration)
}

pub(super) fn wait<T>(ns: u64, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    ordinary()?;
    let duration = budget(ns)?;
    runtime()?.block_on(async {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| Error::new(TIMEOUT, "wait deadline expired"))?
    })
}

thread_local! {
    pub(super) static ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

pub(super) fn boundary(f: impl FnOnce() -> Result<()>) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(_) => Some(Error::new(
            PANIC,
            "Rust panic at executable ABI boundary; outcome may be unknown",
        )),
    };
    ERROR.with(|slot| {
        slot.borrow_mut()
            .clone_from(&error.as_ref().map_or_else(String::new, ToString::to_string));
    });
    error.map_or(0, |error| error.code)
}
