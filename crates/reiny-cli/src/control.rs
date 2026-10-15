//! One latched cancellation source for signals, control input and publication.

use std::io::{BufRead, Read};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use reiny_launch::{CancelToken, PrepareError};
use serde::Deserialize;

#[derive(Clone, Default)]
pub(crate) struct Control {
    pub(crate) cancel: CancelToken,
    state: Arc<Mutex<State>>,
    changed: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct State {
    error: Option<String>,
    finished: bool,
    cancelled_at: Option<std::time::Instant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    v: u32,
    cmd: Command,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Command {
    Cancel,
}

impl Control {
    pub(crate) fn install(stdin: bool) -> Result<Self> {
        let control = Self::default();
        let signal = control.clone();
        ctrlc::set_handler(move || signal.request(None))
            .context("installing cancellation handler")?;
        if stdin {
            let input = control.clone();
            std::thread::Builder::new()
                .name("reiny-control".into())
                .spawn(move || input.read(std::io::stdin().lock()))?;
        }
        Ok(control)
    }

    fn read(&self, mut reader: impl BufRead) {
        loop {
            let mut line = Vec::new();
            let result = reader.by_ref().take(4097).read_until(b'\n', &mut line);
            match result {
                Ok(0) => {
                    self.request(None);
                    return;
                }
                Ok(_) => {
                    let parsed = (|| {
                        anyhow::ensure!(
                            line.len() <= 4096 && line.last() == Some(&b'\n'),
                            "control message must be a bounded newline-terminated JSON object"
                        );
                        let message: Message = serde_json::from_slice(&line)?;
                        anyhow::ensure!(message.v == 1, "unsupported control protocol");
                        match message.cmd {
                            Command::Cancel => Ok(()),
                        }
                    })();
                    match parsed {
                        Ok(()) => self.request(None),
                        Err(error) => {
                            self.request(Some(format!("invalid control input: {error:#}")));
                            return;
                        }
                    }
                }
                Err(error) => {
                    self.request(Some(format!("reading control input: {error}")));
                    return;
                }
            }
        }
    }

    pub(crate) fn request(&self, error: Option<String>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.finished {
            return;
        }
        if state.error.is_none() {
            state.error = error;
        }
        state
            .cancelled_at
            .get_or_insert_with(std::time::Instant::now);
        self.cancel.cancel();
        self.changed.notify_waiters();
    }

    pub(crate) fn cancelled_at(&self) -> Option<std::time::Instant> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancelled_at
    }

    pub(crate) fn check(&self) -> Result<()> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(error) = &state.error {
            anyhow::bail!("{error}");
        }
        if self.cancel.is_cancelled() {
            return Err(PrepareError::Cancelled.into());
        }
        Ok(())
    }

    pub(crate) async fn cancelled(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.cancel.is_cancelled() {
                return;
            }
            notified.await;
        }
    }

    /// Controls cannot overtake the final write or change its committed outcome.
    pub(crate) fn publish<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(error) = &state.error {
            anyhow::bail!("{error}");
        }
        let result = self.cancel.publish(operation)?;
        state.finished = true;
        Ok(result)
    }

    pub(crate) fn active<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(error) = &state.error {
            anyhow::bail!("{error}");
        }
        Ok(self.cancel.publish(operation)?)
    }

    pub(crate) fn failure(&self, error: anyhow::Error) -> anyhow::Error {
        match self.check() {
            Ok(()) => error,
            Err(cancelled) => cancelled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_winning_publication_prevents_the_write() -> Result<()> {
        // Given cancellation latched before the publication lock is acquired.
        let control = Control::default();
        control.request(None);
        let mut wrote = false;
        // When publication is attempted.
        let result = control.publish(|| {
            wrote = true;
            Ok(())
        });
        // Then no side effect runs and the typed cancellation survives.
        assert!(!wrote);
        assert!(matches!(
            result
                .err()
                .context("publication unexpectedly succeeded")?
                .downcast_ref::<PrepareError>(),
            Some(PrepareError::Cancelled)
        ));
        Ok(())
    }

    #[test]
    fn committed_publication_cannot_be_reclassified_by_later_controls() -> Result<()> {
        // Given successful publication already won the terminal-state lock.
        let control = Control::default();
        let published = control.publish(|| Ok(7))?;
        // When late cancellation and malformed input arrive.
        control.request(None);
        control.read(std::io::Cursor::new(b"{\"v\":2,\"cmd\":\"cancel\"}\n"));
        // Then the committed result remains successful.
        assert_eq!(published, 7);
        control.check()?;
        assert!(!control.cancel.is_cancelled());
        Ok(())
    }

    #[test]
    fn malformed_frames_latch_failure_instead_of_plain_cancellation() -> Result<()> {
        // Given unsupported versions, unknown fields, commands and truncated frames.
        for input in [
            b"{\"v\":2,\"cmd\":\"cancel\"}\n".as_slice(),
            b"{\"v\":1,\"cmd\":\"cancel\",\"extra\":0}\n",
            b"{\"v\":1,\"cmd\":\"stop\"}\n",
            b"{\"v\":1,\"cmd\":\"cancel\"}",
        ] {
            let control = Control::default();
            // When the control reader reaches the malformed frame.
            control.read(std::io::Cursor::new(input));
            // Then cleanup is cancelled but its terminal result is failure.
            assert!(control.cancel.is_cancelled());
            assert!(
                control
                    .check()
                    .err()
                    .context("malformed control was accepted")?
                    .downcast_ref::<PrepareError>()
                    .is_none()
            );
        }
        Ok(())
    }
}
