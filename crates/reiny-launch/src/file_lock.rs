//! Scope-bound file locks, independent of inherited descriptor lifetimes.

use std::fs::{File, TryLockError};

pub(crate) struct FileLock(File);

impl FileLock {
    pub(crate) fn new(file: File) -> Result<Self, TryLockError> {
        file.try_lock()?;
        Ok(Self(file))
    }

    pub(crate) fn wait(
        path: &std::path::Path,
        cancel: &crate::progress::CancelToken,
    ) -> anyhow::Result<Self> {
        Self::wait_with(path, cancel, || {})
    }

    pub(crate) fn wait_with(
        path: &std::path::Path,
        cancel: &crate::progress::CancelToken,
        contended: impl FnOnce(),
    ) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let mut contended = Some(contended);
        loop {
            cancel.check()?;
            match file.try_lock() {
                Ok(()) => return Ok(Self(file)),
                Err(TryLockError::WouldBlock) => {
                    if let Some(contended) = contended.take() {
                        contended();
                    }
                    cancel.wait_timeout(std::time::Duration::from_millis(25))?;
                }
                Err(TryLockError::Error(error)) => return Err(error.into()),
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // A forked child can retain the same open-file description until exec.
        // Closing our descriptor alone would leave its lock held in that child.
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "release file lock");
        }
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::FileLock;
    use crate::progress::{CancelToken, PrepareError};

    #[test]
    fn contended_lock_wakes_on_cancellation_before_the_holder_releases() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("held.lock");
        let held = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        held.lock()?;
        let token = CancelToken::default();
        let cancel = token.clone();
        let (waiting, contention) = std::sync::mpsc::channel();
        let (finished, completion) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let result = FileLock::wait_with(&path, &cancel, || {
                let _ = waiting.send(());
            });
            let _ = finished.send(result.map(|_| ()));
        });
        contention.recv_timeout(std::time::Duration::from_secs(10))?;
        token.cancel();
        let result = completion.recv_timeout(std::time::Duration::from_secs(10))?;
        assert!(result.is_err_and(|error| {
            error
                .downcast_ref::<PrepareError>()
                .is_some_and(|error| matches!(error, PrepareError::Cancelled))
        }));
        waiter
            .join()
            .map_err(|_| anyhow::anyhow!("lock waiter panicked"))?;
        held.unlock()?;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::FileLock;

    #[test]
    fn scope_exit_releases_lock_even_with_an_inherited_descriptor() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let lock = FileLock::new(file.reopen()?)?;
        let inherited = lock.0.try_clone()?;
        assert!(FileLock::new(file.reopen()?).is_err());

        drop(lock);
        let next = FileLock::new(file.reopen()?)?;
        drop(inherited);
        drop(next);
        Ok(())
    }
}
