//! Scope-bound file locks, independent of inherited descriptor lifetimes.

use std::fs::{File, TryLockError};

pub(crate) struct FileLock(File);

impl FileLock {
    pub(crate) fn new(file: File) -> Result<Self, TryLockError> {
        file.try_lock()?;
        Ok(Self(file))
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
