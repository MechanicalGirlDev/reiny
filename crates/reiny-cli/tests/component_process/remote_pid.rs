//! Linux fixture-only identity-stable exit observation; never product authority.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{Context, Result};
use tokio::io::unix::AsyncFd;

unsafe extern "C" {
    fn pidfd_open(pid: i32, flags: u32) -> i32;
    fn pidfd_send_signal(fd: i32, signal: i32, info: *const std::ffi::c_void, flags: u32) -> i32;
    fn prctl(option: i32, ...) -> i32;
    fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
}

pub(super) struct Adoption(i32);

pub(super) fn adopt_orphans() -> Result<Adoption> {
    let mut previous = 0_i32;
    // SAFETY: Linux PR_GET_CHILD_SUBREAPER writes one int to this valid pointer.
    if unsafe { prctl(37, &raw mut previous, 0_usize, 0_usize, 0_usize) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: Linux PR_SET_CHILD_SUBREAPER changes only this fixture process.
    // This allows the test to reap its SSH clients after intentionally killing
    // its owner. It makes no claim about production remote containment.
    if unsafe { prctl(36, 1_usize, 0_usize, 0_usize, 0_usize) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(Adoption(previous))
}

impl Drop for Adoption {
    fn drop(&mut self) {
        // SAFETY: restore this process's original setting while the shared
        // fixture lock still excludes other process tests.
        assert_eq!(unsafe { prctl(36, self.0, 0_usize, 0_usize, 0_usize) }, 0);
    }
}

pub(super) fn reap_adopted(pid: u64) -> Result<()> {
    let pid = i32::try_from(pid)?;
    let mut status = 0;
    // SAFETY: the PID is an owned fixture client adopted after owner death.
    // Its pidfd has already signalled exit, so this does not wait on timing luck.
    if unsafe { waitpid(pid, &raw mut status, 0) } != pid {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

pub(super) struct Exit(AsyncFd<OwnedFd>);

impl Exit {
    pub(super) fn subscribe(pid: u64) -> Result<Self> {
        // SAFETY: pidfd_open receives a checked PID and flags=0; it returns a new
        // owned descriptor, not a borrowed descriptor or process-control handle.
        let fd = unsafe { pidfd_open(i32::try_from(pid)?, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: successful pidfd_open transfers ownership of this descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self(AsyncFd::new(fd)?))
    }

    pub(super) fn kill(&self) -> Result<()> {
        // SAFETY: signal only the exact owned fixture process referenced by its
        // pidfd, never a potentially reused numeric PID. Null siginfo is valid.
        if unsafe { pidfd_send_signal(self.0.get_ref().as_raw_fd(), 9, std::ptr::null(), 0) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    pub(super) async fn exited(self) -> Result<()> {
        let _ready = tokio::time::timeout(super::DEADLINE, self.0.readable())
            .await
            .context("owned fixture native process did not exit")??;
        Ok(())
    }
}
