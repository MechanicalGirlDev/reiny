//! Retained, event-driven completion of an owned Windows process tree.

use std::future::Future;
use std::io;
use std::os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle};
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use process_wrap::tokio::{ChildWrapper, CommandWrap, CommandWrapper, JobObject, KillOnDrop};
use windows_sys::Win32::{
    Foundation::{INVALID_HANDLE_VALUE, WAIT_OBJECT_0},
    System::{
        IO::{CreateIoCompletionPort, GetQueuedCompletionStatus},
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_ASSOCIATE_COMPLETION_PORT, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectAssociateCompletionPortInformation, JobObjectExtendedLimitInformation,
            SetInformationJobObject, TerminateJobObject,
        },
        Threading::{INFINITE, WaitForSingleObject},
    },
};

/// Complete an owned tree only after its job reports `ACTIVE_PROCESS_ZERO`.
///
/// Register after [`JobObject`], alongside [`KillOnDrop`], on a native Tokio
/// command. Allocation happens during spawn, and assignment precedes resume.
/// Existing creation flags, stdio and tree killing remain owned by process-wrap.
///
/// Unlike process-wrap 10.0.1's job wait, this ignores nonterminal job messages.
/// Cancelling a wait retains its observer in the child. Dropping or unwrapping
/// the child terminates and joins the tree synchronously, without a Tokio runtime.
/// Native Windows integration tests exercise the FFI; Miri cannot execute it.
#[derive(Debug, Default)]
pub struct WindowsJobCompletion {
    job: Option<Job>,
}

fn checked(result: i32) -> io::Result<()> {
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[derive(Debug)]
struct Job {
    handle: OwnedHandle,
    port: OwnedHandle,
    assigned: bool,
    empty: Mutex<bool>,
}

impl Job {
    fn new() -> io::Result<Self> {
        // SAFETY: null attributes/name request a new anonymous, noninherited job.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the new non-null job handle has exactly one owner.
        let job = unsafe { OwnedHandle::from_raw_handle(handle) };
        // SAFETY: INVALID_HANDLE_VALUE requests a new private completion port.
        let handle =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the new non-null port handle has exactly one owner.
        let port = unsafe { OwnedHandle::from_raw_handle(handle) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the owned job is valid and the initialized structure/size match
        // JobObjectExtendedLimitInformation; Windows copies it during this call.
        checked(unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                u32::try_from(std::mem::size_of_val(&limits)).map_err(io::Error::other)?,
            )
        })?;
        let association = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
            CompletionKey: job.as_raw_handle(),
            CompletionPort: port.as_raw_handle(),
        };
        // SAFETY: both handles remain owned; the job is still empty, so no
        // process notification can precede this initialized association.
        checked(unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectAssociateCompletionPortInformation,
                (&raw const association).cast(),
                u32::try_from(std::mem::size_of_val(&association)).map_err(io::Error::other)?,
            )
        })?;
        Ok(Self {
            handle: job,
            port,
            assigned: false,
            empty: Mutex::new(false),
        })
    }

    fn terminate(&self) -> io::Result<()> {
        // SAFETY: this handle owns the private job and remains live through the call.
        checked(unsafe { TerminateJobObject(self.handle.as_raw_handle(), 1) })
    }

    fn wait_empty(&self) -> io::Result<()> {
        // JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO from winnt.h.
        const ACTIVE_PROCESS_ZERO: u32 = 4;
        let mut empty = self
            .empty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*empty {
            let (mut message, mut key) = (0, 0);
            let mut process = std::ptr::null_mut();
            // SAFETY: the port stays owned for the entire wait. All output
            // pointers refer to initialized locals; the process token is never dereferenced.
            checked(unsafe {
                GetQueuedCompletionStatus(
                    self.port.as_raw_handle(),
                    &raw mut message,
                    &raw mut key,
                    &raw mut process,
                    INFINITE,
                )
            })?;
            *empty = key == self.handle.as_raw_handle().addr() && message == ACTIVE_PROCESS_ZERO;
        }
        Ok(())
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // Also covers a later spawn hook failing before the child wrapper exists.
        if self.assigned
            && !*self
                .empty
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            let killed = self.terminate();
            let waited = self.wait_empty();
            if let Err(error) = killed.and(waited) {
                tracing::error!(%error, "failed to finish Windows completion job");
            }
        }
    }
}

impl CommandWrapper for WindowsJobCompletion {
    fn pre_spawn(
        &mut self,
        _command: &mut tokio::process::Command,
        core: &CommandWrap,
    ) -> io::Result<()> {
        if !core.has_wrap::<JobObject>() || !core.has_wrap::<KillOnDrop>() {
            return Err(io::Error::other(
                "Windows completion requires JobObject and KillOnDrop",
            ));
        }
        self.job = Some(Job::new()?);
        Ok(())
    }

    fn post_spawn(
        &mut self,
        _command: &mut tokio::process::Command,
        child: &mut tokio::process::Child,
        _core: &CommandWrap,
    ) -> io::Result<()> {
        let job = self
            .job
            .as_mut()
            .ok_or_else(|| io::Error::other("completion job missing"))?;
        let handle = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("child handle missing"))?;
        // SAFETY: JobObject's pre_spawn suspends this native child, and all
        // post_spawn hooks precede every wrap_child hook (including resume).
        let result =
            checked(unsafe { AssignProcessToJobObject(job.handle.as_raw_handle(), handle) });
        if result.is_ok() {
            job.assigned = true;
        } else {
            let killed = child.start_kill();
            // SAFETY: the borrowed native child handle remains valid until return.
            let waited = unsafe { WaitForSingleObject(handle, INFINITE) };
            if let Err(error) = killed {
                tracing::error!(%error, "failed to terminate unassigned Windows child");
            }
            if waited != WAIT_OBJECT_0 {
                tracing::error!(waited, "failed to reap unassigned Windows child");
            }
        }
        result
    }

    fn wrap_child(
        &mut self,
        inner: Box<dyn ChildWrapper>,
        _core: &CommandWrap,
    ) -> io::Result<Box<dyn ChildWrapper>> {
        let job = self
            .job
            .take()
            .ok_or_else(|| io::Error::other("completion job missing"))?;
        if !job.assigned {
            return Err(io::Error::other(
                "Windows completion requires a native child spawn",
            ));
        }
        let job = Arc::new(job);
        let observed = Arc::clone(&job);
        let (notify, done) = tokio::sync::oneshot::channel();
        let worker = std::thread::Builder::new()
            .name("reiny-windows-job".into())
            .spawn(move || {
                let result = observed.wait_empty();
                let _notified = notify.send(());
                result
            })?;
        Ok(Box::new(CompletionChild {
            completion: Completion {
                job,
                done,
                worker: Some(worker),
                result: Ok(()),
            },
            inner,
        }))
    }
}

#[derive(Debug)]
struct Completion {
    job: Arc<Job>,
    done: tokio::sync::oneshot::Receiver<()>,
    worker: Option<JoinHandle<io::Result<()>>>,
    result: io::Result<()>,
}

impl Completion {
    fn join(&mut self) -> io::Result<()> {
        if let Some(worker) = self.worker.take() {
            self.result = worker
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("completion observer panicked")));
        }
        self.result
            .as_ref()
            .map_err(|error| {
                error.raw_os_error().map_or_else(
                    || io::Error::new(error.kind(), error.to_string()),
                    io::Error::from_raw_os_error,
                )
            })
            .copied()
    }

    async fn wait(&mut self) -> io::Result<()> {
        if self.worker.is_some() {
            let notified = (&mut self.done).await;
            let result = self.join();
            result.and(notified.map_err(io::Error::other))
        } else {
            self.join()
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let killed = self.job.terminate();
            let waited = self.join();
            if let Err(error) = killed.and(waited) {
                tracing::error!(%error, "failed to join Windows process tree on drop");
            }
        }
    }
}

#[derive(Debug)]
struct CompletionChild {
    // Drop/join completion before releasing the native child and its original job.
    completion: Completion,
    inner: Box<dyn ChildWrapper>,
}

impl ChildWrapper for CompletionChild {
    fn inner(&self) -> &dyn ChildWrapper {
        &*self.inner
    }
    fn inner_mut(&mut self) -> &mut dyn ChildWrapper {
        &mut *self.inner
    }
    fn into_inner(self: Box<Self>) -> Box<dyn ChildWrapper> {
        let Self { completion, inner } = *self;
        drop(completion);
        inner
    }
    fn process_handle(&self) -> Option<BorrowedHandle<'_>> {
        self.inner.process_handle()
    }
    fn wait(&mut self) -> Pin<Box<dyn Future<Output = io::Result<ExitStatus>> + Send + '_>> {
        Box::pin(async {
            self.completion.wait().await?;
            self.inner.wait().await
        })
    }
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if self
            .completion
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            return Ok(None);
        }
        self.completion.join()?;
        self.inner.try_wait()
    }
}
