use super::{
    Arc, Error, INVALID, Mutex, Object, Reservation, Result, STATE, abi, boundary, export, object,
    output, variant, wrong,
};
use std::ffi::c_void;
use std::sync::atomic::{AtomicU8, Ordering};

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Table {
    pub(super) struct_size: u32,
    pub(super) abi_major: u32,
    pub(super) flags: u32,
    pub(super) reserved: u32,
    pub(super) factory_data: *mut c_void,
    pub(super) create: Option<abi::Create>,
    pub(super) run: Option<unsafe extern "C" fn(*mut c_void, u64, u64, u64) -> i32>,
    pub(super) destroy_instance: Option<abi::Destroy>,
    pub(super) release_factory: Option<abi::Destroy>,
}

// SAFETY: [Send/Sync] work_new transfers an independent factory-data claim.
// Only its single native lane invokes create/run/destroy; finalization occurs
// after every claim ends. The caller keeps callback code and data live until then.
unsafe impl Send for Table {}
// SAFETY: [Send/Sync] the copied table is immutable; the admitted mutex permits
// only one invocation. Callback-data synchronization is the C caller's contract.
unsafe impl Sync for Table {}

pub(in crate::component) struct Work {
    pub(super) table: Table,
    pub(super) admitted: Mutex<bool>,
    released: Option<Arc<AtomicU8>>,
}

impl Drop for Work {
    fn drop(&mut self) {
        if let Some(release) = self.table.release_factory {
            // SAFETY: [FFI/lifetime] this last owner holds the transferred claim;
            // the header requires this finalizer to be nonblocking/nonunwinding.
            unsafe {
                release(self.table.factory_data);
            }
        }
        if let Some(token) = &self.released {
            token.store(2, Ordering::SeqCst);
        }
    }
}

unsafe fn import(table: *const Table, token: Option<u64>) -> Result<u64> {
    if table.is_null() || !table.is_aligned() {
        return Err(Error::new(INVALID, "invalid job table"));
    }
    // SAFETY: [bounds] caller supplies a readable size prefix with checked alignment.
    if usize::try_from(unsafe { table.cast::<u32>().read() }).ok()
        != Some(std::mem::size_of::<Table>())
    {
        return Err(Error::new(INVALID, "invalid job table size"));
    }
    // SAFETY: [bounds/initialization] caller promises the validated full table size.
    let table = unsafe { table.read() };
    if table.abi_major != 1
        || table.flags != 0
        || table.reserved != 0
        || table.create.is_none()
        || table.run.is_none()
        || table.destroy_instance.is_none()
    {
        return Err(Error::new(
            INVALID,
            "invalid job table version, flags or callbacks",
        ));
    }
    let released = if let Some(token) = token {
        if table.release_factory.is_some() {
            return Err(Error::new(
                INVALID,
                "notified work forbids foreign finalizers",
            ));
        }
        let token = object(token)?;
        Some(variant!(token, ReleaseToken).clone())
    } else {
        None
    };
    let reservation = Reservation::new()?;
    if let Some(token) = &released {
        token
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| Error::new(STATE, "release token already bound"))?;
    }
    reservation.publish(Object::JobWork(Arc::new(Work {
        table,
        admitted: Mutex::new(false),
        released,
    })))
}

export!(reiny_job_work_new(table: *const Table, out: *mut u64) {
    output(out)?;
    out.write(import(table, None)?);
    Ok(())
});

export!(reiny_job_work_new_notified(table: *const Table, token: u64, out: *mut u64) {
    output(out)?;
    out.write(import(table, Some(token))?);
    Ok(())
});
