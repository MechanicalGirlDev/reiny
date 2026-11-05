use super::{
    Arc, CALLBACK, Context, Error, INVALID, Mutex, Object, Result, Sink, Work, insert, lock,
    object, variant, wrong,
};
use std::ffi::c_void;

struct Borrowed(u64);

impl Drop for Borrowed {
    fn drop(&mut self) {
        if let Err(error) = crate::c_api::remove(self.0, true) {
            tracing::error!(%error, "cannot release job callback handle");
        }
    }
}

struct Lane {
    work: Arc<Work>,
    context: Borrowed,
    instance: *mut c_void,
}

impl Lane {
    fn invoke(call: impl FnOnce(u64) -> i32) -> Result<()> {
        let sink = Borrowed(insert(Object::ErrorSink(Mutex::new(None)), true)?);
        let code = call(sink.0);
        if code == 0 {
            return Ok(());
        }
        let object = object(sink.0)?;
        Err(lock(variant!(object, ErrorSink))?
            .take()
            .unwrap_or_else(|| {
                Error::new(
                    if code < 0 { code } else { CALLBACK },
                    format!("job callback returned {code}"),
                )
            }))
    }

    fn execute(&mut self) -> Result<Vec<u8>> {
        let table = self.work.table;
        let create = table
            .create
            .ok_or_else(|| Error::new(INVALID, "missing create callback"))?;
        let mut instance = std::ptr::null_mut();
        Self::invoke(|error| {
            // SAFETY: [FFI/lifetime] live copied table, independently owned data,
            // writable local instance slot; all hooks remain on this native lane.
            unsafe { create(table.factory_data, self.context.0, &raw mut instance, error) }
        })?;
        if instance.is_null() {
            return Err(Error::new(CALLBACK, "create returned no instance"));
        }
        self.instance = instance;
        let sink = Borrowed(insert(
            Object::JobSink(Mutex::new(Sink {
                owner: std::thread::current().id(),
                value: None,
            })),
            true,
        )?);
        let run = table
            .run
            .ok_or_else(|| Error::new(INVALID, "missing run callback"))?;
        Self::invoke(|error| {
            // SAFETY: [FFI/lifetime] successful create supplied this lane's
            // instance; every borrowed handle remains valid through this call.
            unsafe { run(self.instance, self.context.0, sink.0, error) }
        })?;
        let object = object(sink.0)?;
        lock(variant!(object, JobSink))?
            .value
            .take()
            .ok_or_else(|| Error::new(CALLBACK, "successful callback omitted result bytes"))
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        if !self.instance.is_null()
            && let Some(destroy) = self.work.table.destroy_instance
        {
            // SAFETY: [FFI/double-free] only this lane owns the successful create;
            // all callbacks have returned, and this Drop runs exactly once.
            unsafe {
                destroy(self.instance);
            }
        }
    }
}

pub(super) fn run(work: Arc<Work>, context: Context) -> Result<Vec<u8>> {
    let mut lane = Lane {
        work,
        context: Borrowed(insert(Object::JobContext(Arc::new(context)), true)?),
        instance: std::ptr::null_mut(),
    };
    let result = lane.execute();
    drop(lane);
    result
}
