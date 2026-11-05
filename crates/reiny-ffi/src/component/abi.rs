use super::{
    Arc, BUFFER, ERROR, Error, Factory, HANDLE, INVALID, Mutex, Object, PANIC, Registry, Result,
    SCHEMA, STATE, Stored, UNSUPPORTED, boundary, export, failure, insert, lock, object, variant,
    wait, wrong,
};
use std::ffi::c_void;

mod bytes;
pub(crate) use bytes::Bytes;
pub(super) use bytes::{copy_out, output};

pub(super) type Create = unsafe extern "C" fn(*mut c_void, u64, *mut *mut c_void, u64) -> i32;
pub(super) type Hook = unsafe extern "C" fn(*mut c_void, u64, u64) -> i32;
pub(super) type Stop = unsafe extern "C" fn(*mut c_void, u64, u32, u64) -> i32;
pub(super) type Destroy = unsafe extern "C" fn(*mut c_void);

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct FactoryV1 {
    pub(crate) struct_size: u32,
    pub(crate) abi_major: u32,
    pub(crate) flags: u32,
    pub(crate) mode: u32,
    pub(crate) factory_data: *mut c_void,
    pub(crate) create: Option<Create>,
    pub(crate) prepare: Option<Hook>,
    pub(crate) run: Option<Hook>,
    pub(crate) stop: Option<Stop>,
    pub(crate) destroy_instance: Option<Destroy>,
    pub(crate) release_factory: Option<Destroy>,
}

// SAFETY: [Send/Sync] registration requires factory_data to support concurrent
// creates on separate instance lanes. The table is immutable and callback code
// lives until the last registration/lane claim is gone. Instance pointers never
// enter this table and are confined to one lane.
unsafe impl Send for FactoryV1 {}
// SAFETY: [Send/Sync] the same registration contract synchronizes factory_data;
// sharing this immutable copied table introduces no additional mutation.
unsafe impl Sync for FactoryV1 {}

impl FactoryV1 {
    pub(super) fn validate(&self) -> Result<()> {
        if usize::try_from(self.struct_size).ok() != Some(std::mem::size_of::<Self>())
            || self.abi_major != 1
            || self.flags != 0
        {
            return Err(Error::new(
                INVALID,
                "invalid factory ABI size, version or flags",
            ));
        }
        if self.mode != 1 {
            return Err(Error::new(
                UNSUPPORTED,
                "only service factories are supported",
            ));
        }
        if self.create.is_none()
            || self.prepare.is_none()
            || self.run.is_none()
            || self.stop.is_none()
            || self.destroy_instance.is_none()
        {
            return Err(Error::new(INVALID, "missing service callback"));
        }
        Ok(())
    }
}

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_exec_abi_version() -> u32 {
    1
}

/// # Safety
/// The output pointers follow the copy contract in `reiny_component.h`.
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn reiny_exec_last_error_copy(
    dest: *mut u8,
    capacity: usize,
    required: *mut usize,
) -> i32 {
    // Reading an error must not clear or replace it between size and copy calls.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut bytes = ERROR.with(|slot| slot.borrow().as_bytes().to_vec());
        bytes.push(0);
        // SAFETY: [bounds/aliasing] caller supplies the documented disjoint
        // writable outputs; copy_out checks null, capacity and alignment.
        unsafe { copy_out(&bytes, dest, capacity, required) }
    })) {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => error.code,
        Err(_) => PANIC,
    }
}

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_exec_release(handle: u64) -> i32 {
    boundary(|| {
        object(handle)?;
        drop(crate::c_api::remove(handle, false).map_err(|e| Error::new(HANDLE, e))?);
        Ok(())
    })
}

export!(reiny_exec_clone(handle: u64, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    if matches!(value.as_ref(), Object::ErrorSink(_) | Object::StepSink(_) | Object::JobSink(_)) {
        return Err(Error::new(STATE, "callback error sink cannot be retained"));
    }
    let id = crate::c_api::insert(crate::c_api::Object::Executable(Stored { object: value, borrowed: false })).map_err(failure)?;
    out.write(id);
    Ok(())
});

export!(reiny_exec_registry_new(out: *mut u64) {
    output(out)?;
    let id = insert(Object::Registry(Mutex::new(Registry { sealed: false, factories: std::collections::BTreeMap::new() })), false)?;
    out.write(id);
    Ok(())
});

unsafe fn register(
    registry: u64,
    name: Bytes,
    revision: Bytes,
    table: *const FactoryV1,
    token: Option<u64>,
) -> Result<()> {
    if table.is_null() || !table.is_aligned() {
        return Err(Error::new(INVALID, "invalid factory table"));
    }
    // Read only the size prefix before assuming the full v1 allocation exists.
    // SAFETY: [FFI/bounds] registration promises a readable size prefix; its
    // alignment and non-null address were checked above.
    if usize::try_from(unsafe { table.cast::<u32>().read() }).ok()
        != Some(std::mem::size_of::<FactoryV1>())
    {
        return Err(Error::new(INVALID, "unsupported factory table size"));
    }
    // SAFETY: [FFI/bounds] exact size was checked; the caller guarantees that
    // size bytes of initialized table storage remain readable for this call.
    let table = unsafe { table.read() };
    table.validate()?;
    // SAFETY: [bounds/lifetime] registration borrows the header's valid slices.
    let (name, revision) = unsafe { (name.text()?, revision.text()?) };
    if name.is_empty() || revision.is_empty() {
        return Err(Error::new(INVALID, "empty factory identity"));
    }
    let released = if let Some(token) = token {
        if table.release_factory.is_some() {
            return Err(Error::new(
                INVALID,
                "notified registration forbids a foreign finalizer",
            ));
        }
        let value = object(token)?;
        Some(variant!(value, ReleaseToken).clone())
    } else {
        None
    };
    let value = object(registry)?;
    let mut registry = lock(variant!(value, Registry))?;
    if registry.sealed || registry.factories.contains_key(&name) {
        return Err(Error::new(
            STATE,
            "registry sealed or factory already registered",
        ));
    }
    if let Some(released) = &released {
        released
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .map_err(|_| Error::new(STATE, "factory release token already bound"))?;
    }
    registry
        .factories
        .insert(name, Arc::new(Factory::new(table, revision, released)));
    Ok(())
}

export!(reiny_exec_register(registry: u64, name: Bytes, revision: Bytes, table: *const FactoryV1) {
    register(registry, name, revision, table, None)
});

export!(reiny_exec_register_notified(registry: u64, name: Bytes, revision: Bytes, table: *const FactoryV1, token: u64) {
    register(registry, name, revision, table, Some(token))
});

export!(reiny_exec_factory_release_token_new(out: *mut u64) {
    output(out)?;
    out.write(insert(Object::ReleaseToken(Arc::new(std::sync::atomic::AtomicU8::new(0))), false)?);
    Ok(())
});

export!(reiny_exec_factory_released(token: u64, out: *mut u32) {
    output(out)?;
    let value = object(token)?;
    let state = variant!(value, ReleaseToken).load(std::sync::atomic::Ordering::SeqCst);
    out.write(u32::from(state == 2));
    Ok(())
});

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_exec_registry_seal(handle: u64) -> i32 {
    boundary(|| {
        let value = object(handle)?;
        lock(variant!(value, Registry))?.sealed = true;
        Ok(())
    })
}

export!(reiny_exec_schema_new(name: Bytes, fingerprint_version: u16, fingerprint: u64, out: *mut u64) {
    output(out)?;
    let schema = reiny::encoded::EncodedType::new(name.text()?, fingerprint_version, fingerprint)
        .map_err(|e| Error::new(SCHEMA, e.to_string()))?;
    out.write(insert(Object::Schema(schema), false)?);
    Ok(())
});

export!(reiny_exec_error_set(sink: u64, code: i32, message: Bytes) {
    if code >= 0 { return Err(Error::new(INVALID, "callback error code must be negative")); }
    let text = message.text()?;
    let value = object(sink)?;
    *lock(variant!(value, ErrorSink))? = Some(Error::new(code, text));
    Ok(())
});

export!(reiny_exec_buffer_copy(handle: u64, dest: *mut u8, capacity: usize, required: *mut usize) {
    let value = object(handle)?;
    copy_out(variant!(value, Buffer), dest, capacity, required)
});

export!(reiny_exec_context_info(handle: u64, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let context = variant!(value, Context);
    let json = serde_json::json!({
        "id": context.inner.id(), "domain": context.inner.domain(),
        "config": context.inner.config_table(), "config_origin": context.inner.config_dir()
    });
    out.write(insert(Object::Buffer(serde_json::to_vec(&json).map_err(failure)?), false)?);
    Ok(())
});

export!(reiny_exec_context_is_stopping(handle: u64, out: *mut u32) {
    output(out)?;
    let value = object(handle)?;
    out.write(u32::from(*variant!(value, Context).shutdown.borrow()));
    Ok(())
});

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_exec_context_wait_stop(handle: u64, timeout_ns: u64) -> i32 {
    boundary(|| {
        let value = object(handle)?;
        let context = variant!(value, Context);
        wait(timeout_ns, async {
            context.stopped().await;
            Ok(())
        })
    })
}
