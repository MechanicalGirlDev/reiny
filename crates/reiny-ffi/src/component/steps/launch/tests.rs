use super::*;
mod notified;
use crate::component::steps::{
    Sink, StepSchema, Tick, reiny_step_output_set, reiny_step_value_copy, reiny_step_value_new,
};
use crate::component::{CALLBACK, HANDLE, Mutex, Result, insert};

fn table() -> Result<StepFactoryV1> {
    extern "C" fn create(_: *mut c_void, _: u64, _: *mut *mut c_void, _: u64) -> i32 {
        CALLBACK
    }
    extern "C" fn hook(_: *mut c_void, _: u64, _: u64) -> i32 {
        CALLBACK
    }
    extern "C" fn step(_: *mut c_void, _: u64, _: *const Tick, _: u64, _: u64, _: u64) -> i32 {
        CALLBACK
    }
    extern "C" fn stop(_: *mut c_void, _: u64, _: u32, _: u64) -> i32 {
        CALLBACK
    }
    extern "C" fn destroy(_: *mut c_void) {}
    Ok(StepFactoryV1 {
        struct_size: u32::try_from(std::mem::size_of::<StepFactoryV1>()).map_err(failure)?,
        abi_major: 1,
        flags: 0,
        reserved: 0,
        factory_data: std::ptr::null_mut(),
        create: Some(create),
        prepare: Some(hook),
        step: Some(step),
        stop: Some(stop),
        destroy_instance: Some(destroy),
        release_factory: None,
    })
}

#[test]
fn step_table_prefix_and_copied_registration_are_safe() -> Result<()> {
    let mut table = table()?;
    let name = Bytes {
        data: b"step".as_ptr(),
        len: 4,
    };
    let mut registry = 0;
    let schema = insert(
        Object::StepSchema(StepSchema {
            inputs: std::collections::BTreeMap::new(),
            outputs: std::collections::BTreeMap::new(),
        }),
        false,
    )?;
    // SAFETY: [FFI/bounds/aliasing] disjoint live scalar, table, and static text.
    unsafe {
        assert_eq!(abi::reiny_exec_registry_new(&raw mut registry), 0);
        assert_eq!(
            reiny_step_register(registry, name, name, schema, std::ptr::null()),
            INVALID
        );
        table.struct_size = 4;
        assert_eq!(
            reiny_step_register(registry, name, name, schema, &raw const table),
            INVALID
        );
        table.struct_size = u32::try_from(std::mem::size_of::<StepFactoryV1>()).map_err(failure)?;
        assert_eq!(
            reiny_step_register(registry, name, name, schema, &raw const table),
            0
        );
    }
    table.step = None;
    assert!(table.step.is_none());
    let registry_value = object(registry)?;
    assert!(
        lock(variant!(registry_value, Registry))?.factories["step"]
            .step
            .is_some()
    );
    assert_eq!(abi::reiny_exec_release(schema), 0);
    assert_eq!(abi::reiny_exec_release(registry), 0);
    Ok(())
}

#[test]
fn immutable_step_bytes_and_borrowed_output_sink_preserve_ownership() -> Result<()> {
    let schema = insert(
        Object::Schema(reiny::encoded::EncodedType::new("test.Number", 2, 42).map_err(failure)?),
        false,
    )?;
    let bytes = [0, 255, 10];
    let mut value = 0;
    let mut required = std::mem::MaybeUninit::<usize>::uninit();
    let mut copy = [0; 3];
    // SAFETY: [bounds/aliasing] all inputs and outputs are disjoint allocations;
    // required is writable uninitialized scalar storage.
    unsafe {
        assert_eq!(
            reiny_step_value_new(
                schema,
                Bytes {
                    data: bytes.as_ptr(),
                    len: bytes.len()
                },
                &raw mut value
            ),
            0
        );
        assert_eq!(
            reiny_step_value_copy(
                value,
                schema,
                copy.as_mut_ptr(),
                copy.len(),
                required.as_mut_ptr()
            ),
            0
        );
        assert_eq!(required.assume_init(), 3);
    }
    assert_eq!(copy, bytes);
    let sink = insert(
        Object::StepSink(Mutex::new(Sink {
            owner: std::thread::current().id(),
            outputs: std::collections::BTreeMap::new(),
        })),
        true,
    )?;
    let name = Bytes {
        data: b"out".as_ptr(),
        len: 3,
    };
    let mut cloned = 123;
    // SAFETY: [bounds/aliasing] static name and disjoint writable scalar.
    unsafe {
        assert_eq!(abi::reiny_exec_clone(sink, &raw mut cloned), STATE);
        assert_eq!(reiny_step_output_set(sink, name, value), 0);
        assert_eq!(reiny_step_output_set(sink, name, value), STATE);
    }
    assert_eq!(cloned, 123);
    assert_eq!(abi::reiny_exec_release(sink), HANDLE);
    drop(crate::c_api::remove(sink, true).map_err(failure)?);
    assert_eq!(abi::reiny_exec_release(value), 0);
    assert_eq!(abi::reiny_exec_release(schema), 0);
    assert!(
        Time {
            secs: u64::MAX,
            nanos: 999_999_999
        }
        .duration()
        .is_ok()
    );
    assert!(
        Time {
            secs: 0,
            nanos: 1_000_000_000
        }
        .duration()
        .is_err()
    );
    Ok(())
}
