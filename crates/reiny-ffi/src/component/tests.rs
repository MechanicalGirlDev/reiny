//! Pure Rust exercises of the exact unsafe ABI helpers, suitable for Miri.
use super::abi::{Bytes, copy_out, output};
use super::{BUFFER, HANDLE, INVALID, Object, Reservation, Result, failure, insert, object};

#[test]
fn input_slices_validate_utf8_lengths_and_embedded_nuls() -> Result<()> {
    let message = [10, 3, 0, 255, 0];
    let slice = Bytes {
        data: message.as_ptr(),
        len: message.len(),
    };
    // SAFETY: [bounds/lifetime] message is a live allocation with slice.len bytes.
    assert_eq!(unsafe { slice.bytes()? }, message);
    // SAFETY: [bounds/lifetime] same valid allocation; text rejects its contents.
    assert_eq!(unsafe { slice.text() }.err().map(|e| e.code), Some(INVALID));
    let null = Bytes {
        data: std::ptr::null(),
        len: 0,
    };
    // SAFETY: [bounds] the ABI explicitly permits null with zero length.
    let empty = unsafe { null.bytes()? };
    assert_eq!(empty, Vec::<u8>::new());
    let null_nonempty = Bytes {
        data: std::ptr::null(),
        len: 1,
    };
    // SAFETY: [bounds] this deliberately invalid shape is rejected before reading.
    let invalid = unsafe { null_nonempty.bytes() };
    assert_eq!(invalid.err().map(|e| e.code), Some(INVALID));
    Ok(())
}

#[test]
fn copy_outputs_preserve_short_buffers_and_handle_uninitialized_scalars() -> Result<()> {
    let value = [1, 0, 255, 3];
    let mut required = std::mem::MaybeUninit::<usize>::uninit();
    // SAFETY: [initialization/aliasing] required is writable aligned storage;
    // a null, empty destination is the documented size-query form.
    unsafe {
        copy_out(&value, std::ptr::null_mut(), 0, required.as_mut_ptr())?;
    }
    // SAFETY: [initialization] successful copy_out initialized this scalar.
    assert_eq!(unsafe { required.assume_init() }, value.len());
    let mut destination = [77; 2];
    let mut length = 0;
    // SAFETY: [bounds/aliasing] disjoint writable output allocations are live.
    let result = unsafe {
        copy_out(
            &value,
            destination.as_mut_ptr(),
            destination.len(),
            &raw mut length,
        )
    };
    assert_eq!(result.err().map(|e| e.code), Some(BUFFER));
    assert_eq!(destination, [77; 2]);
    assert_eq!(length, 4);
    let mut destination = [0; 4];
    // SAFETY: [bounds/aliasing] disjoint output allocation fits the full snapshot.
    unsafe {
        copy_out(
            &value,
            destination.as_mut_ptr(),
            destination.len(),
            &raw mut length,
        )?;
    }
    assert_eq!(destination, value);
    assert!(output::<u64>(std::ptr::null_mut()).is_err());
    Ok(())
}

#[test]
fn borrowed_claims_and_reused_output_slots_do_not_revive_stale_handles() -> Result<()> {
    let borrowed = insert(Object::Buffer(vec![1]), true)?;
    assert!(crate::c_api::remove(borrowed, false).is_err());
    drop(crate::c_api::remove(borrowed, true).map_err(failure)?);
    let reservation = Reservation::new()?;
    let replacement = reservation.publish(Object::Buffer(vec![2]))?;
    assert_ne!(replacement, borrowed);
    assert_eq!(object(borrowed).err().map(|e| e.code), Some(HANDLE));
    drop(crate::c_api::remove(replacement, false).map_err(failure)?);
    Ok(())
}

#[test]
fn notified_registration_copies_table_and_releases_without_foreign_code() -> Result<()> {
    use super::abi::{
        FactoryV1, reiny_exec_factory_release_token_new, reiny_exec_factory_released,
        reiny_exec_register_notified, reiny_exec_registry_new, reiny_exec_release,
    };
    use std::ffi::c_void;
    extern "C" fn create(_: *mut c_void, _: u64, _: *mut *mut c_void, _: u64) -> i32 {
        super::CALLBACK
    }
    extern "C" fn hook(_: *mut c_void, _: u64, _: u64) -> i32 {
        super::CALLBACK
    }
    extern "C" fn stop(_: *mut c_void, _: u64, _: u32, _: u64) -> i32 {
        super::CALLBACK
    }
    extern "C" fn destroy(_: *mut c_void) {}
    let table = FactoryV1 {
        struct_size: u32::try_from(std::mem::size_of::<FactoryV1>()).map_err(failure)?,
        abi_major: 1,
        flags: 0,
        mode: 1,
        factory_data: std::ptr::null_mut(),
        create: Some(create),
        prepare: Some(hook),
        run: Some(hook),
        stop: Some(stop),
        destroy_instance: Some(destroy),
        release_factory: None,
    };
    let identity = Bytes {
        data: b"factory".as_ptr(),
        len: 7,
    };
    let mut registry = 0;
    let mut token = 0;
    let mut released = 99;
    // SAFETY: [bounds/alignment/lifetime] all scalar outputs and the table
    // refer to disjoint live stack allocations; identity points to static bytes.
    unsafe {
        assert_eq!(reiny_exec_registry_new(&raw mut registry), 0);
        assert_eq!(reiny_exec_factory_release_token_new(&raw mut token), 0);
        assert_eq!(
            reiny_exec_register_notified(registry, identity, identity, &raw const table, token),
            0
        );
        assert_eq!(reiny_exec_factory_released(token, &raw mut released), 0);
    }
    assert_eq!(released, 0);
    assert_eq!(reiny_exec_release(registry), 0);
    // SAFETY: [bounds/alignment] released remains a live writable scalar.
    let status = unsafe { reiny_exec_factory_released(token, &raw mut released) };
    assert_eq!(status, 0);
    assert_eq!(released, 1);
    assert_eq!(reiny_exec_release(token), 0);
    Ok(())
}
