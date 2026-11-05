use super::*;

#[test]
fn job_work_table_tokens_and_single_use_sink_preserve_owned_claims() -> Result<()> {
    extern "C" fn create(
        _: *mut std::ffi::c_void,
        _: u64,
        _: *mut *mut std::ffi::c_void,
        _: u64,
    ) -> i32 {
        CALLBACK
    }
    extern "C" fn run(_: *mut std::ffi::c_void, _: u64, _: u64, _: u64) -> i32 {
        CALLBACK
    }
    extern "C" fn destroy(_: *mut std::ffi::c_void) {}
    let mut table = work::Table {
        struct_size: u32::try_from(std::mem::size_of::<work::Table>()).map_err(failure)?,
        abi_major: 1,
        flags: 0,
        reserved: 0,
        factory_data: std::ptr::null_mut(),
        create: Some(create),
        run: Some(run),
        destroy_instance: Some(destroy),
        release_factory: Some(destroy),
    };
    let (mut token, mut handle, mut released) = (0, 123, 99);
    // SAFETY: [bounds/aliasing] exact initialized table and disjoint live output
    // slots; null finalizer means no foreign work executes in this helper test.
    unsafe {
        assert_eq!(abi::reiny_exec_factory_release_token_new(&raw mut token), 0);
        assert_eq!(
            work::reiny_job_work_new_notified(&raw const table, token, &raw mut handle),
            INVALID
        );
        assert_eq!(handle, 123);
        table.release_factory = None;
        assert_eq!(
            work::reiny_job_work_new_notified(&raw const table, token, std::ptr::null_mut()),
            INVALID
        );
        assert_eq!(
            work::reiny_job_work_new_notified(&raw const table, token, &raw mut handle),
            0
        );
        let mut rejected = 123;
        assert_eq!(
            work::reiny_job_work_new_notified(&raw const table, token, &raw mut rejected),
            STATE
        );
        assert_eq!(rejected, 123);
        assert_eq!(
            abi::reiny_exec_factory_released(token, &raw mut released),
            0
        );
        assert_eq!(released, 0);
        assert_eq!(abi::reiny_exec_release(handle), 0);
        assert_eq!(
            abi::reiny_exec_factory_released(token, &raw mut released),
            0
        );
        assert_eq!(released, 1);
    }
    assert_eq!(abi::reiny_exec_release(token), 0);
    let sink = insert(
        Object::JobSink(Mutex::new(Sink {
            owner: std::thread::current().id(),
            value: None,
        })),
        true,
    )?;
    let mut bytes = [1, 0, 255];
    let input = Bytes {
        data: bytes.as_ptr(),
        len: bytes.len(),
    };
    let mut cloned = 123;
    // SAFETY: [bounds/lifetime] input and writable scalar are disjoint live storage.
    unsafe {
        assert_eq!(reiny_job_succeed(sink, input), 0);
        assert_eq!(reiny_job_succeed(sink, input), STATE);
        assert_eq!(abi::reiny_exec_clone(sink, &raw mut cloned), STATE);
    }
    bytes.fill(9);
    assert_eq!(
        lock(variant!(object(sink)?, JobSink))?.value,
        Some(vec![1, 0, 255])
    );
    assert_eq!(cloned, 123);
    assert_eq!(abi::reiny_exec_release(sink), super::super::HANDLE);
    drop(crate::c_api::remove(sink, true).map_err(failure)?);
    Ok(())
}
