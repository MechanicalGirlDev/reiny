use super::*;

#[test]
fn notified_step_registration_binds_only_on_success_and_preserves_token_ownership() -> Result<()> {
    let mut table = table()?;
    let name = Bytes {
        data: b"step".as_ptr(),
        len: 4,
    };
    let other = Bytes {
        data: b"other".as_ptr(),
        len: 5,
    };
    let schema = insert(
        Object::StepSchema(StepSchema {
            inputs: std::collections::BTreeMap::new(),
            outputs: std::collections::BTreeMap::new(),
        }),
        false,
    )?;
    let (mut registry, mut sealed, mut token, mut spare, mut clone, mut released) =
        (0, 0, 0, 0, 0, 99);
    // SAFETY: [FFI/bounds/aliasing] live table, static strings and disjoint
    // writable output slots satisfy both registration and token contracts.
    unsafe {
        assert_eq!(abi::reiny_exec_registry_new(&raw mut registry), 0);
        assert_eq!(abi::reiny_exec_registry_new(&raw mut sealed), 0);
        assert_eq!(abi::reiny_exec_registry_seal(sealed), 0);
        assert_eq!(abi::reiny_exec_factory_release_token_new(&raw mut token), 0);
        assert_eq!(abi::reiny_exec_factory_release_token_new(&raw mut spare), 0);
        table.release_factory = table.destroy_instance;
        assert_eq!(
            reiny_step_register_notified(registry, name, name, schema, &raw const table, token),
            INVALID
        );
        table.release_factory = None;
        assert_eq!(
            reiny_step_register_notified(sealed, name, name, schema, &raw const table, token),
            STATE
        );
        assert_eq!(
            reiny_step_register_notified(registry, name, name, schema, &raw const table, schema),
            crate::component::KIND
        );
        assert_eq!(
            reiny_step_register_notified(registry, name, name, token, &raw const table, token),
            crate::component::KIND
        );
        assert_eq!(
            reiny_step_register_notified(registry, name, name, schema, &raw const table, token),
            0
        );
        assert_eq!(
            reiny_step_register_notified(registry, other, name, schema, &raw const table, token),
            STATE
        );
        assert_eq!(
            reiny_step_register_notified(registry, name, name, schema, &raw const table, spare),
            STATE
        );
        assert_eq!(
            reiny_step_register_notified(registry, other, name, schema, &raw const table, spare),
            0
        );
        assert_eq!(abi::reiny_exec_clone(token, &raw mut clone), 0);
        assert_eq!(abi::reiny_exec_release(token), 0);
        assert_eq!(
            abi::reiny_exec_factory_released(token, &raw mut released),
            HANDLE
        );
        assert_eq!(released, 99);
        assert_eq!(
            abi::reiny_exec_factory_released(clone, &raw mut released),
            0
        );
        assert_eq!(released, 0);
        assert_eq!(abi::reiny_exec_release(registry), 0);
        assert_eq!(
            abi::reiny_exec_factory_released(clone, &raw mut released),
            0
        );
        assert_eq!(released, 1);
        assert_eq!(
            abi::reiny_exec_factory_released(spare, &raw mut released),
            0
        );
        assert_eq!(released, 1);
    }
    for handle in [sealed, schema, clone, spare] {
        assert_eq!(abi::reiny_exec_release(handle), 0);
    }
    Ok(())
}
