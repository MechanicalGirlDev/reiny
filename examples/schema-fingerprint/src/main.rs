mod descriptors;
mod live;

use anyhow::{Context, Result, ensure};
use prost::Message;
use prost_types::FileDescriptorSet;

reiny::schema!();

fn generated_parity<T: reiny::MessageType>() -> Result<()> {
    let descriptor = T::DESCRIPTOR.context("missing generated descriptor")?;
    ensure!(
        reiny_build::message_fingerprint(descriptor.file_set, T::TYPE_NAME)?
            == Some(T::SCHEMA_FINGERPRINT),
        "generated fingerprint disagrees with descriptor API for {}",
        T::TYPE_NAME
    );
    Ok(())
}

fn fingerprint(set: &FileDescriptorSet) -> Result<u64> {
    reiny_build::message_fingerprint(&set.encode_to_vec(), "qa.Envelope")?
        .context("missing Envelope")
}

#[tokio::main]
async fn main() -> Result<()> {
    generated_parity::<exported_types::Envelope>()?;
    generated_parity::<exported_types::Node>()?;
    generated_parity::<exported_types::Left>()?;
    generated_parity::<exported_types::Right>()?;
    for (root, changed_type) in [("qa.Node", "Child"), ("qa.Left", "Right")] {
        let mut set = FileDescriptorSet::decode(REINY_DESCRIPTORS)?;
        let before = reiny_build::message_fingerprint(REINY_DESCRIPTORS, root)?;
        let changed = set
            .file
            .iter_mut()
            .flat_map(|file| &mut file.message_type)
            .find(|message| message.name() == changed_type)
            .context("missing generated referenced type")?;
        changed
            .field
            .push(descriptors::field("changed", 99, 13, None));
        ensure!(
            reiny_build::message_fingerprint(&set.encode_to_vec(), root)? != before,
            "generated recursive closure missed a referenced change for {root}"
        );
    }
    // Given a root containing a referenced message and enum.
    let original = descriptors::fixture();
    let expected = fingerprint(&original)?;
    ensure!(
        expected == <exported_types::Envelope as reiny::MessageType>::SCHEMA_FINGERPRINT,
        "hand-built descriptor and protoc descriptor disagree"
    );
    let mut nested = original.clone();
    nested.file[0].message_type[1].field[0].r#type = Some(4);

    // When only a nested message changes, its root must change too.
    ensure!(
        fingerprint(&nested)? != expected,
        "nested message change was not detected"
    );
    let mut enumeration = original.clone();
    enumeration.file[0].enum_type[0].value[1].number = Some(2);
    ensure!(
        fingerprint(&enumeration)? != expected,
        "referenced enum change was not detected"
    );

    // Then declaration order and unrelated definitions must not change identity.
    let mut reordered = original.clone();
    reordered.file[0].message_type.reverse();
    for message in &mut reordered.file[0].message_type {
        message.field.reverse();
    }
    ensure!(
        fingerprint(&reordered)? == expected,
        "order changed identity"
    );
    let mut unrelated = original.clone();
    unrelated.file[0]
        .message_type
        .push(prost_types::DescriptorProto {
            name: Some("Unrelated".to_owned()),
            field: vec![descriptors::field("different", 1, 9, None)],
            ..prost_types::DescriptorProto::default()
        });
    ensure!(
        fingerprint(&unrelated)? == expected,
        "unreachable type changed identity"
    );

    let mut recursive = original.clone();
    recursive.file[0].message_type[1]
        .field
        .push(descriptors::field("parent", 3, 11, Some(".qa.Envelope")));
    let recursive_hash = fingerprint(&recursive)?;
    ensure!(
        recursive_hash != expected,
        "recursive reference change was not detected"
    );
    recursive.file[0].message_type.reverse();
    ensure!(
        fingerprint(&recursive)? == recursive_hash,
        "cycle order changed identity"
    );

    let mut missing = original.clone();
    missing.file[0].message_type[0].field[0].type_name = Some(".qa.Missing".to_owned());
    ensure!(
        fingerprint(&missing).is_err(),
        "missing reference was accepted"
    );
    if std::env::args().nth(1).as_deref() == Some("canonical") {
        println!("SCHEMA_CANONICAL_OK");
        return Ok(());
    }
    live::verify(
        expected,
        [fingerprint(&nested)?, fingerprint(&enumeration)?],
    )
    .await?;
    println!("SCHEMA_CLOSURE_OK");
    Ok(())
}
