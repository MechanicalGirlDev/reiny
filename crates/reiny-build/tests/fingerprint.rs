//! Public descriptor fingerprint compatibility and canonicalization regressions.
#![cfg(feature = "descriptors")]
#![allow(clippy::unwrap_used)]

use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumOptions, EnumValueDescriptorProto,
    FieldDescriptorProto, FieldOptions, FileDescriptorProto, FileDescriptorSet, MessageOptions,
    OneofDescriptorProto,
};
use reiny_build::message_fingerprint;

fn field(name: &str, number: i32, kind: Type, reference: Option<&str>) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(kind as i32),
        label: Some(Label::Optional as i32),
        type_name: reference.map(str::to_owned),
        ..Default::default()
    }
}

fn message(name: &str, fields: Vec<FieldDescriptorProto>) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.into()),
        field: fields,
        ..Default::default()
    }
}

fn fixture() -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("original.proto".into()),
            package: Some("qa".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                message(
                    "Root",
                    vec![field("child", 1, Type::Message, Some(".qa.Child"))],
                ),
                message(
                    "Child",
                    vec![
                        field("some_value", 1, Type::Int32, None),
                        field("kind", 2, Type::Enum, Some(".qa.Kind")),
                    ],
                ),
            ],
            enum_type: vec![EnumDescriptorProto {
                name: Some("Kind".into()),
                value: ["ZERO", "ONE", "TWO"]
                    .into_iter()
                    .zip(0..)
                    .map(|(name, number)| EnumValueDescriptorProto {
                        name: Some(name.into()),
                        number: Some(number),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn hash(set: &FileDescriptorSet) -> u64 {
    message_fingerprint(&set.encode_to_vec(), "qa.Root")
        .unwrap()
        .unwrap()
}

fn error(set: &FileDescriptorSet) -> String {
    format!(
        "{:#}",
        message_fingerprint(&set.encode_to_vec(), "qa.Root").unwrap_err()
    )
}

#[test]
fn referenced_children_enums_and_nested_definitions_change_root() {
    let original = fixture();
    let baseline = hash(&original);
    let mut changed = original.clone();
    changed.file[0].message_type[1].field[0].r#type = Some(Type::Uint32 as i32);
    assert_ne!(baseline, hash(&changed));
    let mut changed = original.clone();
    changed.file[0].enum_type[0].value[1].number = Some(10);
    assert_ne!(baseline, hash(&changed));
    let mut nested = original;
    let child = nested.file[0].message_type.remove(1);
    nested.file[0].message_type[0].nested_type.push(child);
    nested.file[0].message_type[0].field[0].type_name = Some(".qa.Root.Child".into());
    let baseline = hash(&nested);
    nested.file[0].message_type[0].nested_type[0].field[0].r#type = Some(Type::Uint32 as i32);
    assert_ne!(baseline, hash(&nested));
}

#[test]
fn maps_follow_synthetic_entry_and_value_definitions() {
    let mut set = fixture();
    let mut entry = message(
        "ItemsEntry",
        vec![
            field("key", 1, Type::String, None),
            field("value", 2, Type::Message, Some(".qa.Child")),
        ],
    );
    entry.options = Some(MessageOptions {
        map_entry: Some(true),
        ..Default::default()
    });
    set.file[0].message_type[0].nested_type.push(entry);
    let mut items = field("items", 3, Type::Message, Some(".qa.Root.ItemsEntry"));
    items.label = Some(Label::Repeated as i32);
    set.file[0].message_type[0].field = vec![items];
    let baseline = hash(&set);
    let mut changed = set.clone();
    changed.file[0].message_type[1].field[0].r#type = Some(Type::Uint32 as i32);
    assert_ne!(baseline, hash(&changed));
    let mut changed = set.clone();
    changed.file[0].message_type[0].nested_type[0]
        .options
        .as_mut()
        .unwrap()
        .map_entry = Some(false);
    assert_ne!(baseline, hash(&changed));
    set.file[0].message_type[0].nested_type[0].field[0].r#type = Some(Type::Uint32 as i32);
    assert_ne!(baseline, hash(&set));
}

#[test]
fn self_and_mutual_recursion_terminate_and_include_each_definition() {
    let mut set = fixture();
    set.file[0].message_type[0]
        .field
        .push(field("self", 2, Type::Message, Some(".qa.Root")));
    let self_hash = hash(&set);
    set.file[0].message_type[1]
        .field
        .push(field("parent", 3, Type::Message, Some(".qa.Root")));
    let mutual_hash = hash(&set);
    assert_ne!(self_hash, mutual_hash);
    set.file[0].message_type.reverse();
    assert_eq!(mutual_hash, hash(&set));
    set.file[0].message_type[0].field[0].r#type = Some(Type::Uint32 as i32);
    assert_ne!(mutual_hash, hash(&set));
}

#[test]
fn declaration_order_and_unrelated_source_metadata_are_ignored() {
    let mut set = fixture();
    let root = &mut set.file[0].message_type[0];
    root.oneof_decl = ["left", "right"]
        .into_iter()
        .map(|name| OneofDescriptorProto {
            name: Some(name.into()),
            ..Default::default()
        })
        .collect();
    root.field[0].oneof_index = Some(0);
    let mut other = field("other", 2, Type::Int32, None);
    other.oneof_index = Some(1);
    root.field.push(other);
    set.file.push(FileDescriptorProto {
        package: Some("unrelated".into()),
        message_type: vec![message("Ignore", vec![])],
        ..Default::default()
    });
    let baseline = hash(&set);
    let root = &mut set.file[0].message_type[0];
    root.oneof_decl.reverse();
    for field in &mut root.field {
        field.oneof_index = field.oneof_index.map(|index| 1 - index);
    }
    for message in &mut set.file[0].message_type {
        message.field.reverse();
    }
    set.file[0].message_type.reverse();
    set.file[0].enum_type[0].value.swap(1, 2);
    set.file[0].name = Some("renamed.proto".into());
    set.file[0].source_code_info = Some(prost_types::SourceCodeInfo::default());
    set.file[0].message_type[0].options = Some(MessageOptions {
        deprecated: Some(true),
        ..Default::default()
    });
    set.file[1].message_type[0]
        .field
        .push(field("ignored", 1, Type::Bytes, None));
    set.file.reverse();
    assert_eq!(baseline, hash(&set));
}

#[test]
fn relative_references_and_effective_json_and_packed_are_canonical() {
    let mut set = fixture();
    let baseline = hash(&set);
    set.file[0].message_type[0].field[0].type_name = Some("Child".into());
    set.file[0].message_type[1].field[0].json_name = Some("someValue".into());
    assert_eq!(baseline, hash(&set));
    let value = &mut set.file[0].message_type[1].field[0];
    value.label = Some(Label::Repeated as i32);
    let baseline = hash(&set);
    set.file[0].message_type[1].field[0].options = Some(FieldOptions {
        packed: Some(true),
        ..Default::default()
    });
    assert_eq!(baseline, hash(&set));
    set.file[0].message_type[1].field[0]
        .options
        .as_mut()
        .unwrap()
        .packed = Some(false);
    assert_ne!(baseline, hash(&set));
}

#[test]
fn syntax_presence_defaults_and_json_names_are_semantic() {
    let original = fixture();
    let baseline = hash(&original);
    let mut changed = original.clone();
    changed.file[0].syntax = Some("proto2".into());
    assert_ne!(baseline, hash(&changed));
    let proto2 = hash(&changed);
    changed.file[0].syntax = None;
    assert_eq!(proto2, hash(&changed));
    changed.file[0].message_type[1].field[0].default_value = Some("42".into());
    assert_ne!(proto2, hash(&changed));
    let mut changed = original.clone();
    changed.file[0].message_type[1].field[0].json_name = Some("customValue".into());
    assert_ne!(baseline, hash(&changed));
    let mut changed = original;
    let child = &mut changed.file[0].message_type[1];
    child.oneof_decl.push(OneofDescriptorProto {
        name: Some("_some_value".into()),
        ..Default::default()
    });
    child.field[0].oneof_index = Some(0);
    let explicit_oneof = hash(&changed);
    changed.file[0].message_type[1].field[0].proto3_optional = Some(true);
    assert_ne!(baseline, hash(&changed));
    assert_ne!(explicit_oneof, hash(&changed));
}

#[test]
fn enum_defaults_aliases_and_reserved_declarations_are_semantic() {
    let original = fixture();
    let baseline = hash(&original);
    let mut changed = original.clone();
    changed.file[0].enum_type[0].value.swap(0, 1);
    assert_ne!(baseline, hash(&changed));
    let mut changed = original.clone();
    changed.file[0].enum_type[0].options = Some(EnumOptions {
        allow_alias: Some(true),
        ..Default::default()
    });
    assert_ne!(baseline, hash(&changed));
    let mut changed = original.clone();
    changed.file[0].enum_type[0]
        .reserved_name
        .push("OLD".into());
    assert_ne!(baseline, hash(&changed));
    let mut changed = original.clone();
    changed.file[0].enum_type[0].reserved_range.push(
        prost_types::enum_descriptor_proto::EnumReservedRange {
            start: Some(10),
            end: Some(20),
        },
    );
    assert_ne!(baseline, hash(&changed));
    let mut changed = original.clone();
    changed.file[0].message_type[0]
        .reserved_name
        .push("old".into());
    assert_ne!(baseline, hash(&changed));
    let mut changed = original;
    changed.file[0].message_type[0].reserved_range.push(
        prost_types::descriptor_proto::ReservedRange {
            start: Some(10),
            end: Some(20),
        },
    );
    assert_ne!(baseline, hash(&changed));
}

#[test]
fn missing_root_is_none_but_missing_references_and_duplicates_fail() {
    let original = fixture();
    assert_eq!(
        message_fingerprint(&original.encode_to_vec(), "qa.Missing").unwrap(),
        None
    );
    let mut changed = original.clone();
    changed.file[0].message_type.remove(1);
    assert!(error(&changed).contains("unresolved protobuf reference .qa.Child"));
    let mut changed = original.clone();
    changed.file[0].enum_type.clear();
    assert!(error(&changed).contains("unresolved protobuf reference .qa.Kind"));
    let mut changed = original.clone();
    changed.file.push(original.file[0].clone());
    assert!(error(&changed).contains("duplicate protobuf definition qa.Root"));
    let mut changed = original;
    changed.file[0].enum_type[0].name = Some("Root".into());
    assert!(error(&changed).contains("duplicate protobuf definition qa.Root"));
}

#[test]
fn malformed_oneofs_types_and_unsupported_syntax_fail_clearly() {
    for index in [-1, 0, 3] {
        let mut set = fixture();
        set.file[0].message_type[0].field[0].oneof_index = Some(index);
        assert!(error(&set).contains("invalid oneof index"));
    }
    let mut set = fixture();
    set.file[0].syntax = Some("editions".into());
    assert!(error(&set).contains("unsupported protobuf syntax"));
    let mut set = fixture();
    set.file[0].message_type[0].field[0].r#type = Some(Type::Enum as i32);
    assert!(error(&set).contains("wrong-kind reference"));
    let mut set = fixture();
    set.file[0].message_type[0].extension_range.push(
        prost_types::descriptor_proto::ExtensionRange {
            start: Some(100),
            end: Some(200),
            ..Default::default()
        },
    );
    assert!(error(&set).contains("extensions are unsupported"));
}

#[test]
fn version_two_binary_format_has_fixed_vectors() {
    for (syntax, expected) in [
        ("proto2", 0x4823_a409_cbcd_1d92),
        ("proto3", 0x0624_34f6_c407_00d7),
    ] {
        let set = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                package: Some("qa".into()),
                syntax: Some(syntax.into()),
                message_type: vec![message("Root", vec![])],
                ..Default::default()
            }],
        };
        assert_eq!(hash(&set), expected);
    }
}

#[test]
fn enum_definition_and_reserved_declaration_order_is_canonical() {
    let mut set = fixture();
    let mut other = set.file[0].enum_type[0].clone();
    other.name = Some("Other".into());
    set.file[0].enum_type.push(other);
    set.file[0].message_type[0]
        .field
        .push(field("other", 2, Type::Enum, Some(".qa.Other")));
    set.file[0].message_type[0].reserved_name = vec!["old_a".into(), "old_b".into()];
    set.file[0].message_type[0].reserved_range = vec![
        prost_types::descriptor_proto::ReservedRange {
            start: Some(10),
            end: Some(20),
        },
        prost_types::descriptor_proto::ReservedRange {
            start: Some(30),
            end: Some(40),
        },
    ];
    let baseline = hash(&set);
    set.file[0].enum_type.reverse();
    set.file[0].message_type[0].reserved_name.reverse();
    set.file[0].message_type[0].reserved_range.reverse();
    assert_eq!(baseline, hash(&set));
}

#[test]
fn file_boundaries_and_reference_spelling_do_not_change_the_closure() {
    let mut set = fixture();
    let baseline = hash(&set);
    let child = set.file[0].message_type.remove(1);
    let enumeration = set.file[0].enum_type.remove(0);
    set.file.push(FileDescriptorProto {
        name: Some("child.proto".into()),
        package: Some("qa".into()),
        syntax: Some("proto3".into()),
        message_type: vec![child],
        enum_type: vec![enumeration],
        ..Default::default()
    });
    set.file[0].dependency.push("child.proto".into());
    assert_eq!(baseline, hash(&set));
    set.file.reverse();
    assert_eq!(baseline, hash(&set));
}
