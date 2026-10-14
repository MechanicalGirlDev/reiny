//! Versioned compatibility metadata, deliberately independent of prost serialization and Rust Debug.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    FileDescriptorSet,
};

#[derive(Clone, Copy)]
enum Definition<'a> {
    Message(&'a DescriptorProto),
    Enum(&'a EnumDescriptorProto),
}

pub(super) struct Index<'a> {
    definitions: BTreeMap<String, (&'a FileDescriptorProto, Definition<'a>)>,
}

impl<'a> Index<'a> {
    pub(super) fn new(set: &'a FileDescriptorSet) -> Result<Self> {
        let mut index = Self {
            definitions: BTreeMap::new(),
        };
        for file in &set.file {
            index.insert_scope(file, file.package(), &file.message_type, &file.enum_type)?;
        }
        Ok(index)
    }

    fn insert_scope(
        &mut self,
        file: &'a FileDescriptorProto,
        scope: &str,
        messages: &'a [DescriptorProto],
        enums: &'a [EnumDescriptorProto],
    ) -> Result<()> {
        for message in messages {
            let name = self.insert(file, scope, message.name(), Definition::Message(message))?;
            self.insert_scope(file, &name, &message.nested_type, &message.enum_type)?;
        }
        for enumeration in enums {
            self.insert(
                file,
                scope,
                enumeration.name(),
                Definition::Enum(enumeration),
            )?;
        }
        Ok(())
    }

    fn insert(
        &mut self,
        file: &'a FileDescriptorProto,
        scope: &str,
        name: &str,
        definition: Definition<'a>,
    ) -> Result<String> {
        ensure!(!name.is_empty(), "unnamed protobuf definition in {scope}");
        let name = if scope.is_empty() {
            name.to_owned()
        } else {
            format!("{scope}.{name}")
        };
        ensure!(
            self.definitions
                .insert(name.clone(), (file, definition))
                .is_none(),
            "duplicate protobuf definition {name}"
        );
        Ok(name)
    }

    pub(super) fn message(&self, root: &str) -> Result<Option<u64>> {
        let Some((_, Definition::Message(_))) = self.definitions.get(root) else {
            return Ok(None);
        };
        let mut pending = vec![root.to_owned()];
        let mut records = BTreeMap::new();
        while let Some(name) = pending.pop() {
            if records.contains_key(&name) {
                continue;
            }
            let (file, definition) = self.definitions[&name];
            let syntax = match file.syntax() {
                "" | "proto2" => "proto2",
                "proto3" => "proto3",
                other => bail!("unsupported protobuf syntax {other:?} for {name}"),
            };
            let mut record = Canonical::default();
            record.string(&name);
            record.string(syntax);
            match definition {
                Definition::Message(message) => {
                    record.byte(1);
                    self.encode_message(&mut record, &name, syntax, message, &mut pending)?;
                }
                Definition::Enum(enumeration) => {
                    record.byte(2);
                    encode_enum(&mut record, &name, enumeration)?;
                }
            }
            records.insert(name, record.0);
        }
        let mut canonical = Canonical::default();
        canonical.string("reiny.schema-fingerprint");
        canonical
            .0
            .extend(reiny_core::SCHEMA_FINGERPRINT_VERSION.to_le_bytes());
        canonical.string(root);
        canonical.count(records.len());
        for record in records.values() {
            canonical.bytes(record);
        }
        // FNV-1a is stable across compiler versions; this is not an authentication hash.
        Ok(Some(
            canonical
                .0
                .iter()
                .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                    (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
                }),
        ))
    }

    fn resolve(&self, scope: &str, reference: &str) -> Result<String> {
        if let Some(absolute) = reference.strip_prefix('.') {
            ensure!(
                self.definitions.contains_key(absolute),
                "unresolved protobuf reference {reference} in {scope}"
            );
            return Ok(absolute.to_owned());
        }
        ensure!(
            !reference.is_empty(),
            "missing protobuf reference in {scope}"
        );
        let mut scope = scope;
        loop {
            let candidate = if scope.is_empty() {
                reference.to_owned()
            } else {
                format!("{scope}.{reference}")
            };
            if self.definitions.contains_key(&candidate) {
                return Ok(candidate);
            }
            if scope.is_empty() {
                bail!("unresolved protobuf reference {reference}");
            }
            scope = scope.rsplit_once('.').map_or("", |(parent, _)| parent);
        }
    }

    fn encode_message(
        &self,
        out: &mut Canonical,
        name: &str,
        syntax: &str,
        message: &DescriptorProto,
        pending: &mut Vec<String>,
    ) -> Result<()> {
        ensure!(
            message.extension.is_empty() && message.extension_range.is_empty(),
            "protobuf extensions are unsupported in reachable message {name}"
        );
        out.boolean(
            message
                .options
                .as_ref()
                .is_some_and(prost_types::MessageOptions::map_entry),
        );
        out.boolean(
            message
                .options
                .as_ref()
                .is_some_and(prost_types::MessageOptions::message_set_wire_format),
        );
        out.ranges(
            message
                .reserved_range
                .iter()
                .map(|range| (range.start(), range.end())),
        );
        out.strings(message.reserved_name.iter().map(String::as_str));
        let oneofs: BTreeSet<_> = message
            .oneof_decl
            .iter()
            .map(prost_types::OneofDescriptorProto::name)
            .collect();
        ensure!(
            oneofs.len() == message.oneof_decl.len() && !oneofs.contains(""),
            "invalid or duplicate oneof name in {name}"
        );
        out.strings(oneofs);
        let mut fields: Vec<_> = message.field.iter().collect();
        fields.sort_by_key(|field| field.number());
        let mut numbers = BTreeSet::new();
        let mut names = BTreeSet::new();
        out.count(fields.len());
        for field in fields {
            ensure!(
                field.number() > 0 && numbers.insert(field.number()),
                "invalid or duplicate field number in {name}"
            );
            ensure!(
                !field.name().is_empty() && names.insert(field.name()),
                "invalid or duplicate field name in {name}"
            );
            self.encode_field(out, name, syntax, message, field, pending)
                .with_context(|| format!("fingerprinting {name}.{}", field.name()))?;
        }
        Ok(())
    }

    fn field_type(
        &self,
        name: &str,
        field: &FieldDescriptorProto,
    ) -> Result<(Type, Option<String>)> {
        let reference = field
            .type_name
            .as_deref()
            .map(|reference| self.resolve(name, reference))
            .transpose()?;
        let inferred = reference
            .as_ref()
            .map(|reference| match self.definitions[reference].1 {
                Definition::Message(_) => Type::Message,
                Definition::Enum(_) => Type::Enum,
            });
        let kind = field
            .r#type
            .map(Type::try_from)
            .transpose()
            .context("invalid field type")?
            .or(inferred)
            .context("missing field type")?;
        match kind {
            Type::Message | Type::Group | Type::Enum => {
                ensure!(
                    inferred
                        == Some(if kind == Type::Group {
                            Type::Message
                        } else {
                            kind
                        }),
                    "missing or wrong-kind reference for {}",
                    field.name()
                );
            }
            _ => ensure!(reference.is_none(), "scalar field has a type reference"),
        }
        Ok((kind, reference))
    }

    fn encode_field(
        &self,
        out: &mut Canonical,
        name: &str,
        syntax: &str,
        message: &DescriptorProto,
        field: &FieldDescriptorProto,
        pending: &mut Vec<String>,
    ) -> Result<()> {
        ensure!(
            field.extendee.is_none(),
            "protobuf extension fields are unsupported"
        );
        let (kind, reference) = self.field_type(name, field)?;
        let label = field
            .label
            .map(Label::try_from)
            .transpose()
            .context("invalid field label")?
            .unwrap_or(Label::Optional);
        let oneof = field
            .oneof_index
            .map(|index| {
                usize::try_from(index)
                    .ok()
                    .and_then(|index| message.oneof_decl.get(index))
                    .with_context(|| format!("invalid oneof index {index}"))
            })
            .transpose()?;
        if field.proto3_optional() {
            ensure!(
                syntax == "proto3" && label == Label::Optional && oneof.is_some(),
                "invalid proto3 optional field"
            );
            ensure!(
                message
                    .field
                    .iter()
                    .filter(|other| other.oneof_index == field.oneof_index)
                    .count()
                    == 1,
                "proto3 optional oneof must have one field"
            );
        }
        ensure!(
            oneof.is_none() || label == Label::Optional,
            "oneof field must be optional"
        );
        ensure!(
            syntax != "proto3" || label != Label::Required,
            "proto3 field cannot be required"
        );
        let packable = !matches!(
            kind,
            Type::String | Type::Bytes | Type::Message | Type::Group
        );
        let packed_option = field.options.as_ref().and_then(|options| options.packed);
        ensure!(
            packed_option != Some(true) || (packable && label == Label::Repeated),
            "packed requires a repeated scalar or enum"
        );
        let packed =
            packable && label == Label::Repeated && packed_option.unwrap_or(syntax == "proto3");
        let presence = label != Label::Repeated
            && (syntax == "proto2"
                || oneof.is_some()
                || matches!(kind, Type::Message | Type::Group));
        out.integer(field.number());
        out.string(field.name());
        out.integer(kind as i32);
        out.integer(label as i32);
        out.optional(reference.as_deref());
        out.optional(oneof.map(prost_types::OneofDescriptorProto::name));
        out.boolean(field.proto3_optional());
        out.boolean(presence);
        out.boolean(packed);
        out.optional(field.default_value.as_deref());
        let json_name = field.json_name.clone().unwrap_or_else(|| {
            let mut uppercase = false;
            field
                .name()
                .chars()
                .filter_map(|character| {
                    if character == '_' {
                        uppercase = true;
                        None
                    } else {
                        let character = if uppercase {
                            character.to_ascii_uppercase()
                        } else {
                            character
                        };
                        uppercase = false;
                        Some(character)
                    }
                })
                .collect()
        });
        out.string(&json_name);
        if let Some(reference) = reference {
            pending.push(reference);
        }
        Ok(())
    }
}

fn encode_enum(out: &mut Canonical, name: &str, enumeration: &EnumDescriptorProto) -> Result<()> {
    let default = enumeration
        .value
        .first()
        .with_context(|| format!("empty enum {name}"))?;
    out.integer(default.number());
    out.boolean(
        enumeration
            .options
            .as_ref()
            .is_some_and(prost_types::EnumOptions::allow_alias),
    );
    out.ranges(
        enumeration
            .reserved_range
            .iter()
            .map(|range| (range.start(), range.end())),
    );
    out.strings(enumeration.reserved_name.iter().map(String::as_str));
    let mut values: Vec<_> = enumeration.value.iter().collect();
    values.sort_by_key(|value| (value.number(), value.name()));
    out.count(values.len());
    let mut names = BTreeSet::new();
    for value in values {
        ensure!(
            !value.name().is_empty() && names.insert(value.name()),
            "invalid or duplicate enum value name in {name}"
        );
        out.integer(value.number());
        out.string(value.name());
    }
    Ok(())
}

/// Every variable-length item is length-prefixed; integers are fixed-width little-endian.
#[derive(Default)]
struct Canonical(Vec<u8>);

impl Canonical {
    fn byte(&mut self, value: u8) {
        self.0.push(value);
    }
    fn boolean(&mut self, value: bool) {
        self.byte(u8::from(value));
    }
    fn integer(&mut self, value: i32) {
        self.0.extend(value.to_le_bytes());
    }
    fn count(&mut self, value: usize) {
        self.0.extend((value as u64).to_le_bytes());
    }
    fn bytes(&mut self, value: &[u8]) {
        self.count(value.len());
        self.0.extend(value);
    }
    fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }
    fn optional(&mut self, value: Option<&str>) {
        self.boolean(value.is_some());
        if let Some(value) = value {
            self.string(value);
        }
    }
    fn strings<'a>(&mut self, values: impl IntoIterator<Item = &'a str>) {
        let mut values: Vec<_> = values.into_iter().collect();
        values.sort_unstable();
        self.count(values.len());
        for value in values {
            self.string(value);
        }
    }
    fn ranges(&mut self, values: impl Iterator<Item = (i32, i32)>) {
        let mut values: Vec<_> = values.collect();
        values.sort_unstable();
        self.count(values.len());
        for (start, end) in values {
            self.integer(start);
            self.integer(end);
        }
    }
}
