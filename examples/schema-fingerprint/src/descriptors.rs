use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet,
};

pub fn field(name: &str, number: i32, kind: i32, reference: Option<&str>) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_owned()),
        number: Some(number),
        label: Some(1),
        r#type: Some(kind),
        type_name: reference.map(str::to_owned),
        ..FieldDescriptorProto::default()
    }
}

pub fn fixture() -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("schema.proto".to_owned()),
            package: Some("qa".to_owned()),
            syntax: Some("proto3".to_owned()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Envelope".to_owned()),
                    field: vec![field("child", 1, 11, Some(".qa.Child"))],
                    ..DescriptorProto::default()
                },
                DescriptorProto {
                    name: Some("Child".to_owned()),
                    field: vec![
                        field("value", 1, 13, None),
                        field("kind", 2, 14, Some(".qa.Kind")),
                    ],
                    ..DescriptorProto::default()
                },
            ],
            enum_type: vec![EnumDescriptorProto {
                name: Some("Kind".to_owned()),
                value: vec![
                    EnumValueDescriptorProto {
                        name: Some("ZERO".to_owned()),
                        number: Some(0),
                        ..EnumValueDescriptorProto::default()
                    },
                    EnumValueDescriptorProto {
                        name: Some("FIRST".to_owned()),
                        number: Some(1),
                        ..EnumValueDescriptorProto::default()
                    },
                ],
                ..EnumDescriptorProto::default()
            }],
            ..FileDescriptorProto::default()
        }],
    }
}
