//! Automatic mapping between identically shaped types — through the proto descriptor
//! (`Topic::DESCRIPTOR`), by field name, to and from a serde struct (a ROS msg).
//!
//! The path is prost bytes → `DynamicMessage` → serde (proto field names as they are, `snake_case`) →
//! `R`. Back the other way it is `R` → serde → `DynamicMessage` → prost bytes → `T`. If a name or a
//! type does not line up the conversion fails, and that one message is warned about and dropped (the

use prost::Message as ProstMessage;
use prost_reflect::{
    DescriptorPool, DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions,
};
use reiny::{Result, Topic};
use ros2_client::Message;

/// Look up `T`'s [`MessageDescriptor`] in `T::DESCRIPTOR`. A type without a descriptor is an error.
pub(crate) fn descriptor<T: Topic>() -> Result<MessageDescriptor> {
    let Some(descriptor) = T::DESCRIPTOR else {
        anyhow::bail!(
            "{}: no DESCRIPTOR (hand-written Topic impl?); use the closure form",
            T::TYPE
        );
    };
    let pool = DescriptorPool::decode(descriptor.file_set)
        .map_err(|e| anyhow::anyhow!("{}: bad descriptor set: {e}", T::TYPE))?;
    pool.get_message_by_name(descriptor.message).ok_or_else(|| {
        anyhow::anyhow!(
            "{}: message '{}' not in its descriptor set",
            T::TYPE,
            descriptor.message
        )
    })
}

/// prost's `T` → ROS's `R`.
pub(crate) fn to_ros<T: ProstMessage, R: Message>(
    descriptor: &MessageDescriptor,
    value: &T,
) -> Result<R> {
    let dynamic = DynamicMessage::decode(descriptor.clone(), value.encode_to_vec().as_slice())?;
    let json = dynamic.serialize_with_options(
        serde_json::value::Serializer,
        // Every field has to appear, defaults included. prost-reflect omits proto3 defaults by
        // default, and a ROS msg is a fixed struct with no optional fields — so skipping them turns
        // a perfectly ordinary message (a pose at the origin, a zero velocity, an empty string) into
        // a "missing field" deserialization failure, and the bridge would drop it.
        &SerializeOptions::new()
            .use_proto_field_name(true)
            .skip_default_fields(false),
    )?;
    Ok(serde_json::from_value(json)?)
}

/// ROS's `R` → prost's `T`.
pub(crate) fn from_ros<R: Message, T: ProstMessage + Default>(
    descriptor: &MessageDescriptor,
    value: &R,
) -> Result<T> {
    let json = serde_json::to_value(value)?;
    let dynamic = DynamicMessage::deserialize_with_options(
        descriptor.clone(),
        json,
        &DeserializeOptions::new().deny_unknown_fields(false),
    )?;
    Ok(T::decode(dynamic.encode_to_vec().as_slice())?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto,
    };
    use reiny::Descriptor;
    use serde::{Deserialize, Serialize};

    /// The proto side: three doubles, as `reiny-build` would generate them.
    #[derive(Clone, PartialEq, ProstMessage)]
    struct Vec3 {
        #[prost(double, tag = "1")]
        x: f64,
        #[prost(double, tag = "2")]
        y: f64,
        #[prost(double, tag = "3")]
        z: f64,
    }

    /// The ROS side: the same field names, so the mapping needs no code at all.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct RosVec3 {
        x: f64,
        y: f64,
        z: f64,
    }
    impl Message for RosVec3 {}

    /// A ROS struct with a field the proto does not have. Unknown fields are not denied, so this is
    /// the "the ROS message carries something extra" case.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct RosVec3Extra {
        x: f64,
        y: f64,
        z: f64,
        frame_id: String,
    }
    impl Message for RosVec3Extra {}

    /// A ROS struct whose names do not line up at all.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct RosOther {
        qx: f64,
    }
    impl Message for RosOther {}

    fn field(name: &str, number: i32) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            json_name: Some(name.to_string()),
            number: Some(number),
            label: Some(field_descriptor_proto::Label::Optional as i32),
            r#type: Some(field_descriptor_proto::Type::Double as i32),
            ..Default::default()
        }
    }

    /// A `FileDescriptorSet` for `hs.Vec3`, built by hand so the test needs no protoc.
    fn file_set() -> Vec<u8> {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("hs/vec3.proto".to_string()),
                package: Some("hs".to_string()),
                syntax: Some("proto3".to_string()),
                message_type: vec![DescriptorProto {
                    name: Some("Vec3".to_string()),
                    field: vec![field("x", 1), field("y", 2), field("z", 3)],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn vec3_descriptor() -> MessageDescriptor {
        DescriptorPool::decode(file_set().as_slice())
            .expect("the hand-built set decodes")
            .get_message_by_name("hs.Vec3")
            .expect("hs.Vec3 is in it")
    }

    /// The round trip that `export_auto` / `import_auto` are: identical field names map with no code,
    /// and the values survive both directions.
    #[test]
    fn identical_shapes_map_both_ways() {
        let d = vec3_descriptor();
        let proto = Vec3 {
            x: 1.5,
            y: -2.0,
            z: 0.0,
        };

        let ros: RosVec3 = to_ros(&d, &proto).expect("proto → ros");
        assert_eq!(
            ros,
            RosVec3 {
                x: 1.5,
                y: -2.0,
                z: 0.0
            }
        );

        let back: Vec3 = from_ros(&d, &ros).expect("ros → proto");
        assert_eq!(back, proto);
    }

    /// Unknown fields are not denied, so a ROS message carrying more than the proto does still maps
    /// (the extra field is simply dropped).
    #[test]
    fn a_ros_field_the_proto_lacks_is_ignored() {
        let d = vec3_descriptor();
        let ros = RosVec3Extra {
            x: 1.0,
            y: 2.0,
            z: 3.0,
            frame_id: "base_link".to_string(),
        };
        let proto: Vec3 = from_ros(&d, &ros).expect("the extra field is ignored");
        assert_eq!(
            proto,
            Vec3 {
                x: 1.0,
                y: 2.0,
                z: 3.0
            }
        );
    }

    /// Names that do not line up fail the conversion rather than producing zeros — a silently
    /// default-valued message is exactly what a bridge must not publish.
    #[test]
    fn mismatched_names_fail_instead_of_defaulting() {
        let d = vec3_descriptor();
        let err = to_ros::<Vec3, RosOther>(
            &d,
            &Vec3 {
                x: 1.0,
                y: 2.0,
                z: 3.0,
            },
        )
        .expect_err("the ROS struct has no matching field");
        assert!(!err.to_string().is_empty());
    }

    /// The closure form exists precisely for types the automatic path cannot serve, so the error has
    /// to say so instead of failing somewhere further down.
    #[test]
    fn a_type_without_a_descriptor_points_at_the_closure_form() {
        struct HandWritten;
        impl Topic for HandWritten {
            const TYPE: &'static str = "HandWritten";
        }

        let err = descriptor::<HandWritten>().expect_err("no DESCRIPTOR");
        let text = err.to_string();
        assert!(text.contains("HandWritten"), "{text}");
        assert!(text.contains("closure form"), "{text}");
    }

    /// Bytes that are not a descriptor set are reported as such, naming the type.
    #[test]
    fn a_broken_descriptor_set_is_reported() {
        struct Broken;
        impl Topic for Broken {
            const TYPE: &'static str = "Broken";
            const DESCRIPTOR: Option<Descriptor> = Some(Descriptor {
                message: "hs.Broken",
                file_set: b"not-a-descriptor-set",
            });
        }

        let err = descriptor::<Broken>().expect_err("undecodable set");
        let text = err.to_string();
        assert!(text.contains("Broken"), "{text}");
        assert!(text.contains("bad descriptor set"), "{text}");
    }
}
