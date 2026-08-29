//! 同形の型の自動写像 —— proto の descriptor(`Topic::DESCRIPTOR`)を経由して、フィールド名で
//! serde の構造体(ROS の msg)と行き来する。
//!
//! 経路: prost bytes → `DynamicMessage` → serde(proto のフィールド名のまま、`snake_case`)→ `R`。
//! 逆は `R` → serde → `DynamicMessage` → prost bytes → `T`。名前か型が合わなければ変換が
//! 失敗し、その 1 件は警告して捨てる(bridge は落とさない)。

use prost::Message as ProstMessage;
use prost_reflect::{
    DescriptorPool, DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions,
};
use reiny::{Result, Topic};
use ros2_client::Message;

/// `T::DESCRIPTOR` から `T` の [`MessageDescriptor`] を引く。descriptor を持たない型はエラー。
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

/// prost の `T` → ROS の `R`。
pub(crate) fn to_ros<T: ProstMessage, R: Message>(
    descriptor: &MessageDescriptor,
    value: &T,
) -> Result<R> {
    let dynamic = DynamicMessage::decode(descriptor.clone(), value.encode_to_vec().as_slice())?;
    let json = dynamic.serialize_with_options(
        serde_json::value::Serializer,
        &SerializeOptions::new().use_proto_field_name(true),
    )?;
    Ok(serde_json::from_value(json)?)
}

/// ROS の `R` → prost の `T`。
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
