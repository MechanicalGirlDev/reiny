use super::{codec, protocol};

#[cfg(feature = "zenoh")]
mod component;
#[cfg(feature = "zenoh")]
mod fixture;
#[cfg(feature = "zenoh")]
mod loopback;
#[cfg(feature = "zenoh")]
mod rejection;

#[test]
fn strict_json_rejects_duplicate_keys_before_maps_collapse() {
    for json in [
        r#"{"inputs":{"port":1,"port":2}}"#,
        r#"{"inputs":{"port":1,"\u0070ort":2}}"#,
        r#"{"nested":[{"x":1,"x":2}]}"#,
        r#"{"x":1} {"x":2}"#,
    ] {
        assert!(codec::decode::<serde_json::Value>(json.as_bytes()).is_err());
    }
}

#[test]
fn strict_json_preserves_valid_nested_values() -> crate::Result<()> {
    let value: serde_json::Value =
        codec::decode(br#"{"a":[true,false,null,-2,2,1.25,"text",{"b":"c"}]}"#)?;
    assert_eq!(value["a"][7]["b"], "c");
    Ok(())
}

#[test]
fn bounded_serialization_rejects_oversize_payloads() -> crate::Result<()> {
    let limit = usize::try_from(protocol::MAX_FRAME_BYTES)?;
    assert!(codec::encode(&"x".repeat(limit)).is_err());
    Ok(())
}
