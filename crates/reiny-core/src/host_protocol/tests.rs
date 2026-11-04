use super::*;
use alloc::{boxed::Box, format, vec, vec::Vec};
use serde::{Deserialize, de::value::U16Deserializer};

fn identity() -> SessionIdentity {
    SessionIdentity {
        owner_generation: OwnerGeneration(11),
        graph_generation: GraphGeneration(23),
        host_id: HostId("sensors".into()),
        host_generation: HostGeneration(37),
        graph_digest: PreparedDigest("graph-a".into()),
        bundle_digest: PreparedDigest("bundle-b".into()),
    }
}

#[test]
fn framing_preserves_payload_at_size_boundaries() -> Result<(), Box<dyn core::error::Error>> {
    for size in [1, 255, 256, 65_536, usize::try_from(MAX_FRAME_BYTES)?] {
        let payload = vec![b'x'; size];
        let header = frame_header(&payload)?;
        let frame: Vec<_> = header.into_iter().chain(payload.iter().copied()).collect();

        assert_eq!(payload_length(header), Ok(size));
        assert_eq!(u32::from_be_bytes(header), u32::try_from(size)?);
        assert_eq!(frame_payload(&frame), Ok(payload.as_slice()));
    }
    Ok(())
}

#[test]
fn invalid_frame_lengths_are_rejected_before_body_read() -> Result<(), core::num::TryFromIntError> {
    for length in [0, MAX_FRAME_BYTES + 1, u32::MAX] {
        assert_eq!(
            payload_length(length.to_be_bytes()),
            Err(FrameError::Length(length))
        );
        assert_eq!(
            frame_payload(&length.to_be_bytes()),
            Err(FrameError::Length(length))
        );
    }
    assert_eq!(frame_header(&[]), Err(FrameError::Length(0)));
    let oversized = vec![0; usize::try_from(MAX_FRAME_BYTES)? + 1];
    assert_eq!(
        frame_header(&oversized),
        Err(FrameError::Length(MAX_FRAME_BYTES + 1))
    );
    Ok(())
}

#[test]
fn incomplete_and_concatenated_frames_are_not_one_frame() {
    let frame = [0, 0, 0, 2, b'{', b'}'];
    for length in 0..frame.len() {
        assert_eq!(frame_payload(&frame[..length]), Err(FrameError::Truncated));
    }
    let concatenated = [frame, frame].concat();
    assert_eq!(frame_payload(&concatenated), Err(FrameError::TrailingBytes));
}

#[test]
fn unsupported_versions_cannot_deserialize() -> Result<(), serde::de::value::Error> {
    for version in [0, 2, u16::MAX] {
        let deserializer = U16Deserializer::<serde::de::value::Error>::new(version);
        assert!(ProtocolVersion::deserialize(deserializer).is_err());
        assert_eq!(
            ProtocolVersion::try_from(version),
            Err(EnvelopeError::Version(version))
        );
    }
    let valid = U16Deserializer::<serde::de::value::Error>::new(1);
    assert_eq!(ProtocolVersion::deserialize(valid)?, ProtocolVersion);
    Ok(())
}

#[test]
fn every_session_coordinate_is_checked() {
    let expected = identity();
    for coordinate in 0..6 {
        let mut session = expected.clone();
        match coordinate {
            0 => session.owner_generation = OwnerGeneration(12),
            1 => session.graph_generation = GraphGeneration(24),
            2 => session.host_id = HostId("control".into()),
            3 => session.host_generation = HostGeneration(38),
            4 => session.graph_digest = PreparedDigest("graph-changed".into()),
            _ => session.bundle_digest = PreparedDigest("bundle-changed".into()),
        }
        let message = Envelope {
            version: ProtocolVersion,
            session,
            sequence: Sequence(0),
            body: OwnerMessage::Run {},
        };

        assert_eq!(message.check(&expected, None), Err(EnvelopeError::Identity));
    }
}

#[test]
fn sequences_reject_replay_reordering_and_nonzero_initial_values() {
    for (previous, current, result) in [
        (None, 0, Ok(())),
        (None, 1, Err(EnvelopeError::Sequence)),
        (Some(Sequence(0)), 1, Ok(())),
        (Some(Sequence(1)), 9, Ok(())),
        (Some(Sequence(9)), 9, Err(EnvelopeError::Sequence)),
        (Some(Sequence(9)), 8, Err(EnvelopeError::Sequence)),
        (Some(Sequence(u64::MAX)), 0, Err(EnvelopeError::Sequence)),
    ] {
        let message = Envelope {
            version: ProtocolVersion,
            session: identity(),
            sequence: Sequence(current),
            body: OwnerMessage::Commit {},
        };

        assert_eq!(message.check(&identity(), previous), result);
    }
}

#[test]
fn nonce_debug_does_not_reveal_secret_bytes() {
    let secret = *b"host-bootstrap-secret-0123456789";
    let nonce = Nonce::new(secret);
    let message = Envelope {
        version: ProtocolVersion,
        session: identity(),
        sequence: Sequence(0),
        body: HostMessage::Authenticate(Authenticate {
            nonce,
            compatibility: Compatibility::current(3),
            registrations: Vec::new(),
        }),
    };

    let rendered = format!("{message:?}");
    assert!(!rendered.contains("host-bootstrap-secret"));
    assert!(!rendered.contains("104, 111, 115, 116"));
}
