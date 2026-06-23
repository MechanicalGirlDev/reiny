use std::net::SocketAddr;

use reiny_proto::TeleopCommand;
use prost::Message;
use tracing::{info, warn};

use crate::CommError;

/// Message router - decode and dispatch received data
pub struct MessageRouter;

impl MessageRouter {
    /// Try to decode as TeleopCommand
    pub fn decode_teleop_command(data: &[u8]) -> Result<TeleopCommand, CommError> {
        TeleopCommand::decode(data).map_err(CommError::from)
    }

    /// Handle received data (Phase 1: log output only)
    pub fn handle_received(data: &[u8], from: SocketAddr) {
        match Self::decode_teleop_command(data) {
            Ok(cmd) => {
                info!(
                    "TeleopCommand from {}: mode={:?}, timestamp={:?}",
                    from,
                    cmd.input_mode(),
                    cmd.timestamp
                );
                if let Some(left) = &cmd.left_arm
                    && let Some(pose) = &left.target_pose
                    && let Some(pos) = &pose.position
                {
                    info!(
                        "  Left arm target: ({:.3}, {:.3}, {:.3})",
                        pos.x, pos.y, pos.z
                    );
                }
                if let Some(right) = &cmd.right_arm
                    && let Some(pose) = &right.target_pose
                    && let Some(pos) = &pose.position
                {
                    info!(
                        "  Right arm target: ({:.3}, {:.3}, {:.3})",
                        pos.x, pos.y, pos.z
                    );
                }
            }
            Err(e) => {
                warn!("Failed to decode message from {}: {}", from, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reiny_proto::{ArmCommand, Pose, Vector3};
    use prost::Message;

    #[test]
    fn decode_round_trips_a_teleop_command() {
        let original = TeleopCommand {
            left_arm: Some(ArmCommand {
                target_pose: Some(Pose {
                    position: Some(Vector3 {
                        x: 1.0,
                        y: 2.0,
                        z: 3.0,
                    }),
                    orientation: None,
                }),
                gripper_value: 0.5,
            }),
            ..Default::default()
        };

        let bytes = original.encode_to_vec();
        let decoded = MessageRouter::decode_teleop_command(&bytes).expect("should decode");

        let pos = decoded
            .left_arm
            .unwrap()
            .target_pose
            .unwrap()
            .position
            .unwrap();
        assert_eq!((pos.x, pos.y, pos.z), (1.0, 2.0, 3.0));
    }

    #[test]
    fn decode_empty_buffer_is_default_message() {
        // proto3: an empty buffer is a valid, all-default message.
        let decoded = MessageRouter::decode_teleop_command(&[]).expect("empty is valid proto3");
        assert!(decoded.left_arm.is_none());
        assert!(decoded.right_arm.is_none());
    }

    #[test]
    fn decode_invalid_bytes_errors() {
        // A leading byte with an invalid wire type / truncated varint fails to decode.
        let garbage = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(MessageRouter::decode_teleop_command(&garbage).is_err());
    }
}
