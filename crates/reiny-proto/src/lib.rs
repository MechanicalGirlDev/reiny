//! HOS Protocol Buffer definitions
//!
//! Generated from proto/*.proto files

pub mod hos {
    include!(concat!(env!("OUT_DIR"), "/hos.rs"));
}

pub use hos::*;

#[cfg(test)]
mod tests {
    use crate::hos::{ControlCommand, ControlMode, JointTargets, SetTorque, control_command};

    #[test]
    fn control_types_are_generated() {
        // enum 既定は IDLE(=0)。
        assert_eq!(ControlMode::default(), ControlMode::Idle);

        let jt = JointTargets {
            timestamp: None,
            targets: vec![0.1, 0.2, 0.3],
        };
        assert_eq!(jt.targets.len(), 3);

        // oneof は control_command::Payload に生成される。
        let cmd = ControlCommand {
            timestamp: None,
            payload: Some(control_command::Payload::SetTorque(SetTorque {
                joint_indices: vec![],
                enabled: false,
            })),
        };
        assert!(cmd.payload.is_some());
    }

    #[test]
    fn reset_command_is_generated() {
        let r = crate::hos::ResetCommand { timestamp: None };
        assert!(r.timestamp.is_none());
    }

    #[test]
    fn velocity_command_is_generated() {
        let v = crate::hos::VelocityCommand {
            timestamp: None,
            vx: 0.5,
            vy: -0.2,
            omega_z: 0.1,
        };
        assert_eq!(v.vx, 0.5);
        assert_eq!(v.vy, -0.2);
        assert_eq!(v.omega_z, 0.1);
    }

    #[test]
    fn world_state_types_exist() {
        let _ = crate::WorldState::default();
        let _ = crate::Transform::default();
    }

    #[test]
    fn cartesian_target_roundtrip() {
        use crate::hos::{CartesianTarget, ChainTarget, Pose, Quaternion, TargetSource, Vector3};
        use prost::Message;

        // 部分指令: left_arm のみ(右は targets に含めない = 無指令)。
        let t = CartesianTarget {
            timestamp: None,
            targets: vec![ChainTarget {
                chain: "left_arm".to_string(),
                pose: Some(Pose {
                    position: Some(Vector3 {
                        x: 0.1,
                        y: 0.2,
                        z: 0.3,
                    }),
                    orientation: Some(Quaternion {
                        x: 0.0,
                        y: 0.0,
                        z: 0.0,
                        w: 1.0,
                    }),
                }),
                gripper: 0.5,
            }],
            source: TargetSource::Gizmo as i32,
        };
        let buf = t.encode_to_vec();
        let back = CartesianTarget::decode(&buf[..]).expect("decode CartesianTarget");

        assert_eq!(back.targets.len(), 1);
        assert_eq!(back.targets[0].chain, "left_arm");
        assert_eq!(back.source, TargetSource::Gizmo as i32);
        assert_eq!(back.targets[0].gripper, 0.5);
        let pos = back.targets[0].pose.as_ref().unwrap().position.unwrap();
        assert_eq!(pos.x, 0.1);
    }

    #[test]
    fn sim_control_types_are_generated() {
        use crate::hos::{SimControl, SimReset, sim_control};
        // physics 有効/無効は oneof scalar、リセットは oneof message。
        let enable = SimControl {
            timestamp: None,
            payload: Some(sim_control::Payload::SetPhysicsEnabled(true)),
        };
        assert!(matches!(
            enable.payload,
            Some(sim_control::Payload::SetPhysicsEnabled(true))
        ));
        let reset = SimControl {
            timestamp: None,
            payload: Some(sim_control::Payload::Reset(SimReset {})),
        };
        assert!(matches!(
            reset.payload,
            Some(sim_control::Payload::Reset(_))
        ));
    }

    #[test]
    fn sim_state_and_servo_load_are_generated() {
        use crate::hos::{ServoLoad, ServoLoadEntry, SimState};
        let s = SimState {
            timestamp: None,
            physics_enabled: true,
            step_count: 42,
            base_pinned: true,
        };
        assert!(s.physics_enabled);
        assert_eq!(s.step_count, 42);
        assert!(s.base_pinned);

        let load = ServoLoad {
            timestamp: None,
            entries: vec![ServoLoadEntry {
                name: "j1".into(),
                torque: 1.5,
                tau_max: 30.0,
                load_percent: 5.0,
            }],
        };
        assert_eq!(load.entries.len(), 1);
        assert_eq!(load.entries[0].name, "j1");
        assert_eq!(load.entries[0].tau_max, 30.0);
    }

    #[test]
    fn robot_state_has_wholebody_fields() {
        use crate::hos::{ControlMode, JointStatus, RobotState};
        let s = RobotState {
            joints: vec![JointStatus {
                name: "j1".into(),
                angle: 0.5,
                torque_enabled: true,
                velocity: 0.0,
            }],
            control_mode: ControlMode::Manual as i32,
            ..Default::default()
        };
        assert_eq!(s.joints.len(), 1);
        assert_eq!(s.control_mode, ControlMode::Manual as i32);
    }

    #[test]
    fn scene_graph_and_control_types_are_generated() {
        use crate::hos::{
            BoxShape, Pose, SceneControl, SceneGraph, SceneObject, SceneObjectClass,
            ShapeDescriptor, SpawnProp, scene_control, shape_descriptor,
        };

        // SceneGraph: revision + objects(種別/親子/形状記述子)。
        let graph = SceneGraph {
            timestamp: None,
            revision: 3,
            objects: vec![SceneObject {
                id: "ground".into(),
                name: "Ground".into(),
                object_class: SceneObjectClass::Ground as i32,
                parent_id: "world".into(),
                visual: Some(ShapeDescriptor {
                    shape: Some(shape_descriptor::Shape::Box(BoxShape {
                        hx: 50.0,
                        hy: 50.0,
                        hz: 0.1,
                    })),
                    r: 0.5,
                    g: 0.5,
                    b: 0.5,
                    a: 1.0,
                }),
                dynamic: false,
            }],
        };
        assert_eq!(graph.revision, 3);
        assert_eq!(
            graph.objects[0].object_class,
            SceneObjectClass::Ground as i32
        );

        // SceneControl: spawn は oneof message、despawn は oneof scalar。
        let spawn = SceneControl {
            timestamp: None,
            command: Some(scene_control::Command::Spawn(SpawnProp {
                name: "box".into(),
                visual: None,
                pose: Some(Pose::default()),
                dynamic: true,
                mass: 0.0,
            })),
        };
        assert!(matches!(
            spawn.command,
            Some(scene_control::Command::Spawn(_))
        ));
        let despawn = SceneControl {
            timestamp: None,
            command: Some(scene_control::Command::DespawnId("prop_0".into())),
        };
        assert!(matches!(
            despawn.command,
            Some(scene_control::Command::DespawnId(_))
        ));
    }
}
