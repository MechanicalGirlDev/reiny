# reiny-ros2

A bridge library between [reiny](https://github.com/MechanicalGirlDev/reiny)
and ROS 2, on pure-Rust DDS ([ros2-client](https://crates.io/crates/ros2-client))
— no ROS installation, no `colcon`. You write a small bridge launch and say,
per type, how a reiny message becomes a ROS message and back:

```rust
use reiny::prelude::*;
use reiny_ros2::ros2_client::{MessageTypeName, ServiceTypeName};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let ros = reiny_ros2::Ros::new(&cloudy, "reiny_bridge")?;      // ROS_DOMAIN_ID from the environment
    ros.export::<RobotState, JointState, _>(                          // reiny → ROS
        "/joint_states", MessageTypeName::new("sensor_msgs", "JointState"), Qos::SENSOR,
        |s| JointState { /* … */ })?;
    ros.import::<Twist, CmdVel, _>(                                   // ROS → reiny
        "/cmd_vel", MessageTypeName::new("geometry_msgs", "Twist"), Qos::COMMAND,
        |t| CmdVel { /* … */ })?;
    ros.export_service::<Calibrate, SetBoolReq, SetBoolRes, _, _>(    // ROS clients → a reiny service
        "/calibrate", &ServiceTypeName::new("std_srvs", "SetBool"), |q| Calibrate { .. }, |r| SetBoolRes { .. })?;
    cloudy.shutdown().await;
    Ok(())
}
```

- ROS message types are `serde` structs implementing `ros2_client::Message`
  (three lines each, or the `ros2-interfaces-<distro>` crates).
- `export_auto` / `import_auto` map same-shaped types by proto field name
  through `Topic::DESCRIPTOR` — zero lines when names and types line up.
- QoS: reliability / durability / history map 1:1 onto DDS; `priority` and
  `express` are dropped. Imported samples carry the bridge's id as source.
- The ROS distribution is a feature (`jazzy` by default, `humble` … `lyrical`),
  passed through to `ros2-client`.

Tested against a ros2-client node in the same process (RustDDS loopback);
interoperability with `rmw_fastrtps` / `rmw_connextdds` is what `ros2-client`
verifies upstream.

License: MIT
