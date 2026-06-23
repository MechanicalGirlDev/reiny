//! Zenoh pub/sub communication module
//!
//! Provides typed publishers and subscribers for inter-process communication
//! within the HOS system (e.g., hos-app to hos-gui).

mod liveliness;
mod publisher;
mod session;
mod subscriber;

pub use liveliness::{PresenceEvent, PresenceSubscriber, PresenceToken, scan_alive};
pub use publisher::ZenohPublisher;
pub use session::HosSession;
pub use subscriber::ZenohSubscriber;

/// Zenoh topic keys for HOS
pub mod topics {
    /// Robot state topic (RobotState, 60Hz)
    pub const ROBOT_STATE: &str = "hos/robot/state";

    /// Backend-agnostic whole-body world state (WorldState): base pose + joints + TF.
    /// Published by both the physics-sim and real backends; consumed by hs-gui.
    pub const WORLD_STATE: &str = "hos/robot/world_state";

    /// Robot config bundle topic (RobotConfigBundle, ~1Hz)
    pub const CONFIG: &str = "hos/robot/config";

    /// Base IMU sample (ImuData). Published by the physics-sim backend (and, in
    /// future, the real backend via a HAL ImuDriver); consumed by hs-policy to
    /// build the locomotion observation (base_ang_vel + projected_gravity).
    pub const ROBOT_IMU: &str = "hos/robot/imu";

    /// E-Stop topic (bool)
    pub const ESTOP: &str = "hos/control/estop";

    /// Diagnostic info topic (future)
    pub const DIAGNOSTIC: &str = "hos/diagnostic";

    /// Joint target stream (JointTargets, Manual モード) GUI -> hs-app
    pub const CONTROL_JOINT_TARGET: &str = "hos/control/joint_target";

    /// Control events (ControlCommand: torque/home) GUI -> hs-app
    pub const CONTROL_COMMAND: &str = "hos/control/command";

    /// Control mode switch (ControlModeCommand) GUI -> hs-app
    pub const CONTROL_MODE: &str = "hos/control/mode";

    /// Controller soft-reset (ResetCommand) 外部 -> hs-app
    pub const CONTROL_RESET: &str = "hos/control/reset";

    /// EE デカルト目標 (CartesianTarget) GUI Gizmo/外部 -> hs-app (指令)。
    /// Teleop モードのとき IK を駆動。トラフィック分離のため値変化時のみ publish。
    pub const CONTROL_CARTESIAN_TARGET: &str = "hos/control/cartesian_target";

    /// アクティブな EE 目標の echo (CartesianTarget) hs-app -> hs-gui (表示)。
    /// mode==Teleop && !estop のとき、変化時のみ publish。
    pub const TELEOP_TARGET: &str = "hos/robot/teleop_target";

    /// 移動速度指令ストリーム (VelocityCommand) GUI -> hs-policy
    pub const CONTROL_VELOCITY: &str = "hos/control/command_velocity";

    /// Teleop IK チューニング (IkParams: DLS ソルバの姿勢重み) GUI -> hs-app。
    /// 回転ギズモで姿勢を指令するときに重みを上げ、純並進操作のときは下げる。
    pub const CONTROL_IK_PARAMS: &str = "hos/control/ik_params";

    // ---- sim 専用トピック (hos/sim/*) ----
    // 物理シム(`backend = "sim"`, physics 有効)固有の制御/状態。real backend は
    // 配信しない。GUI は backend が sim のときだけ購読・操作する。

    /// 物理シム制御コマンド (SimControl: physics 有効/無効・リセット) GUI -> hs-app
    pub const SIM_CONTROL: &str = "hos/sim/control";

    /// 物理シム状態 (SimState: physics_enabled・step_count) hs-app -> GUI
    pub const SIM_STATE: &str = "hos/sim/state";

    /// 各関節のサーボ負荷 (ServoLoad: 適用モータトルク/飽和/負荷率) hs-app -> GUI
    pub const SIM_SERVO_LOAD: &str = "hos/sim/servo_load";

    // ---- scene 専用トピック (hos/scene/*) ----
    // シーングラフ(オブジェクト管理)。構造+メタデータは変化時のみ低レートで配信し、
    // 姿勢は従来どおり WorldState.tf(frame_id == object_id)で高速配信する。

    /// シーン構造 + メタデータ (SceneGraph: id/種別/親子/形状) hs-app -> GUI。変化時のみ。
    pub const SCENE_GRAPH: &str = "hos/scene/graph";

    /// シーン編集コマンド (SceneControl: spawn/despawn/set_transform) GUI -> hs-app
    pub const SCENE_CONTROL: &str = "hos/scene/control";

    // ---- プラグインインスタンス名前空間 (hos/<id>/...) ----
    // 任意の component インスタンスが own する名前空間。<id> は基底名 + 起動順の連番
    // (例: system-monitor-1)。1 インスタンスは最大 3 つの facet を持つ:
    //   - gui     : GUI パネル。layout(構成)/data(値)/command(操作) + 生存 token hos/<id>/gui。
    //   - topics  : プラグイン独自の汎用 pub/sub (hos/<id>/topics/<name>)。
    //   - configs : プラグインの TOML 設定 (RobotConfigBundle 同形で外向きに publish)。
    // インスタンスの生存(= id の占有)は liveliness token hos/<id> で表す。起動時の連番採番は
    // この token を hos/* で走査して占有中の id を集め、空き最小の連番を取る。hos/* は hos の
    // 直下 1 セグメントだけに一致するため、hos/<id>/gui 等の facet token とは衝突しない。

    /// インスタンス生存 token 走査用ワイルドカード(連番採番で占有中 id を集める)。
    pub const PLUGIN_INSTANCE_ALL: &str = "hos/*";
    /// GUI facet 生存 token 購読用ワイルドカード(hs-gui が GUI 付きインスタンスを拾う)。
    pub const PLUGIN_GUI_ALL: &str = "hos/*/gui";
    /// GUI layout 購読用ワイルドカード(hs-gui が全 GUI パネルの構成宣言を受ける)。
    pub const PLUGIN_GUI_LAYOUT_ALL: &str = "hos/*/gui/layout";
    /// GUI data 購読用ワイルドカード(hs-gui が全 GUI パネルの値を受ける)。
    pub const PLUGIN_GUI_DATA_ALL: &str = "hos/*/gui/data";

    /// インスタンス生存 token キー(id 占有マーカ。GUI の有無に依らず全インスタンスが宣言)。
    pub fn plugin_instance_liveliness(id: &str) -> String {
        format!("hos/{id}")
    }
    /// GUI facet 生存 token キー(GUI パネルを持つインスタンスが宣言。hs-gui が出現/退場を観測)。
    pub fn plugin_gui_liveliness(id: &str) -> String {
        format!("hos/{id}/gui")
    }
    /// GUI layout キー(構成宣言: プラグイン -> hs-gui)。
    pub fn plugin_gui_layout(id: &str) -> String {
        format!("hos/{id}/gui/layout")
    }
    /// GUI data キー(値: プラグイン -> hs-gui)。
    pub fn plugin_gui_data(id: &str) -> String {
        format!("hos/{id}/gui/data")
    }
    /// GUI command キー(操作: hs-gui -> プラグイン)。
    pub fn plugin_gui_command(id: &str) -> String {
        format!("hos/{id}/gui/command")
    }
    /// configs facet キー(プラグインの生 TOML 設定: プラグイン -> 外部。読取専用)。
    pub fn plugin_config(id: &str) -> String {
        format!("hos/{id}/configs")
    }
    /// topics facet 個別キー(プラグイン独自データ: プラグイン -> 任意の購読者)。
    pub fn plugin_topic(id: &str, name: &str) -> String {
        format!("hos/{id}/topics/{name}")
    }
    /// topics facet 購読用ワイルドカード(同名トピックを全インスタンス横断で拾う)。
    /// 連番 (-N) に縛られず「name で」購読できるので、プラグイン間連携で便利。
    pub fn plugin_topic_any(name: &str) -> String {
        format!("hos/*/topics/{name}")
    }
}

#[cfg(test)]
mod topic_tests {
    use super::topics;

    #[test]
    fn control_topics_are_namespaced() {
        assert_eq!(topics::CONTROL_JOINT_TARGET, "hos/control/joint_target");
        assert_eq!(topics::CONTROL_COMMAND, "hos/control/command");
        assert_eq!(topics::CONTROL_MODE, "hos/control/mode");
        assert_eq!(topics::CONTROL_RESET, "hos/control/reset");
        assert_eq!(topics::ESTOP, "hos/control/estop");
        assert_eq!(
            topics::CONTROL_CARTESIAN_TARGET,
            "hos/control/cartesian_target"
        );
        assert_eq!(topics::CONTROL_VELOCITY, "hos/control/command_velocity");
        assert_eq!(topics::CONTROL_IK_PARAMS, "hos/control/ik_params");
    }

    #[test]
    fn target_topics_are_separated() {
        // 指令(control)と echo(robot) は別キー: hs-app の自己受信ループを防ぐ。
        assert_eq!(
            topics::CONTROL_CARTESIAN_TARGET,
            "hos/control/cartesian_target"
        );
        assert_eq!(topics::TELEOP_TARGET, "hos/robot/teleop_target");
        assert_ne!(topics::CONTROL_CARTESIAN_TARGET, topics::TELEOP_TARGET);
    }

    #[test]
    fn sim_topics_are_namespaced() {
        // sim 専用トピックは hos/sim/<name> 名前空間に属する。
        assert_eq!(topics::SIM_CONTROL, "hos/sim/control");
        assert_eq!(topics::SIM_STATE, "hos/sim/state");
        assert_eq!(topics::SIM_SERVO_LOAD, "hos/sim/servo_load");
    }

    #[test]
    fn scene_topics_are_namespaced() {
        // scene 専用トピックは hos/scene/<name> 名前空間に属する。
        assert_eq!(topics::SCENE_GRAPH, "hos/scene/graph");
        assert_eq!(topics::SCENE_CONTROL, "hos/scene/control");
    }

    #[test]
    fn plugin_namespace_is_id_first() {
        // facet は id の後ろ(hos/<id>/<facet>)。連番 id をそのまま流し込める。
        let id = "system-monitor-1";
        assert_eq!(
            topics::plugin_instance_liveliness(id),
            "hos/system-monitor-1"
        );
        assert_eq!(
            topics::plugin_gui_liveliness(id),
            "hos/system-monitor-1/gui"
        );
        assert_eq!(
            topics::plugin_gui_layout(id),
            "hos/system-monitor-1/gui/layout"
        );
        assert_eq!(topics::plugin_gui_data(id), "hos/system-monitor-1/gui/data");
        assert_eq!(
            topics::plugin_gui_command(id),
            "hos/system-monitor-1/gui/command"
        );
        assert_eq!(topics::plugin_config(id), "hos/system-monitor-1/configs");
        assert_eq!(
            topics::plugin_topic(id, "wave"),
            "hos/system-monitor-1/topics/wave"
        );
        assert_eq!(topics::plugin_topic_any("wave"), "hos/*/topics/wave");
    }

    #[test]
    fn instance_wildcard_does_not_match_gui_facet() {
        // hos/* はインスタンス token(1 セグメント)だけ。facet token(hos/<id>/gui)とは
        // セグメント数が違うので採番走査が GUI token を拾わない、という前提を固定する。
        assert_eq!(topics::PLUGIN_INSTANCE_ALL, "hos/*");
        assert_eq!(topics::PLUGIN_GUI_ALL, "hos/*/gui");
    }
}
