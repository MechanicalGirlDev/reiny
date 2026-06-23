//! GUI facet — プラグインの「画面」記述。`gui` フィーチャでのみ有効。
//!
//! [`GuiPanel`] で宣言的にパネル構成を組み立て、[`Plugin::gui`](crate::plugin::Plugin::gui) へ
//! 渡す。パネルは [`PluginLayout`] として低レートで再 announce され、hs-gui がワイルドカードで
//! 拾ってタブとして描画する。値の更新と操作の取得は [`PluginHandle`](crate::plugin::PluginHandle)
//! 側のメソッドで行う(GUI は 1 facet にすぎず、topics/configs と同じインスタンスにぶら下がる)。
//!
//! ```no_run
//! use reiny_component::gui::GuiPanel;
//! use reiny_component::plugin::Plugin;
//! # use reiny_component::Shutdown;
//! # fn run(shutdown: Shutdown) -> anyhow::Result<()> {
//! let panel = GuiPanel::new("System Monitor")
//!     .gauge("cpu", "CPU", "cpu", 0.0, 100.0, "%")
//!     .button("greet", "Greet");
//! let plugin = Plugin::new("system-monitor").gui(panel).serve(shutdown.clone())?;
//! while !shutdown.is_triggered() {
//!     plugin.set_number("cpu", 42.0);
//!     while let Some(cmd) = plugin.try_command() {
//!         tracing::info!("widget {} pressed", cmd.widget_id);
//!     }
//!     std::thread::sleep(std::time::Duration::from_millis(50));
//! }
//! # Ok(()) }
//! ```

use reiny_proto::{
    PluginLayout, PluginValue, PluginWidget, plugin_value, plugin_widget::Kind,
};

/// 受信した操作コマンド。プラグイン作者は proto に直接依存せず、これと
/// [`command_number`] / [`command_flag`] / [`command_text`] で値を読める。
pub use reiny_proto::PluginCommand;

/// 操作コマンドの値を数値として読む(数値以外/未設定は None)。
pub fn command_number(cmd: &PluginCommand) -> Option<f64> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(plugin_value::V::Number(n)) => Some(*n),
        _ => None,
    }
}
/// 操作コマンドの値を真偽として読む(ボタンは true、トグルは ON/OFF)。
pub fn command_flag(cmd: &PluginCommand) -> Option<bool> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(plugin_value::V::Flag(b)) => Some(*b),
        _ => None,
    }
}
/// 操作コマンドの値を文字列として読む(ドロップダウンの選択肢)。
pub fn command_text(cmd: &PluginCommand) -> Option<String> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(plugin_value::V::Text(s)) => Some(s.clone()),
        _ => None,
    }
}
/// 操作コマンドの値を3次元ベクトルとして読む(Controller の速度指令 [vx, vy, ωz])。
pub fn command_vec3(cmd: &PluginCommand) -> Option<[f64; 3]> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(plugin_value::V::Vec3(v)) => Some([v.x, v.y, v.z]),
        _ => None,
    }
}
/// 操作コマンドの値を float 配列として読む(配列以外/未設定は None)。
pub fn command_array(cmd: &PluginCommand) -> Option<Vec<f64>> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(plugin_value::V::Array(a)) => Some(a.values.clone()),
        _ => None,
    }
}

/// 受信した [`PluginValue`](topics 購読など)を数値として読む(数値以外/未設定は None)。
pub fn value_as_number(v: &PluginValue) -> Option<f64> {
    match v.v.as_ref() {
        Some(plugin_value::V::Number(n)) => Some(*n),
        _ => None,
    }
}
/// 受信した [`PluginValue`] を真偽として読む。
pub fn value_as_flag(v: &PluginValue) -> Option<bool> {
    match v.v.as_ref() {
        Some(plugin_value::V::Flag(b)) => Some(*b),
        _ => None,
    }
}
/// 受信した [`PluginValue`] を文字列として読む。
pub fn value_as_text(v: &PluginValue) -> Option<String> {
    match v.v.as_ref() {
        Some(plugin_value::V::Text(s)) => Some(s.clone()),
        _ => None,
    }
}
/// 受信した [`PluginValue`] を3次元ベクトルとして読む。
pub fn value_as_vec3(v: &PluginValue) -> Option<[f64; 3]> {
    match v.v.as_ref() {
        Some(plugin_value::V::Vec3(v)) => Some([v.x, v.y, v.z]),
        _ => None,
    }
}
/// 受信した [`PluginValue`] を float 配列として読む(topics の構造化テレメトリ用)。
pub fn value_as_array(v: &PluginValue) -> Option<Vec<f64>> {
    match v.v.as_ref() {
        Some(plugin_value::V::Array(a)) => Some(a.values.clone()),
        _ => None,
    }
}

/// 宣言的なパネル構成ビルダ。`new` → ウィジェット追加メソッドを連ねて
/// [`Plugin::gui`](crate::plugin::Plugin::gui) へ渡す。`plugin_id` はインスタンス側で
/// 採番後の連番 id(例: `system-monitor-1`)が注入されるため、ここでは設定しない。
#[derive(Debug, Clone)]
pub struct GuiPanel {
    layout: PluginLayout,
}

impl GuiPanel {
    /// `title`(タブ見出し)でパネルを開始する。
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            layout: PluginLayout {
                plugin_id: String::new(), // serve 時に採番済み id が入る
                title: title.into(),
                widgets: Vec::new(),
            },
        }
    }

    fn push(mut self, id: &str, label: &str, bind_key: &str, kind: Kind) -> Self {
        self.layout.widgets.push(PluginWidget {
            id: id.to_string(),
            label: label.to_string(),
            bind_key: bind_key.to_string(),
            kind: Some(kind),
        });
        self
    }

    /// テキスト/数値ラベル(`bind_key` の値を表示、`unit` を後置)。
    pub fn label(self, id: &str, label: &str, bind_key: &str, unit: &str) -> Self {
        let w = reiny_proto::LabelWidget {
            unit: unit.to_string(),
        };
        self.push(id, label, bind_key, Kind::LabelWidget(w))
    }

    /// [min,max] 区間のゲージ。
    pub fn gauge(
        self,
        id: &str,
        label: &str,
        bind_key: &str,
        min: f64,
        max: f64,
        unit: &str,
    ) -> Self {
        let w = reiny_proto::GaugeWidget {
            min,
            max,
            unit: unit.to_string(),
        };
        self.push(id, label, bind_key, Kind::Gauge(w))
    }

    /// 時系列ライングラフ。`history` サンプル保持、`min==max` で自動スケール。
    pub fn graph(
        self,
        id: &str,
        label: &str,
        bind_key: &str,
        history: u32,
        min: f64,
        max: f64,
    ) -> Self {
        let w = reiny_proto::GraphWidget { history, min, max };
        self.push(id, label, bind_key, Kind::Graph(w))
    }

    /// 押下で `flag:true` を送るボタン(表示値なし)。
    pub fn button(self, id: &str, label: &str) -> Self {
        self.push(
            id,
            label,
            "",
            Kind::Button(reiny_proto::ButtonWidget {}),
        )
    }

    /// 連続値スライダー(変化時に `number` を送出)。現在値は `bind_key` で表示。
    pub fn slider(
        self,
        id: &str,
        label: &str,
        bind_key: &str,
        min: f64,
        max: f64,
        step: f64,
    ) -> Self {
        let w = reiny_proto::SliderWidget { min, max, step };
        self.push(id, label, bind_key, Kind::Slider(w))
    }

    /// ON/OFF トグル(変化時に `flag` を送出)。現在状態は `bind_key`(flag)で表示。
    pub fn toggle(self, id: &str, label: &str, bind_key: &str) -> Self {
        self.push(
            id,
            label,
            bind_key,
            Kind::Toggle(reiny_proto::ToggleWidget {}),
        )
    }

    /// 選択肢ドロップダウン(選択時に `text` を送出)。現在選択は `bind_key`(text)で表示。
    pub fn dropdown(self, id: &str, label: &str, bind_key: &str, options: &[&str]) -> Self {
        let w = reiny_proto::DropdownWidget {
            options: options.iter().map(|s| (*s).to_string()).collect(),
        };
        self.push(id, label, bind_key, Kind::Dropdown(w))
    }

    /// 移動速度指令コントローラー(左右ジョイスティック＋WASD/QE＋vx/vy/ωz 上限)。
    /// hs-gui が描画・入力捕捉し、毎フレーム `vec3`([vx, vy, ωz])を送出する。
    /// プラグインは [`command_vec3`] で読み、`hos/control/command_velocity` 等へ流す。
    pub fn controller(self, id: &str, label: &str) -> Self {
        self.push(
            id,
            label,
            "",
            Kind::Controller(reiny_proto::ControllerWidget {}),
        )
    }

    /// 構築済み [`PluginLayout`](内部用)。
    pub(crate) fn into_layout(self) -> PluginLayout {
        self.layout
    }
}

// `PluginValue` は他クレート(proto)の型なので孤児則により `From` は実装できない。
// 代わりに小さなコンストラクタを提供し、型付きセッターから使う。
pub(crate) fn value_number(n: f64) -> PluginValue {
    PluginValue {
        v: Some(plugin_value::V::Number(n)),
    }
}
pub(crate) fn value_flag(b: bool) -> PluginValue {
    PluginValue {
        v: Some(plugin_value::V::Flag(b)),
    }
}
pub(crate) fn value_text(s: String) -> PluginValue {
    PluginValue {
        v: Some(plugin_value::V::Text(s)),
    }
}
pub(crate) fn value_array(values: Vec<f64>) -> PluginValue {
    PluginValue {
        v: Some(plugin_value::V::Array(reiny_proto::FloatArray { values })),
    }
}
