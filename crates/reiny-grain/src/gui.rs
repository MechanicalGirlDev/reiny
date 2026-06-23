//! GUI facet — grain の「画面」記述。`gui` フィーチャでのみ有効。
//!
//! [`GuiPanel`] で宣言的にパネル構成を組み立て、[`Grain::gui`](crate::grain::Grain::gui) へ
//! 渡す。パネルは [`GrainLayout`] として低レートで再 announce され、hs-gui がワイルドカードで
//! 拾ってタブとして描画する。値の更新と操作の取得は [`GrainHandle`](crate::grain::GrainHandle)
//! 側のメソッドで行う(GUI は 1 facet にすぎず、topics/configs と同じインスタンスにぶら下がる)。
//!
//! ```no_run
//! use reiny_grain::gui::GuiPanel;
//! use reiny_grain::grain::Grain;
//! # use reiny_grain::Shutdown;
//! # fn run(shutdown: Shutdown) -> anyhow::Result<()> {
//! let panel = GuiPanel::new("System Monitor")
//!     .gauge("cpu", "CPU", "cpu", 0.0, 100.0, "%")
//!     .button("greet", "Greet");
//! let grain = Grain::new("system-monitor").gui(panel).serve(shutdown.clone())?;
//! while !shutdown.is_triggered() {
//!     grain.set_number("cpu", 42.0);
//!     while let Some(cmd) = grain.try_command() {
//!         tracing::info!("widget {} pressed", cmd.widget_id);
//!     }
//!     std::thread::sleep(std::time::Duration::from_millis(50));
//! }
//! # Ok(()) }
//! ```

use reiny_proto::{GrainLayout, GrainValue, GrainWidget, grain_value, grain_widget::Kind};

/// 受信した操作コマンド。grain 作者は proto に直接依存せず、これと
/// [`command_number`] / [`command_flag`] / [`command_text`] で値を読める。
pub use reiny_proto::GrainCommand;

/// 操作コマンドの値を数値として読む(数値以外/未設定は None)。
pub fn command_number(cmd: &GrainCommand) -> Option<f64> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(grain_value::V::Number(n)) => Some(*n),
        _ => None,
    }
}
/// 操作コマンドの値を真偽として読む(ボタンは true、トグルは ON/OFF)。
pub fn command_flag(cmd: &GrainCommand) -> Option<bool> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(grain_value::V::Flag(b)) => Some(*b),
        _ => None,
    }
}
/// 操作コマンドの値を文字列として読む(ドロップダウンの選択肢)。
pub fn command_text(cmd: &GrainCommand) -> Option<String> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(grain_value::V::Text(s)) => Some(s.clone()),
        _ => None,
    }
}
/// 操作コマンドの値を3次元ベクトルとして読む(Controller の速度指令 [vx, vy, ωz])。
pub fn command_vec3(cmd: &GrainCommand) -> Option<[f64; 3]> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(grain_value::V::Vec3(v)) => Some([v.x, v.y, v.z]),
        _ => None,
    }
}
/// 操作コマンドの値を float 配列として読む(配列以外/未設定は None)。
pub fn command_array(cmd: &GrainCommand) -> Option<Vec<f64>> {
    match cmd.value.as_ref().and_then(|v| v.v.as_ref()) {
        Some(grain_value::V::Array(a)) => Some(a.values.clone()),
        _ => None,
    }
}

/// 受信した [`GrainValue`](topics 購読など)を数値として読む(数値以外/未設定は None)。
pub fn value_as_number(v: &GrainValue) -> Option<f64> {
    match v.v.as_ref() {
        Some(grain_value::V::Number(n)) => Some(*n),
        _ => None,
    }
}
/// 受信した [`GrainValue`] を真偽として読む。
pub fn value_as_flag(v: &GrainValue) -> Option<bool> {
    match v.v.as_ref() {
        Some(grain_value::V::Flag(b)) => Some(*b),
        _ => None,
    }
}
/// 受信した [`GrainValue`] を文字列として読む。
pub fn value_as_text(v: &GrainValue) -> Option<String> {
    match v.v.as_ref() {
        Some(grain_value::V::Text(s)) => Some(s.clone()),
        _ => None,
    }
}
/// 受信した [`GrainValue`] を3次元ベクトルとして読む。
pub fn value_as_vec3(v: &GrainValue) -> Option<[f64; 3]> {
    match v.v.as_ref() {
        Some(grain_value::V::Vec3(v)) => Some([v.x, v.y, v.z]),
        _ => None,
    }
}
/// 受信した [`GrainValue`] を float 配列として読む(topics の構造化テレメトリ用)。
pub fn value_as_array(v: &GrainValue) -> Option<Vec<f64>> {
    match v.v.as_ref() {
        Some(grain_value::V::Array(a)) => Some(a.values.clone()),
        _ => None,
    }
}

/// 宣言的なパネル構成ビルダ。`new` → ウィジェット追加メソッドを連ねて
/// [`Grain::gui`](crate::grain::Grain::gui) へ渡す。`grain_id` はインスタンス側で
/// 採番後の連番 id(例: `system-monitor-1`)が注入されるため、ここでは設定しない。
#[derive(Debug, Clone)]
pub struct GuiPanel {
    layout: GrainLayout,
}

impl GuiPanel {
    /// `title`(タブ見出し)でパネルを開始する。
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            layout: GrainLayout {
                grain_id: String::new(), // serve 時に採番済み id が入る
                title: title.into(),
                widgets: Vec::new(),
            },
        }
    }

    fn push(mut self, id: &str, label: &str, bind_key: &str, kind: Kind) -> Self {
        self.layout.widgets.push(GrainWidget {
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
        self.push(id, label, "", Kind::Button(reiny_proto::ButtonWidget {}))
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
    /// grain は [`command_vec3`] で読み、`hos/control/command_velocity` 等へ流す。
    pub fn controller(self, id: &str, label: &str) -> Self {
        self.push(
            id,
            label,
            "",
            Kind::Controller(reiny_proto::ControllerWidget {}),
        )
    }

    /// 構築済み [`GrainLayout`](内部用)。
    pub(crate) fn into_layout(self) -> GrainLayout {
        self.layout
    }
}

// `GrainValue` は他クレート(proto)の型なので孤児則により `From` は実装できない。
// 代わりに小さなコンストラクタを提供し、型付きセッターから使う。
pub(crate) fn value_number(n: f64) -> GrainValue {
    GrainValue {
        v: Some(grain_value::V::Number(n)),
    }
}
pub(crate) fn value_flag(b: bool) -> GrainValue {
    GrainValue {
        v: Some(grain_value::V::Flag(b)),
    }
}
pub(crate) fn value_text(s: String) -> GrainValue {
    GrainValue {
        v: Some(grain_value::V::Text(s)),
    }
}
pub(crate) fn value_array(values: Vec<f64>) -> GrainValue {
    GrainValue {
        v: Some(grain_value::V::Array(reiny_proto::FloatArray { values })),
    }
}
