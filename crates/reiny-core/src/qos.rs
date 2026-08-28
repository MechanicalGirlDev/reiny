//! `QoS` —— reiny の語彙。
//!
//! ROS 2 / DDS の語彙(reliability / history / durability)を採るのは、ROS 2 bridge が `QoS` を
//! **双方向に写す**ため。zenoh 固有の `CongestionControl` は公開 API に出さない —— エンジン
//! ごとに何へ落ちるかは `docs/design/0.5.0.md` §2.3 の表。

/// publisher の QoS。builder の `.qos(Qos::…)` にまとめて渡すか、糖衣(`.reliability()` 等)で
/// 1 項目ずつ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Qos {
    /// 輻輳時に捨てるか(`BestEffort`)、待つか(`Reliable`)。
    pub reliability: Reliability,
    /// 送信優先度。
    pub priority: Priority,
    /// 履歴。publisher で意味を持つのは `KeepAll` と `KeepLast(1)` だけ
    /// (`KeepLast(1)` + `TransientLocal` = latched)。`KeepLast(n > 1)` は build 時にエラー ——
    /// n 件のリングは購読側の `.latest(n)` の仕事。
    pub history: History,
    /// `TransientLocal` = latched(直近 1 件を、遅れて来た購読者に配る)。
    pub durability: Durability,
    /// バッチングを飛ばして即時送信する(低レイテンシ・低スループット)。
    pub express: bool,
}

impl Qos {
    /// 既定 = [`Qos::COMMAND`]: `Reliable` / `Normal` / `KeepAll` / `Volatile` / express なし。
    pub const DEFAULT: Qos = Qos {
        reliability: Reliability::Reliable,
        priority: Priority::Normal,
        history: History::KeepAll,
        durability: Durability::Volatile,
        express: false,
    };

    /// センサ値: `BestEffort` / `KeepLast(1)` / `Volatile` —— ROS 2 の `SensorDataQoS` 相当。
    /// 古い値を待つより新しい値を出す。
    pub const SENSOR: Qos = Qos {
        reliability: Reliability::BestEffort,
        history: History::KeepLast(1),
        ..Self::DEFAULT
    };

    /// 指令: 既定そのもの。落とさない、全部届ける。
    pub const COMMAND: Qos = Self::DEFAULT;

    /// 状態: `Reliable` / `KeepLast(1)` / `TransientLocal` —— latched。起動時に 1 回配れば
    /// 済む設定値や、変化したときだけ出す状態量。
    pub const STATE: Qos = Qos {
        history: History::KeepLast(1),
        durability: Durability::TransientLocal,
        ..Self::DEFAULT
    };
}

impl Default for Qos {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// 輻輳時の振る舞い。「再送」の意味ではない —— どのエンジンでも効くのは「捨てる / 待つ」だけ。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Reliability {
    /// 送信路が詰まっていたら捨てる。
    BestEffort,
    /// 送信路が空くまで `send` が待つ。
    #[default]
    Reliable,
}

/// 送信優先度。エンジンが持たなければ無視される(link / iceoryx2)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Priority {
    /// 制御ループの指令など、他の全てより先に。
    RealTime,
    /// 対話的な操作。
    High,
    /// 既定。
    #[default]
    Normal,
    /// 大きなデータ、遅れてよいもの。
    Low,
    /// ログ、統計。
    Background,
}

/// 履歴の深さ。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum History {
    /// 直近 n 件。
    KeepLast(usize),
    /// 全部(バッファの許す限り)。
    #[default]
    KeepAll,
}

/// 遅れて来た購読者に直近値を配るか。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// 配らない。購読を始めた後に出たものだけ届く。
    #[default]
    Volatile,
    /// 配る(latched)。
    TransientLocal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles() {
        assert_eq!(Qos::default(), Qos::COMMAND);
        assert_eq!(Qos::STATE.durability, Durability::TransientLocal);
        assert_eq!(Qos::STATE.history, History::KeepLast(1));
        assert_eq!(Qos::SENSOR.reliability, Reliability::BestEffort);
        assert_eq!(Qos::SENSOR.durability, Durability::Volatile);
    }
}
