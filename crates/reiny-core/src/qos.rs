//! `QoS` — reiny's vocabulary.
//!
//! The ROS 2 / DDS vocabulary (reliability / history / durability) was adopted because the ROS 2
//! bridge maps `QoS` **in both directions**. zenoh's own `CongestionControl` is kept out of the
//! public API — what each engine lowers these to is tabulated in `docs/design/0.5.0.md` §2.3.

/// A publisher's `QoS`. Pass the whole thing to the builder's `.qos(Qos::…)`, or set one field at a
/// time through the sugar (`.reliability()` and friends).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Qos {
    /// Under congestion, drop (`BestEffort`) or wait (`Reliable`).
    pub reliability: Reliability,
    /// Send priority.
    pub priority: Priority,
    /// History. The only values meaningful on a publisher are `KeepAll` and `KeepLast(1)`
    /// (`KeepLast(1)` + `TransientLocal` = latched). `KeepLast(n > 1)` is a build-time error — an
    /// n-deep ring is the subscriber's job, via `.latest(n)`.
    pub history: History,
    /// `TransientLocal` = latched (the most recent sample is delivered to late subscribers).
    pub durability: Durability,
    /// Skip batching and send immediately (lower latency, lower throughput).
    pub express: bool,
}

impl Qos {
    /// The default = [`Qos::COMMAND`]: `Reliable` / `Normal` / `KeepAll` / `Volatile`, no express.
    pub const DEFAULT: Qos = Qos {
        reliability: Reliability::Reliable,
        priority: Priority::Normal,
        history: History::KeepAll,
        durability: Durability::Volatile,
        express: false,
    };

    /// Sensor readings: `BestEffort` / `KeepLast(1)` / `Volatile` — the equivalent of ROS 2's
    /// `SensorDataQoS`. Emit the newer value rather than waiting on the older one.
    pub const SENSOR: Qos = Qos {
        reliability: Reliability::BestEffort,
        history: History::KeepLast(1),
        ..Self::DEFAULT
    };

    /// Commands: the default itself. Drop nothing, deliver everything.
    pub const COMMAND: Qos = Self::DEFAULT;

    /// State: `Reliable` / `KeepLast(1)` / `TransientLocal` — latched. For settings that only need
    /// delivering once at startup, or state that is only emitted when it changes.
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

/// Behavior under congestion. Not a statement about retransmission — the only thing every engine
/// honors is "drop" versus "wait".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Reliability {
    /// Drop when the send path is congested.
    BestEffort,
    /// `send` waits until the send path clears.
    #[default]
    Reliable,
}

/// Send priority. Ignored by engines that have no notion of it (link / iceoryx2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Priority {
    /// Ahead of everything else — control-loop commands and the like.
    RealTime,
    /// Interactive operations.
    High,
    /// The default.
    #[default]
    Normal,
    /// Bulk data; may arrive late.
    Low,
    /// Logs and statistics.
    Background,
}

/// How deep the history goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum History {
    /// The most recent n samples.
    KeepLast(usize),
    /// All of them (as far as the buffer allows).
    #[default]
    KeepAll,
}

/// Whether the most recent value is delivered to late subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// It is not. Only samples emitted after the subscription started arrive.
    #[default]
    Volatile,
    /// It is (latched).
    TransientLocal,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three named profiles, field by field. They are the documented contract of this module,
    /// and each is spelled with `..Self::DEFAULT` — so a change to `DEFAULT` silently moves them.
    #[test]
    fn profiles() {
        assert_eq!(Qos::default(), Qos::COMMAND);
        assert_eq!(Qos::COMMAND, Qos::DEFAULT);

        // COMMAND / DEFAULT: drop nothing, deliver everything.
        assert_eq!(Qos::DEFAULT.reliability, Reliability::Reliable);
        assert_eq!(Qos::DEFAULT.priority, Priority::Normal);
        assert_eq!(Qos::DEFAULT.history, History::KeepAll);
        assert_eq!(Qos::DEFAULT.durability, Durability::Volatile);
        assert!(!Qos::default().express);

        // SENSOR: newest value wins, nothing is kept for late subscribers.
        assert_eq!(Qos::SENSOR.reliability, Reliability::BestEffort);
        assert_eq!(Qos::SENSOR.history, History::KeepLast(1));
        assert_eq!(Qos::SENSOR.durability, Durability::Volatile);
        assert_eq!(Qos::SENSOR.priority, Priority::Normal);

        // STATE: latched, i.e. KeepLast(1) + TransientLocal, and still Reliable.
        assert_eq!(Qos::STATE.reliability, Reliability::Reliable);
        assert_eq!(Qos::STATE.history, History::KeepLast(1));
        assert_eq!(Qos::STATE.durability, Durability::TransientLocal);
    }

    /// Latched is exactly `KeepLast(1)` + `TransientLocal`, and only `STATE` is latched — the
    /// distinction every engine keys off when deciding whether to answer late queries.
    #[test]
    fn only_state_is_latched() {
        let latched = |q: Qos| {
            q.history == History::KeepLast(1) && q.durability == Durability::TransientLocal
        };
        assert!(latched(Qos::STATE));
        assert!(!latched(Qos::SENSOR));
        assert!(!latched(Qos::COMMAND));
    }

    /// Every field's `Default` has to agree with `Qos::DEFAULT`; they are written independently
    /// (`#[default]` on the variant vs. the const), so nothing but a test keeps them in step.
    #[test]
    fn field_defaults_match_the_default_profile() {
        assert_eq!(Reliability::default(), Qos::DEFAULT.reliability);
        assert_eq!(Priority::default(), Qos::DEFAULT.priority);
        assert_eq!(History::default(), Qos::DEFAULT.history);
        assert_eq!(Durability::default(), Qos::DEFAULT.durability);
    }

    /// `Priority`'s `Ord` is derived, so declaration order *is* the semantics: the most urgent
    /// variant sorts first. Reordering the enum for readability would silently invert comparisons.
    #[test]
    fn priority_orders_most_urgent_first() {
        assert!(Priority::RealTime < Priority::High);
        assert!(Priority::High < Priority::Normal);
        assert!(Priority::Normal < Priority::Low);
        assert!(Priority::Low < Priority::Background);
    }
}
