//! `QoS` — reiny's vocabulary.
//!
//! Congestion control describes what a send path does when full, not delivery reliability.
//! History, durability, and receive buffering are separate policies. DDS delivery reliability
//! belongs to the ROS adapter's DDS-specific configuration.

/// A publisher's `QoS`. Pass the whole thing to the builder's `.qos(Qos::…)`, or set one field at a
/// time through its fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Qos {
    /// Behavior when the backend send path is congested.
    pub congestion_control: CongestionControl,
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
    /// Backend-default congestion / `Normal` / `KeepAll` / `Volatile`, no express.
    pub const DEFAULT: Qos = Qos {
        congestion_control: CongestionControl::BackendDefault,
        priority: Priority::Normal,
        history: History::KeepAll,
        durability: Durability::Volatile,
        express: false,
    };

    /// Sensor readings: `Drop` / `KeepLast(1)` / `Volatile`.
    /// Discard a send when congested; this does not configure the subscriber's receive buffer.
    pub const SENSOR: Qos = Qos {
        congestion_control: CongestionControl::Drop,
        history: History::KeepLast(1),
        ..Self::DEFAULT
    };

    /// Commands: wait for congestion to clear. This does not guarantee delivery or retransmission.
    pub const COMMAND: Qos = Qos {
        congestion_control: CongestionControl::Block,
        ..Self::DEFAULT
    };

    /// State: backend-default congestion / `KeepLast(1)` / `TransientLocal` — latched. For settings that only need
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

/// Send-path behavior under congestion, never a delivery or retransmission guarantee.
/// Engines reject explicit policies they cannot enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CongestionControl {
    /// Leave the backend's documented default in effect.
    #[default]
    BackendDefault,
    /// Drop when the send path is congested.
    Drop,
    /// `send` waits until the send path clears.
    Block,
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
        assert_eq!(Qos::default(), Qos::DEFAULT);
        assert_eq!(
            Qos::COMMAND,
            Qos {
                congestion_control: CongestionControl::Block,
                ..Qos::DEFAULT
            }
        );

        // DEFAULT delegates congestion without changing the other independent policies.
        assert_eq!(
            Qos::DEFAULT.congestion_control,
            CongestionControl::BackendDefault
        );
        assert_eq!(Qos::DEFAULT.priority, Priority::Normal);
        assert_eq!(Qos::DEFAULT.history, History::KeepAll);
        assert_eq!(Qos::DEFAULT.durability, Durability::Volatile);
        assert!(!Qos::default().express);

        // SENSOR: discard under congestion, nothing is kept for late subscribers.
        assert_eq!(Qos::SENSOR.congestion_control, CongestionControl::Drop);
        assert_eq!(Qos::SENSOR.history, History::KeepLast(1));
        assert_eq!(Qos::SENSOR.durability, Durability::Volatile);
        assert_eq!(Qos::SENSOR.priority, Priority::Normal);
        const { assert!(!Qos::SENSOR.express) };

        // STATE: latched, i.e. KeepLast(1) + TransientLocal, with backend-default congestion.
        assert_eq!(
            Qos::STATE.congestion_control,
            CongestionControl::BackendDefault
        );
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
        assert_eq!(
            CongestionControl::default(),
            Qos::DEFAULT.congestion_control
        );
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
