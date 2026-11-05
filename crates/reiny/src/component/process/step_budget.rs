use super::{ProcessError, protocol::step::MAX_RETAINED_BYTES, step_wire::invalid};

/// Shared preparation budget, including snapshots not selected by graph routes.
#[derive(Default)]
pub(super) struct SnapshotBudget {
    bytes: usize,
}

impl SnapshotBudget {
    pub(super) fn charge(&mut self, bytes: usize) -> Result<(), ProcessError> {
        let retained = self
            .bytes
            .checked_add(bytes)
            .filter(|total| *total <= MAX_RETAINED_BYTES)
            .ok_or_else(invalid)?;
        self.bytes = retained;
        Ok(())
    }
}
