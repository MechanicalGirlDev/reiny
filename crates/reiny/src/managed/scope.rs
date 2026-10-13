//! Internal transport-test helpers are unavailable to application code.

use crate::{Cloudy, Result};

impl Cloudy {
    pub(crate) fn ensure_standalone(&self) -> Result<()> {
        anyhow::ensure!(
            self.module.bindings.is_none(),
            "modules must use declared named endpoints"
        );
        Ok(())
    }
}
