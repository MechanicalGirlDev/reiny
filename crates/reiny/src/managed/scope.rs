//! Keep an SDK context from reopening a managed engine as a standalone escape hatch.

use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};

use crate::engine::Engine;
use crate::{Cloudy, Result};

// Weak references prevent address reuse from tainting an unrelated engine. A managed engine
// stays managed for its whole lifetime, even after its original Cloudy has been dropped.
static MANAGED_ENGINES: OnceLock<Mutex<Vec<Weak<dyn Engine>>>> = OnceLock::new();

pub(crate) fn check_engine(engine: &Arc<dyn Engine>, managed: bool) -> Result<()> {
    let mut engines = MANAGED_ENGINES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    engines.retain(|entry| entry.strong_count() > 0);
    let found = engines
        .iter()
        .any(|entry| entry.ptr_eq(&Arc::downgrade(engine)));
    anyhow::ensure!(
        managed || !found,
        "a managed engine cannot open a standalone context"
    );
    if managed && !found {
        engines.push(Arc::downgrade(engine));
    }
    Ok(())
}

impl Cloudy {
    pub(crate) fn ensure_standalone(&self) -> Result<()> {
        anyhow::ensure!(
            self.module.bindings.is_none(),
            "managed modules must use declared named endpoints"
        );
        check_engine(&self.engine, false)
    }
}
