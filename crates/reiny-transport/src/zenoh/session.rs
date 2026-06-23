//! Zenoh session wrapper for HOS

use std::sync::Arc;

use tracing::info;
use zenoh::Session;

use crate::CommError;

/// HOS Zenoh session wrapper
///
/// Provides a shared Zenoh session for creating publishers and subscribers.
#[derive(Clone)]
pub struct HosSession {
    session: Arc<Session>,
}

impl HosSession {
    /// Open a new Zenoh session with default configuration
    pub async fn open() -> Result<Self, CommError> {
        info!("Opening Zenoh session...");
        let session = zenoh::open(zenoh::Config::default()).await?;
        info!("Zenoh session opened");
        Ok(Self {
            session: Arc::new(session),
        })
    }

    /// Open a new Zenoh session with custom configuration
    pub async fn open_with_config(config: zenoh::Config) -> Result<Self, CommError> {
        info!("Opening Zenoh session with custom config...");
        let session = zenoh::open(config).await?;
        info!("Zenoh session opened");
        Ok(Self {
            session: Arc::new(session),
        })
    }

    /// Get a reference to the underlying session
    pub fn inner(&self) -> &Arc<Session> {
        &self.session
    }
}
