//! REST-backed site implementation.
//!
//! This module provides a site backed by REST storage, typically used
//! for remote synchronization.

use super::{Acquirable, Remote, TheSite};
use crate::platform::ErrorMappingBackend;
use dialog_storage::{RestStorageBackend, RestStorageConfig};
use std::hash::{Hash, Hasher};

/// Address for REST-backed storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestAddress {
    /// The REST configuration.
    pub config: RestStorageConfig,
}

impl RestAddress {
    /// Create a new REST address.
    pub fn new(config: RestStorageConfig) -> Self {
        Self { config }
    }
}

impl Hash for RestAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the unique parts of the config
        self.config.endpoint.hash(state);
        self.config.bucket.hash(state);
        self.config.key_prefix.hash(state);
    }
}

/// The backend type used for REST storage.
pub type RestBackend = ErrorMappingBackend<RestStorageBackend<Vec<u8>, Vec<u8>>>;

/// REST-backed site.
pub type RestSite = TheSite<RestBackend>;

/// Error type for REST site operations.
#[derive(Debug)]
pub enum RestError {
    /// Failed to create the REST backend.
    ConnectionFailed(String),
}

impl std::fmt::Display for RestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestError::ConnectionFailed(msg) => write!(f, "REST connection failed: {}", msg),
        }
    }
}

impl std::error::Error for RestError {}

impl Acquirable<Remote> for RestSite {
    type Error = RestError;

    fn acquire(address: &Remote) -> Result<Self, Self::Error> {
        match address {
            Remote::Rest(addr) => {
                let backend = RestStorageBackend::new(addr.config.clone())
                    .map_err(|e| RestError::ConnectionFailed(format!("{:?}", e)))?;

                Ok(TheSite::shared(ErrorMappingBackend::new(backend)))
            }
        }
    }
}
