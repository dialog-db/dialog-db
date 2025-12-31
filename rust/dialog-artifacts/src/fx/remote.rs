//! Remote site types.

use dialog_storage::RestStorageConfig;
use std::hash::Hash;

/// Address for a remote site.
///
/// Contains the information needed to establish a connection to a remote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    /// REST API endpoint.
    Rest(RestStorageConfig),
}

impl Address {
    /// Create a new REST address.
    pub fn rest(config: RestStorageConfig) -> Self {
        Address::Rest(config)
    }

    /// Get a display name for this address (for logging/errors).
    pub fn name(&self) -> String {
        match self {
            Address::Rest(config) => config.endpoint.clone(),
        }
    }
}

impl Hash for Address {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Address::Rest(config) => {
                // Hash the endpoint as the primary identifier
                config.endpoint.hash(state);
            }
        }
    }
}
