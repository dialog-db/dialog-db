//! Remote state types.
//!
//! This module provides the [`RemoteState`] struct which stores
//! the configuration and credentials for a remote repository connection.

use serde::{Deserialize, Serialize};

use super::{RemoteCredentials, Site};

/// State information for a remote repository connection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteState {
    /// Name for this remote.
    pub site: Site,

    /// Credentials used to connect to this remote.
    pub credentials: RemoteCredentials,
}
