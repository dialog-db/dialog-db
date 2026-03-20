//! UCAN site configuration — pure config, no credentials.

use crate::capability::AuthorizedRequest;
use dialog_capability::site::{RemoteSite, Site};

use super::UcanInvocation;

/// UCAN site configuration for delegated authorization.
///
/// Contains the access service endpoint. Credentials (delegation chain)
/// are managed by the environment's credential store.
#[derive(Debug, Clone)]
pub struct UcanSite {
    /// The access service URL to POST invocations to.
    pub(crate) endpoint: String,
}

impl UcanSite {
    /// Create a new UCAN site with the given endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    /// Get the access service endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Site for UcanSite {
    type Permit = UcanInvocation;
    type Access = AuthorizedRequest;
}

impl RemoteSite for UcanSite {}
