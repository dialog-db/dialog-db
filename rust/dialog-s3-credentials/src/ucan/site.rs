//! UCAN site configuration — marker trait + address type.

use dialog_capability::Constraint;
use dialog_capability::credential::{self, AuthorizationFormat};
use dialog_capability::site::Site;

use super::UcanInvocation;

/// UCAN authorization format — produces a signed invocation chain.
pub struct UcanFormat;

impl AuthorizationFormat for UcanFormat {
    type Authorization<Fx: Constraint> = UcanInvocation;
}

/// UCAN credentials — serialized invocation material.
///
/// This is the credential type resolved from the credential store
/// for UCAN-based sites. Contains serialized UCAN container bytes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UcanCredentials {
    /// Serialized UCAN container bytes (invocation + delegation chain).
    #[serde(with = "serde_bytes")]
    pub container: Vec<u8>,
    /// The access service endpoint URL.
    pub endpoint: String,
}

/// UCAN site address — wraps the access service endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UcanAddress {
    /// The access service endpoint URL.
    pub endpoint: String,
}

impl UcanAddress {
    /// Create a new UCAN address with the given endpoint.
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

impl credential::Addressable<UcanCredentials> for UcanAddress {
    fn credential_address(&self) -> credential::Address<UcanCredentials> {
        credential::Address::new(&self.endpoint)
    }
}

/// UCAN site configuration for delegated authorization.
///
/// A marker type — no fields. Address info lives in `UcanAddress`.
#[derive(Debug, Clone, Copy)]
pub struct UcanSite;

impl Site for UcanSite {
    type Credentials = UcanCredentials;
    type Format = UcanFormat;
    type Address = UcanAddress;
}

