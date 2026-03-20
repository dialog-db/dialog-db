//! S3 site configuration — pure config, no credentials.

use crate::Address;
use crate::capability::AuthorizedRequest;
use dialog_capability::site::{RemoteSite, Site};
use url::Url;

use super::provider::S3Permit;

/// S3 site configuration for direct S3 access.
///
/// Contains the endpoint and addressing info needed to build S3 URLs.
/// No credential material — that lives in the environment's credential store.
#[derive(Debug, Clone)]
pub struct S3Site {
    /// S3 address (endpoint, region, bucket).
    pub(crate) address: Address,
    /// Parsed endpoint URL.
    pub(crate) endpoint: Url,
    /// Whether to use path-style URLs.
    pub(crate) path_style: bool,
}

impl S3Site {
    /// Create a new S3 site from an address.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    pub fn new(address: Address) -> Result<Self, crate::AccessError> {
        let endpoint = Url::parse(address.endpoint())
            .map_err(|e| crate::AccessError::Configuration(e.to_string()))?;
        let path_style = super::is_path_style_default(&endpoint);

        Ok(Self {
            address,
            endpoint,
            path_style,
        })
    }

    /// Enable path-style URL addressing.
    pub fn with_path_style(mut self) -> Self {
        self.path_style = true;
        self
    }

    /// Get the address.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Get the region.
    pub fn region(&self) -> &str {
        self.address.region()
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        self.address.bucket()
    }

    /// Whether path-style URLs are used.
    pub fn path_style(&self) -> bool {
        self.path_style
    }
}

impl Site for S3Site {
    type Permit = S3Permit;
    type Access = AuthorizedRequest;
}

impl RemoteSite for S3Site {}
