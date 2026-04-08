//! UCAN site configuration -- marker trait + address type.

use dialog_capability::site::{Site, SiteAddress};

// Re-export UCAN types for convenience.
pub use dialog_ucan::{Ucan, UcanInvocation};

/// UCAN site address -- wraps the access service endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

    /// POST a signed UCAN invocation to the access service and get back
    /// a presigned URL for the S3 operation.
    pub async fn authorize(
        &self,
        invocation: &UcanInvocation,
    ) -> Result<dialog_remote_s3::Permit, dialog_remote_s3::AccessError> {
        let body = invocation
            .to_bytes()
            .map_err(|e| dialog_remote_s3::AccessError::Invocation(e.to_string()))?;

        let response = reqwest::Client::new()
            .post(&self.endpoint)
            .header("Content-Type", "application/cbor")
            .body(body)
            .send()
            .await
            .map_err(|e| dialog_remote_s3::AccessError::Service(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(dialog_remote_s3::AccessError::Service(format!(
                "Access service returned {}: {}",
                status, body
            )));
        }

        let body = response
            .bytes()
            .await
            .map_err(|e| dialog_remote_s3::AccessError::Service(e.to_string()))?;

        serde_ipld_dagcbor::from_slice(&body).map_err(|e| {
            dialog_remote_s3::AccessError::Service(format!("Failed to decode response: {}", e))
        })
    }
}

impl SiteAddress for UcanAddress {
    type Site = UcanSite;
}

/// UCAN site configuration for delegated authorization.
///
/// A marker type -- no fields. Address info lives in `UcanAddress`.
#[derive(Debug, Clone, Copy, Default)]
pub struct UcanSite;

impl Site for UcanSite {
    type Protocol = Ucan;
    type Address = UcanAddress;
}
