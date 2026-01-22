use super::{Authorizer, Credentials};
use super::{build_url, extract_host, is_path_style_default};
use crate::AuthorizedRequest;
use crate::access::{AccessError, S3Request, archive, memory, storage};
use async_trait::async_trait;

use dialog_common::capability::{Authorization, AuthorizationError, Capability, Did, Provider};

/// Self-issued authorization for direct S3 access.
///
/// For S3 credentials that own the bucket, authorization is self-issued.
/// This struct holds the subject, audience, and command for the authorized capability.
#[derive(Debug, Clone)]
pub struct S3Authorization {
    credentials: Credentials,
    subject: Did,
    audience: Did,
    can: String,
}

impl S3Authorization {
    /// Create a new S3 authorization.
    pub fn new(credentials: Credentials, subject: Did, audience: Did, can: String) -> Self {
        Self {
            credentials,
            subject,
            audience,
            can,
        }
    }

    pub async fn grant<R: S3Request>(&self, request: &R) -> Result<AuthorizedRequest, AccessError> {
        let path = request.path();
        let mut url = self.credentials.build_url(&path)?;

        // Add query parameters if specified
        if let Some(params) = request.params() {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(&key, &value);
            }
        }

        let host = extract_host(&url)?;

        let mut headers = vec![("host".to_string(), host)];
        if let Some(checksum) = request.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }

        Ok(AuthorizedRequest {
            url,
            method: request.method().to_string(),
            headers,
        })
    }

    /// Authorize a claim and produce a request descriptor.
    ///
    /// This generates either an unsigned URL (for public credentials) or a
    /// presigned URL with AWS SigV4 signature (for private credentials).
    async fn authorize<R: S3Request>(&self, request: &R) -> Result<AuthorizedRequest, AccessError> {
        self.credentials.authorize(request).await
    }
}

impl Authorization for S3Authorization {
    fn subject(&self) -> &Did {
        &self.subject
    }

    fn audience(&self) -> &Did {
        &self.audience
    }

    fn can(&self) -> &str {
        &self.can
    }

    fn invoke<A: dialog_common::Authority>(
        &self,
        authority: &A,
    ) -> Result<Self, AuthorizationError> {
        if &self.audience != authority.did() {
            Err(AuthorizationError::NotAudience {
                audience: self.audience.clone(),
                issuer: authority.did().into(),
            })
        } else {
            Ok(self.clone())
        }
    }
}

// Provider for access::storage::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Get> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<storage::Get>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::storage::Set
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Set> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<storage::Set>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::storage::Delete
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Delete> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<storage::Delete>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::storage::List
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::List> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<storage::List>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::memory::Resolve
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Resolve> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<memory::Resolve>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::memory::Publish
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Publish> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<memory::Publish>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::memory::Retract
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Retract> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<memory::Retract>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::archive::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Get> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<archive::Get>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}

// Provider for access::archive::Put
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Put> for S3Authorization {
    async fn execute(
        &mut self,
        cap: Capability<archive::Put>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&cap).await
    }
}
