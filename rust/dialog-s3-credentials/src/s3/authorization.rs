use super::Credentials;
use super::extract_host;
use crate::AuthorizedRequest;
use crate::capability::{AccessError, S3Request};

use async_trait::async_trait;

use dialog_common::{
    ConditionalSend,
    capability::{Authorization, AuthorizationError, Capability, Did, Effect, Provider},
};

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
        self.credentials.grant(request).await
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Do> Provider<Do> for S3Authorization
where
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        capabality: Capability<Do>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.authorize(&capabality).await
    }
}
