use super::Credentials;
use crate::AuthorizedRequest;
use crate::capability::{AccessError, S3Request};

use async_trait::async_trait;

use dialog_capability::{
    Authority, Authorization, AuthorizationError, Capability, Did, Effect, Provider,
};
use dialog_common::{ConditionalSend, ConditionalSync};

/// Self-issued authorization for direct S3 access.
///
/// For S3 credentials that own the bucket, authorization is self-issued.
/// This struct holds the subject, audience, and command for the authorized capability.
#[derive(Debug, Clone)]
pub struct S3Authorization {
    credentials: Credentials,
    subject: Did,
    audience: Did,
    ability: String,
}

impl S3Authorization {
    /// Create a new S3 authorization.
    pub fn new(credentials: Credentials, subject: Did, audience: Did, ability: String) -> Self {
        Self {
            credentials,
            subject,
            audience,
            ability,
        }
    }

    /// Authorize a claim and produce a request descriptor.
    ///
    /// This generates either an unsigned URL (for public credentials) or a
    /// presigned URL with AWS SigV4 signature (for private credentials).
    pub async fn grant<R: S3Request>(&self, request: &R) -> Result<AuthorizedRequest, AccessError> {
        self.credentials.grant(request).await
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl Authorization for S3Authorization {
    fn subject(&self) -> &Did {
        &self.subject
    }

    fn audience(&self) -> &Did {
        &self.audience
    }

    fn ability(&self) -> &str {
        &self.ability
    }

    async fn invoke<A: Authority + ConditionalSend + ConditionalSync>(
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
        capability: Capability<Do>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.grant(&capability).await
    }
}
