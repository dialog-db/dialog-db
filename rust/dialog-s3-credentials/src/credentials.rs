use super::authorization::Authorization;
use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_common::capability::{Ability, Access, Claim};
use dialog_common::capability::{Authorized, Effect, Provider};
use dialog_common::{Capability, ConditionalSend};

/// Unified credentials enum supporting multiple authorization backends.
#[derive(Debug, Clone)]
pub enum Credentials {
    /// Direct S3 credentials (public or private).
    S3(s3::Credentials),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Access for Credentials {
    type Authorization = Authorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        let result = match self {
            Self::S3(credentials) => Authorization::S3(credentials.claim(claim).await?),
            #[cfg(feature = "ucan")]
            Self::Ucan(credentials) => Authorization::Ucan(credentials.claim(claim).await?),
        };

        Ok(result)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Do> Provider<Authorized<Do, Authorization>> for Credentials
where
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        authorized: Authorized<Do, Authorization>,
    ) -> Result<AuthorizedRequest, AccessError> {
        authorized
            .authorization()
            .grant(authorized.capability())
            .await
    }
}
