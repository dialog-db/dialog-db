use super::authorization::Authorization;
use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_common::Authority;
use dialog_common::capability::{
    Ability, Access, Authorized, Claim, Did, Effect, Principal, Provider,
};
use dialog_common::{Capability, ConditionalSend};

/// Unified credentials enum supporting multiple authorization backends.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
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

/// Credentials enum acts as its own principal.
///
/// For S3 credentials, returns a placeholder DID since S3 doesn't have
/// cryptographic identity. For UCAN credentials, returns the operator's DID
/// (the audience of the delegation chain).
impl Principal for Credentials {
    fn did(&self) -> &Did {
        // Use a static string for S3 - it's a placeholder since S3 auth
        // doesn't depend on the audience DID.
        static S3_DID: String = String::new();
        match self {
            Self::S3(_) => &S3_DID,
            #[cfg(feature = "ucan")]
            Self::Ucan(credentials) => credentials.audience_did(),
        }
    }
}

/// Credentials enum implements Authority for the perform() flow.
///
/// For S3 credentials, sign() is a no-op since S3 uses its own signing.
/// For UCAN credentials, this would need actual signing capability.
impl Authority for Credentials {
    fn sign(&mut self, _payload: &[u8]) -> Vec<u8> {
        match self {
            // S3 doesn't need external signing - it uses AWS SigV4 internally
            Self::S3(_) => Vec::new(),
            #[cfg(feature = "ucan")]
            Self::Ucan(_) => {
                // UCAN signing requires the operator's secret key, which is
                // not stored in Credentials. This path should not be reached
                // in normal usage - UCAN should use a Session with Authority.
                panic!("UCAN credentials require a Session with Authority for signing")
            }
        }
    }
}
