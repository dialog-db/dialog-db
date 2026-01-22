use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_common::capability::{Ability, Access, Authorization, Claim, Effect};
use dialog_common::{Capability, ConditionalSend};

/// Trait describing credentials that can autorize S3Requests
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Authorizer
where
    Self: Sized,
{
    /// Takes S3Request and issues authorization in form of presigned
    /// S3 URL and associated headers.
    async fn authorize<R: S3Request>(&self, request: &R) -> Result<AuthorizedRequest, AccessError>;
}

pub enum Credentials {
    S3(s3::Credentials),
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

#[derive(Debug, Clone)]
pub enum CredentialAuthorization {
    S3(s3::S3Authorization),
    #[cfg(feature = "ucan")]
    Ucan(ucan::UcanAuthorization),
}

impl CredentialAuthorization {
    pub async fn grant<Fx: Effect>(
        &self,
        capability: &Capability<Fx>,
    ) -> Result<AuthorizedRequest, AccessError>
    where
        Capability<Fx>: S3Request,
    {
        match self {
            Self::S3(auth) => auth.grant(capability).await,
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => auth.grant(capability).await,
        }
    }
}

impl Authorization for CredentialAuthorization {
    fn subject(&self) -> &dialog_common::capability::Did {
        match self {
            Self::S3(auth) => auth.subject(),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => auth.subject(),
        }
    }
    fn audience(&self) -> &dialog_common::capability::Did {
        match self {
            Self::S3(auth) => auth.audience(),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => auth.audience(),
        }
    }
    fn can(&self) -> &str {
        match self {
            Self::S3(auth) => auth.can(),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => auth.can(),
        }
    }
    fn invoke<A: dialog_common::Authority>(
        &self,
        authority: &A,
    ) -> Result<Self, dialog_common::capability::AuthorizationError> {
        Ok(match self {
            Self::S3(auth) => Self::S3(auth.invoke(authority)?),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => Self::Ucan(auth.invoke(authority)?),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Access for Credentials {
    type Authorization = CredentialAuthorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        let result = match self {
            Self::S3(credentials) => CredentialAuthorization::S3(credentials.claim(claim).await?),
            #[cfg(feature = "ucan")]
            Self::Ucan(credentials) => {
                CredentialAuthorization::Ucan(credentials.claim(claim).await?)
            }
        };

        Ok(result)
    }
}
