use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_capability::{
    Authority, Authorization as Auth, Capability, DialogCapabilityAuthorizationError, Did, Effect,
};
use dialog_common::{ConditionalSend, ConditionalSync};

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Authorization {
    S3(s3::S3Authorization),
    #[cfg(feature = "ucan")]
    Ucan(ucan::UcanAuthorization),
}

impl Authorization {
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

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl Auth for Authorization {
    fn subject(&self) -> &Did {
        match self {
            Self::S3(auth) => auth.subject(),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => auth.subject(),
        }
    }
    fn audience(&self) -> &Did {
        match self {
            Self::S3(auth) => auth.audience(),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => auth.audience(),
        }
    }
    fn ability(&self) -> &str {
        match self {
            Self::S3(auth) => auth.ability(),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => auth.ability(),
        }
    }
    async fn invoke<A: Authority + ConditionalSend + ConditionalSync>(
        &self,
        authority: &A,
    ) -> Result<Self, DialogCapabilityAuthorizationError> {
        Ok(match self {
            Self::S3(auth) => Self::S3(auth.invoke(authority).await?),
            #[cfg(feature = "ucan")]
            Self::Ucan(auth) => Self::Ucan(auth.invoke(authority).await?),
        })
    }
}
