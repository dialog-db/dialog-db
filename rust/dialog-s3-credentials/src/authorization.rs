use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use dialog_common::Capability;
use dialog_common::{capability, capability::Effect};

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

impl capability::Authorization for Authorization {
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
