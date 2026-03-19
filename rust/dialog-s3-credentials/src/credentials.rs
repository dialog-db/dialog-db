use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_capability::{Access, Authorized, Capability, Constraint, credential};
use dialog_common::{ConditionalSend, ConditionalSync};

/// Unified credentials enum supporting multiple authorization backends.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum Credentials {
    /// Direct S3 credentials (public or private).
    S3(s3::Credentials),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

impl std::hash::Hash for Credentials {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::S3(c) => c.hash(state),
            #[cfg(feature = "ucan")]
            Self::Ucan(c) => c.hash(state),
        }
    }
}

impl From<s3::Credentials> for Credentials {
    fn from(credentials: s3::Credentials) -> Self {
        Self::S3(credentials)
    }
}

#[cfg(feature = "ucan")]
impl From<ucan::Credentials> for Credentials {
    fn from(credentials: ucan::Credentials) -> Self {
        Self::Ucan(credentials)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Access<C> for Credentials
where
    C: Constraint + Clone + ConditionalSend + 'static,
    Capability<C>: ConditionalSend + S3Request,
{
    type Authorization = AuthorizedRequest;
    type Error = AccessError;

    async fn authorize<Env>(
        &self,
        capability: Capability<C>,
        env: &Env,
    ) -> Result<Authorized<C, AuthorizedRequest>, Self::Error>
    where
        Env: dialog_capability::Provider<credential::Identify>
            + dialog_capability::Provider<credential::Sign>
            + ConditionalSync,
    {
        match self {
            Self::S3(credentials) => credentials.authorize(capability, env).await.map_err(Into::into),
            #[cfg(feature = "ucan")]
            Self::Ucan(credentials) => {
                credentials.authorize(capability, env).await.map_err(Into::into)
            }
        }
    }
}
