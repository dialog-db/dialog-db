//! S3 authorization material.

use super::Address;
use super::credential::S3Credential;
use crate::capability::Access;
use crate::{AuthorizationFormatError, Permit, S3Error};
use dialog_effects::credential::Secret;
use serde::{Deserialize, Serialize};

/// S3 authorization material.
///
/// Wraps optional credentials. In production the Operator looks up
/// credentials from the secret store. For testing or public access,
/// `None` is used and produces unsigned requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct S3Authorization(pub Option<S3Credential>);

impl S3Authorization {
    /// Redeem this authorization for a presigned URL permit.
    ///
    /// With credentials, signs via SigV4. Without, builds an unsigned request.
    pub async fn redeem<R: Access>(
        &self,
        request: &R,
        address: &Address,
    ) -> Result<Permit, S3Error> {
        match &self.0 {
            Some(creds) => creds.authorize(request, address).await,
            None => Ok(S3Credential::permit(request, address)),
        }
    }
}

impl From<S3Credential> for S3Authorization {
    fn from(creds: S3Credential) -> Self {
        Self(Some(creds))
    }
}

impl TryFrom<S3Authorization> for Secret {
    type Error = AuthorizationFormatError;

    fn try_from(auth: S3Authorization) -> Result<Self, AuthorizationFormatError> {
        serde_ipld_dagcbor::to_vec(&auth)
            .map(Secret)
            .map_err(|e| AuthorizationFormatError::Serialize(e.to_string()))
    }
}

impl TryFrom<Secret> for S3Authorization {
    type Error = AuthorizationFormatError;

    fn try_from(secret: Secret) -> Result<Self, AuthorizationFormatError> {
        serde_ipld_dagcbor::from_slice(&secret.0)
            .map_err(|e| AuthorizationFormatError::Deserialize(e.to_string()))
    }
}
