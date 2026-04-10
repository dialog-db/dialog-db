//! S3 authorization material.

use dialog_capability::site::SiteAuthorization;

use super::credential::S3Credential;
use super::{Address, S3};
use crate::capability::Access;
use crate::{Permit, S3Error};

/// S3 authorization material.
///
/// Wraps optional credentials. In production the Operator looks up
/// credentials from the secret store. For testing or public access,
/// `None` is used and produces unsigned requests.
#[derive(Debug, Clone, Default)]
pub struct S3Authorization(pub Option<S3Credential>);

impl S3Authorization {
    /// Authorize a request, producing a presigned URL permit.
    ///
    /// With credentials, signs via SigV4. Without, builds an unsigned request.
    pub async fn permit<R: Access>(
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

impl SiteAuthorization for S3Authorization {
    type Protocol = S3;
}

impl From<S3Credential> for S3Authorization {
    fn from(creds: S3Credential) -> Self {
        Self(Some(creds))
    }
}
