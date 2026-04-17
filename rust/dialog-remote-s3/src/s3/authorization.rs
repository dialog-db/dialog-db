//! S3 authorization material.

use super::Address;
use super::credential::S3Credential;
use crate::request::S3Request;
use crate::{AuthorizationFormatError, Permit, S3Error};
use dialog_effects::credential::Secret;
use serde::{Deserialize, Serialize};

/// S3 authorization material — credential paired with the specific
/// request it authorizes.
///
/// Unlike a bare credential, this is request-bound: [`redeem`](Self::redeem)
/// produces a permit for exactly the request captured here. The credential
/// itself is still optional — `None` means public/unsigned access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Authorization {
    /// Credential for SigV4 signing, or `None` for unsigned/public access.
    credential: Option<S3Credential>,
    /// The specific request this authorization is bound to.
    request: S3Request,
}

impl S3Authorization {
    /// Create an authorization for public (unsigned) access to a request.
    pub fn public(request: S3Request) -> Self {
        Self {
            credential: None,
            request,
        }
    }

    /// Redeem this authorization for a presigned URL permit.
    ///
    /// With a credential, signs the captured request via SigV4. Without,
    /// builds an unsigned request.
    pub async fn redeem(&self, address: &Address) -> Result<Permit, S3Error> {
        match &self.credential {
            Some(creds) => creds.authorize(&self.request, address).await,
            None => Ok(S3Credential::permit(&self.request, address)),
        }
    }
}

impl S3Request {
    /// Attest this request with a credential, producing an
    /// [`S3Authorization`] that can be redeemed for a presigned URL.
    pub fn attest(self, credential: S3Credential) -> S3Authorization {
        S3Authorization {
            credential: Some(credential),
            request: self,
        }
    }
}

/// Serialize / deserialize `S3Credential` inside / out of `Secret` for
/// storage in the credential provider. Only the credential is kept — the
/// request is bound at authorize-time by [`S3Fork`](crate::S3Fork).
impl From<S3Credential> for Secret {
    fn from(credential: S3Credential) -> Self {
        // Unwrap-serialization-failure is a programmer error: S3Credential
        // is plain Strings, always serializable.
        Secret(serde_ipld_dagcbor::to_vec(&credential).expect("S3Credential serializes"))
    }
}

impl TryFrom<Secret> for S3Credential {
    type Error = AuthorizationFormatError;

    fn try_from(secret: Secret) -> Result<Self, AuthorizationFormatError> {
        serde_ipld_dagcbor::from_slice(&secret.0)
            .map_err(|e| AuthorizationFormatError::Deserialize(e.to_string()))
    }
}
