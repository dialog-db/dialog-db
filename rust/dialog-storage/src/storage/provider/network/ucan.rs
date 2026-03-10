//! UCAN-credential network address and connection.

use crate::resource::Resource;

/// Credentials for UCAN-delegated S3 access.
pub type Credentials = dialog_s3_credentials::ucan::Credentials;

/// UCAN addresses resolve to S3-backed connections via the access service.
pub type Connection<Issuer> = crate::s3::S3<Issuer>;

/// Opens an S3-backed connection from UCAN `(Credentials, Issuer)`.
///
/// UCAN credentials carry a delegation chain that authorises access to a
/// remote storage site.  Like the direct-S3 path, credentials are converted
/// into the internal `S3Credentials` format via `Into` and the issuer is
/// cloned for pool cache-key use.
///
/// Opening always succeeds (`Error = Infallible`) because the actual
/// credential exchange with the access service happens lazily on first use,
/// not here.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Issuer: Clone + dialog_common::ConditionalSend + dialog_common::ConditionalSync>
    Resource<(Credentials, Issuer)> for Connection<Issuer>
{
    type Error = std::convert::Infallible;

    async fn open(address: &(Credentials, Issuer)) -> Result<Self, Self::Error> {
        let (credentials, issuer) = address;
        Ok(Connection::new(credentials.clone().into(), issuer.clone()))
    }
}
