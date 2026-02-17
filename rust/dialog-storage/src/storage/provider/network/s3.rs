//! S3-credential network address and connection.

use crate::resource::Resource;

/// Credentials for direct S3 access (public or private).
pub type Credentials = dialog_s3_credentials::s3::Credentials;

/// S3-backed connection.
pub type Connection<Issuer> = crate::s3::S3<Issuer>;

/// Opens an S3-backed connection from `(Credentials, Issuer)`.
///
/// The address pairs S3 credentials (endpoint, region, bucket, optional
/// access keys) with an issuer identity. Credentials are converted into the
/// internal `S3Credentials` format via `Into`, and the issuer is cloned so
/// the pool can keep the address around for cache-key comparisons.
///
/// Opening always succeeds (`Error = Infallible`) because the S3 client is
/// constructed lazily — actual HTTP errors surface on first use, not here.
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
