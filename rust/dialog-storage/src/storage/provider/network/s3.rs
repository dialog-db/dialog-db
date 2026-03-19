//! S3-credential network address and connection.

use crate::resource::Resource;

/// Credentials for direct S3 access (public or private).
pub type Credentials = dialog_s3_credentials::s3::Credentials;

/// S3-backed connection.
pub type Connection = crate::s3::S3;

/// Opens an S3-backed connection from credentials.
///
/// Opening always succeeds (`Error = Infallible`) because the S3 client is
/// constructed lazily -- actual HTTP errors surface on first use, not here.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Resource<Credentials> for Connection {
    type Error = std::convert::Infallible;

    async fn open(address: &Credentials) -> Result<Self, Self::Error> {
        Ok(Connection::new(address.clone().into()))
    }
}
