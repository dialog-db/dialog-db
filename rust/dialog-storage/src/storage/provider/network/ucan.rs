//! UCAN-credential network address and connection.

use crate::resource::Resource;

/// Credentials for UCAN-delegated S3 access.
pub type Credentials = dialog_s3_credentials::ucan::Credentials;

/// UCAN addresses resolve to S3-backed connections via the access service.
pub type Connection = crate::s3::S3;

/// Opens an S3-backed connection from UCAN credentials.
///
/// UCAN credentials carry a delegation chain that authorises access to a
/// remote storage site. Opening always succeeds (`Error = Infallible`)
/// because the actual credential exchange with the access service happens
/// lazily on first use, not here.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Resource<Credentials> for Connection {
    type Error = std::convert::Infallible;

    async fn open(address: &Credentials) -> Result<Self, Self::Error> {
        Ok(Connection::new(address.clone().into()))
    }
}
