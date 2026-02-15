//! S3-credential network address and connection.

/// Address for direct S3 access (public or private credentials).
#[cfg(feature = "s3")]
pub type Address = dialog_s3_credentials::s3::Credentials;

/// S3-backed connection.
#[cfg(feature = "s3")]
pub type Connection<Issuer> = crate::s3::S3<Issuer>;

/// Stub connection when the `s3` feature is disabled.
#[cfg(not(feature = "s3"))]
pub type Connection<Issuer> = dialog_common::Impossible<Issuer>;

#[cfg(feature = "s3")]
impl From<Address> for super::Address {
    fn from(credentials: Address) -> Self {
        Self::S3(credentials)
    }
}
