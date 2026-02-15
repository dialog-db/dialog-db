//! UCAN-credential network address and connection.

/// Address for UCAN-delegated S3 access.
pub type Address = dialog_s3_credentials::ucan::Credentials;

/// UCAN addresses resolve to S3-backed connections via the access service.
pub type Connection<Issuer> = crate::s3::S3<Issuer>;

impl From<Address> for super::Address {
    fn from(credentials: Address) -> Self {
        Self::Ucan(credentials)
    }
}
