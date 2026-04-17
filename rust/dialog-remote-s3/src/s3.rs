//! S3 site type, credential types, and Provider implementations.
//!
//! This module provides [`S3`], an S3-compatible storage type
//! that executes pre-authorized HTTP requests via presigned URLs.
//!
//! Submodules:
//! - [`credentials`] — S3 credential types for direct AWS SigV4 signing
//! - [`provider`] — `Provider<Fork<S3, Fx>>` implementations for archive, memory, storage

mod address;
mod authorization;
pub(crate) mod credential;
mod invocation;
mod permit;
pub mod provider;

pub use address::{Address, AddressBuilder};
pub use authorization::S3Authorization;
pub use credential::S3Credential;
pub use invocation::S3Invocation;
pub use permit::Permit;

use dialog_capability::fork::Fork;
use dialog_capability::site::Site;
use dialog_capability::{Capability, Effect};

/// S3 direct-access site.
///
/// Authorization is handled via SigV4 presigned URLs on the [`Address`].
#[derive(Debug, Clone, Copy, Default)]
pub struct S3;

/// Site-owned fork wrapper for S3.
///
/// Carries the `Authorize` impl for S3: fetches session identity from
/// the env via `authority::Identify`, loads credentials for that
/// identity, and bundles them with the capability + address into a
/// `ForkInvocation`.
pub struct S3Fork<Fx: Effect> {
    /// The capability being authorized.
    pub capability: Capability<Fx>,
    /// The S3 address to authorize against.
    pub address: Address,
}

impl<Fx: Effect> From<Fork<S3, Fx>> for S3Fork<Fx> {
    fn from(fork: Fork<S3, Fx>) -> Self {
        let (capability, address) = fork.into_parts();
        Self {
            capability,
            address,
        }
    }
}

impl Site for S3 {
    type Authorization = S3Authorization;
    type Address = Address;
    type Fork<Fx: Effect> = S3Fork<Fx>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_creates_address() {
        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("my-bucket")
            .build()
            .unwrap();

        assert_eq!(address.region(), "us-east-1");
        assert_eq!(address.bucket(), "my-bucket");
    }

    mod url_building_tests {
        use super::*;

        #[dialog_common::test]
        fn it_creates_address_for_virtual_hosted() {
            let address = Address::builder("https://s3.amazonaws.com")
                .region("us-east-1")
                .bucket("my-bucket")
                .build()
                .unwrap();
            assert!(!address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_path_style_for_localhost() {
            let address = Address::builder("http://localhost:9000")
                .region("us-east-1")
                .bucket("bucket")
                .build()
                .unwrap();
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_allows_forcing_path_style() {
            let address = Address::builder("https://custom-s3.example.com")
                .region("us-east-1")
                .bucket("bucket")
                .path_style(true)
                .build()
                .unwrap();
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_r2_address() {
            let address = Address::builder("https://abc123.r2.cloudflarestorage.com")
                .region("auto")
                .bucket("bucket")
                .build()
                .unwrap();
            assert!(!address.path_style());
        }
    }
}
