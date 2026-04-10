//! S3 site type, credential types, and Provider implementations.
//!
//! This module provides [`S3`], an S3-compatible storage type
//! that executes pre-authorized HTTP requests via presigned URLs.
//!
//! Submodules:
//! - [`credentials`] — S3 credential types for direct AWS SigV4 signing
//! - [`provider`] — `Provider<Fork<S3, Fx>>` implementations for archive, memory, storage

mod authorization;
pub(crate) mod credentials;
mod invocation;
mod permit;
pub mod provider;

pub use authorization::S3Authorization;
pub use credentials::S3Credentials;
pub use invocation::S3Invocation;
pub use permit::Permit;

use super::Address;
use dialog_capability::site::{Authentication, Site};

/// S3 direct-access site.
///
/// Uses credential-based [`Authentication`] rather than capability delegation.
/// Authorization is handled via SigV4 presigned URLs on the [`Address`].
#[derive(Debug, Clone, Copy, Default)]
pub struct S3;

impl Authentication for S3 {
    type Credentials = S3Credentials;
}

impl Site for S3 {
    type Authorization = S3Authorization;
    type Address = Address;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    #[dialog_common::test]
    fn it_creates_address() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );

        assert_eq!(address.endpoint(), "https://s3.us-east-1.amazonaws.com");
        assert_eq!(address.region(), "us-east-1");
        assert_eq!(address.bucket(), "my-bucket");
    }

    mod url_building_tests {
        use super::*;

        #[dialog_common::test]
        fn it_creates_address_for_virtual_hosted() {
            let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
            assert!(!address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_path_style_for_localhost() {
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_allows_forcing_path_style() {
            let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket")
                .with_path_style();
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_r2_address() {
            let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
            assert!(!address.path_style());
        }
    }
}
