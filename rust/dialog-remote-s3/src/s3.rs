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

use std::sync::Arc;

use dialog_capability::Effect;
use dialog_capability::Fork;
use dialog_capability::Site;

use crate::S3Error;
use crate::flight::Flight;

/// In-flight block GETs, joined by presigned URL.
///
/// A block is immutable content, so every caller holding the same
/// presigned URL gets the same bytes: the one read that is always safe to
/// share. The URL's signature binds it to the permit (and through it the
/// operator) that redeemed it, so a request signed for someone else carries
/// a different URL and never joins.
///
/// Mutable reads (memory cells) deliberately do not come through here.
pub(crate) type BlockGets = Flight<String, Result<(u16, Arc<Vec<u8>>), S3Error>>;

/// S3 direct-access site.
///
/// Authorization is handled via SigV4 presigned URLs on the [`Address`].
///
/// The site owns its in-flight block GETs, so readers join one another's
/// requests only through the same site (one `Network`, hence one
/// environment), and nothing outlives it. Clones share them.
#[derive(Debug, Clone, Default)]
pub struct S3 {
    gets: Arc<BlockGets>,
}

impl S3 {
    /// The in-flight block GETs shared by clones of this site.
    pub(crate) fn gets(&self) -> &BlockGets {
        &self.gets
    }
}

/// Site-owned fork wrapper for S3.
///
/// Thin newtype around [`Fork<S3, Fx>`] that carries the site-specific
/// [`Authorize`](dialog_capability::SiteFork) impl. Fetches
/// session identity from the env via `authority::Identify`, loads the
/// matching credential, and returns a [`ForkInvocation`] bound to the
/// captured request.
pub struct S3Fork<Fx: Effect>(Fork<S3, Fx>);

impl<Fx: Effect> From<Fork<S3, Fx>> for S3Fork<Fx> {
    fn from(fork: Fork<S3, Fx>) -> Self {
        Self(fork)
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
