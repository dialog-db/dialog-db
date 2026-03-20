//! Concrete environment composition for the repository layer.
//!
//! Uses [`Environment<Local, Credentials>`] to compose local storage
//! and credential capabilities.

use dialog_storage::provider::FileSystem;
#[cfg(any(test, feature = "helpers"))]
use dialog_storage::provider::Volatile;

pub use dialog_effects::environment::Environment;

use dialog_s3_credentials::s3::S3Site;

use crate::repository::credentials::Credentials;

/// Serializable remote address configuration.
///
/// Stored in memory cells. Convert to an [`S3Site`](dialog_s3_credentials::s3::S3Site)
/// for execution via [`to_s3_site`].
pub type RemoteAddress = dialog_s3_credentials::Credentials;

/// Convert a [`RemoteAddress`] to an [`S3Site`] for capability execution.
///
/// Extracts the addressing info (endpoint, region, bucket) from the
/// serialized credentials and builds a pure site configuration.
pub fn to_s3_site(address: &RemoteAddress) -> Result<S3Site, dialog_s3_credentials::AccessError> {
    match address {
        RemoteAddress::S3(s3_creds) => {
            let addr = dialog_s3_credentials::Address::new(
                s3_creds.endpoint(),
                s3_creds.region(),
                s3_creds.bucket(),
            );
            S3Site::new(addr)
        }
        #[cfg(feature = "ucan")]
        RemoteAddress::Ucan(_) => Err(dialog_s3_credentials::AccessError::Configuration(
            "UCAN credentials cannot be converted to an S3 site directly".to_string(),
        )),
    }
}

/// Native environment: filesystem local storage with operator credentials.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeEnvironment = Environment<FileSystem, Credentials>;

/// Web environment: IndexedDB local storage with operator credentials.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type WebEnvironment = Environment<dialog_storage::provider::IndexedDb, Credentials>;

/// Test environment: in-memory local storage, no credentials.
#[cfg(any(test, feature = "helpers"))]
pub type TestEnvironment = Environment<Volatile>;
