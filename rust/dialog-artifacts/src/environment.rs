//! Concrete environment composition for the repository layer.
//!
//! Uses [`Environment<Local, Credentials>`] to compose local storage
//! and credential capabilities.

use dialog_storage::provider::FileSystem;
#[cfg(any(test, feature = "helpers"))]
use dialog_storage::provider::Volatile;

pub use dialog_effects::environment::Environment;

use dialog_remote_s3::Address;

use crate::repository::credentials::Credentials;

pub use crate::repository::remote::address::RemoteAddress;

/// Extract the [`Address`] from a [`RemoteAddress`].
///
/// Pulls out the S3 address from the credentials bundle.
pub fn to_s3_address(remote: &RemoteAddress) -> Result<Address, dialog_remote_s3::AccessError> {
    match remote {
        RemoteAddress::S3(addr, _) => Ok(addr.clone()),
        #[cfg(feature = "ucan")]
        RemoteAddress::Ucan(_) => Err(dialog_remote_s3::AccessError::Configuration(
            "UCAN credentials cannot be converted to an S3 address directly".to_string(),
        )),
    }
}

/// Native environment: filesystem local storage with operator credentials.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeEnvironment = Environment<FileSystem, Credentials<()>>;

/// Web environment: IndexedDB local storage with operator credentials.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type WebEnvironment = Environment<dialog_storage::provider::IndexedDb, Credentials<()>>;

/// Test environment: in-memory local storage, no credentials.
#[cfg(any(test, feature = "helpers"))]
pub type TestEnvironment = Environment<Volatile>;
