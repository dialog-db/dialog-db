//! S3-compatible remote storage backend for dialog-db.
//!
//! This crate provides the [`S3`] site type, credential/address types,
//! and capability-based access control for S3-compatible storage services.
//!
//! # Core Types
//!
//! - [`Address`] - S3 endpoint/bucket/region + URL building + request authorization
//! - [`S3Credentials`] - Direct S3 authentication (SigV4 signed)
//! - [`S3`] - Site marker implementing `Provider<Fork<S3, Fx>>` for HTTP execution
//!
//! # Example
//!
//! ```no_run
//! use dialog_remote_s3::{Address, S3Credentials};
//! use dialog_effects::storage::{Storage, Store, Get};
//! use dialog_capability::{Subject, did};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create address for S3 bucket
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//!
//! // Subject DID identifies whose data we're accessing (used as path prefix)
//! let subject = did!("key:zSubject");
//!
//! // Attach credentials for authenticated access
//! let address = address.with_credentials(S3Credentials::new(
//!     "AKIAIOSFODNN7EXAMPLE",
//!     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//! ));
//!
//! // Build a capability and authorize it using the address.
//! let capability = Subject::from(subject)
//!     .attenuate(Storage)
//!     .attenuate(Store::new("index"))
//!     .invoke(Get::new(b"my-key"));
//!
//! let request = address.authorize(&capability).await?;
//! println!("Presigned URL: {}", request.url);
//! # Ok(())
//! # }
//! ```

mod address;
mod authorized;
pub mod capability;
mod checksum;
mod error;
mod key;
mod permit;
pub mod s3;

#[cfg(feature = "list")]
#[allow(dead_code)]
mod list;

#[cfg(feature = "helpers")]
pub mod helpers;

pub use address::*;
pub use authorized::Authorized;
pub use capability::{Access, Acl, Precondition};
pub use capability::{archive, memory, storage};
pub use checksum::*;
pub use error::AccessError;
pub use permit::Permit;

// Re-export site types at crate root
pub use s3::{Bucket, RequestDescriptorExt, S3, S3StorageError};

// Re-export S3Credentials at crate root for convenience
pub use s3::S3Credentials;

// Re-export key encoding
pub use key::{decode as decode_s3_key, encode as encode_s3_key};

#[cfg(feature = "list")]
pub use list::ListResult;
