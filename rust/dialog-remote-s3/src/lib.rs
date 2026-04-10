//! S3-compatible remote storage backend for dialog-db.
//!
//! This crate provides the [`S3`] site type, credential/address types,
//! and credential-based access control for S3-compatible storage services.
//!
//! # Core Types
//!
//! - [`Address`] - S3 endpoint/bucket/region + URL resolution
//! - [`S3Credential`] - Direct S3 authentication (SigV4 signed)
//! - [`S3`] - Site marker implementing `Provider<Fork<S3, Fx>>` for HTTP execution
//! - [`S3Authorization`] - Authorization material wrapping optional credentials
//!
//! # Example
//!
//! ```no_run
//! use dialog_remote_s3::{Address, S3Credential, S3Authorization};
//! use dialog_effects::archive::{Archive, Catalog, Get};
//! use dialog_capability::{Subject, did};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::builder("https://s3.us-east-1.amazonaws.com")
//!     .region("us-east-1")
//!     .bucket("my-bucket")
//!     .build()?;
//!
//! let subject = did!("key:zSubject");
//!
//! let auth = S3Authorization::from(S3Credential::new(
//!     "AKIAIOSFODNN7EXAMPLE",
//!     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//! ));
//!
//! let capability = Subject::from(subject)
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("blobs"))
//!     .invoke(Get::new([0u8; 32]));
//!
//! let request = auth.redeem(&capability, &address).await?;
//! println!("Presigned URL: {}", request.url);
//! # Ok(())
//! # }
//! ```

pub mod capability;
mod error;
pub mod s3;

#[cfg(feature = "helpers")]
pub mod helpers;

pub use capability::{Access, Precondition};
pub use capability::{archive, memory};
pub use error::S3Error;
pub use s3::*;
