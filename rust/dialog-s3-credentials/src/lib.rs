//! S3 credentials and presigned URL generation.
//!
//! This crate provides credential types for S3-compatible storage services
//! including AWS S3 and Cloudflare R2.
//!
//! It has minimal dependencies and works on both native and WebAssembly targets,
//! making it suitable for use in Cloudflare Workers and other constrained environments.
//!
//! # Credential Types
//!
//! - [`s3::S3Credentials`] - Direct S3 authentication (SigV4 signed)
//! - [`Address`] - S3 endpoint/bucket/region + URL building + request authorization
//! - [`ucan::Credentials`] - UCAN-based authorization via external access service (requires `ucan` feature)
//!
//! # Example
//!
//! ```no_run
//! use dialog_s3_credentials::{Address, s3};
//! use dialog_s3_credentials::capability::storage::{Storage, Store, Get};
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
//! // Create credentials for authenticated access
//! let credentials = s3::S3Credentials::new(
//!     "AKIAIOSFODNN7EXAMPLE",
//!     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//! );
//!
//! // Build a capability and authorize it using the address.
//! let capability = Subject::from(subject)
//!     .attenuate(Storage)
//!     .attenuate(Store::new("index"))
//!     .invoke(Get::new(b"my-key"));
//!
//! let request = address.authorize(&capability, Some(&credentials)).await?;
//! println!("Presigned URL: {}", request.url);
//! # Ok(())
//! # }
//! ```

mod address;
pub mod capability;
mod checksum;
mod credentials;
pub mod s3;

#[cfg(feature = "ucan")]
pub mod ucan;

pub use address::*;
pub use capability::{AccessError, Acl, AuthorizedRequest, Precondition, S3Request};
pub use capability::{archive, memory, storage};
pub use checksum::*;
pub use credentials::*;
