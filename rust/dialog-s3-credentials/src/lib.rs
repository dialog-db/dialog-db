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
//! - [`s3::Credentials`] - Direct S3 access (public or SigV4 signed)
//! - [`ucan::Credentials`] - UCAN-based authorization via external access service (requires `ucan` feature)
//!
//! # Example
//!
//! ```no_run
//! use dialog_s3_credentials::{Address, s3};
//! use dialog_s3_credentials::capability::storage::{Storage, Store, Get};
//! use dialog_capability::{Subject, credential, did};
//! use dialog_capability::credential::Remote;
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
//! // Create credentials (public or private)
//! let credentials = s3::Credentials::private(
//!     address,
//!     "AKIAIOSFODNN7EXAMPLE",
//!     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//! )?;
//!
//! // Build a claim and authorize to get a presigned URL.
//! // S3 credentials serve as their own env for the Authorize/Redeem pipeline.
//! let authorization = credentials
//!     .claim(subject)
//!     .attenuate(Storage)
//!     .attenuate(Store::new("index"))
//!     .invoke(Get::new(b"my-key"))
//!     .acquire(&credentials)
//!     .await?;
//!
//! println!("Presigned URL: {}", authorization.site().url);
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
