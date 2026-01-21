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
//! use dialog_s3_credentials::{Address, s3, access};
//! use dialog_s3_credentials::access::Signer;
//! use dialog_s3_credentials::capability::storage::{Storage, Store};
//! use dialog_common::capability::{Capability, Subject};
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
//! let subject = "did:key:zSubject";
//!
//! // Create credentials (public or private)
//! let credentials = s3::Credentials::private(
//!     address,
//!     subject,
//!     "AKIAIOSFODNN7EXAMPLE",
//!     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//! )?;
//!
//! // Build capability chain: Subject -> Storage -> Store -> access::storage::Get
//! // Uses capability types for hierarchy, access types for effects (Claim impl)
//! let capability: Capability<access::storage::Get> = Subject::from(subject)
//!     .attenuate(Storage)
//!     .attenuate(Store::new("index"))
//!     .invoke(access::storage::Get::new(b"my-key"));
//!
//! // Sign the capability to get a presigned URL
//! let descriptor = credentials.sign(&capability).await?;
//!
//! println!("Presigned URL: {}", descriptor.url);
//! # Ok(())
//! # }
//! ```

pub mod access;
pub mod address;
pub mod capability;
pub mod checksum;
pub mod s3;

#[cfg(feature = "ucan")]
pub mod ucan;

// Primary exports
pub use access::{AuthorizationError, RequestDescriptor, memory, storage};
pub use address::Address;
pub use checksum::{Checksum, Hasher};
