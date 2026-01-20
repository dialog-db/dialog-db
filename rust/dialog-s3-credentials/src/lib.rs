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
//! All credential types implement [`Provider<storage::Get>`](dialog_common::Provider) etc.
//! to produce [`RequestDescriptor`] for making S3 requests.
//!
//! # Example
//!
//! ```no_run
//! use dialog_s3_credentials::{Address, s3, storage};
//! use dialog_common::Effect;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create address for S3 bucket
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//!
//! // Create credentials (public or private)
//! let credentials = s3::Credentials::private(
//!     address,
//!     "AKIAIOSFODNN7EXAMPLE",
//!     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//! )?;
//!
//! // Get a presigned request descriptor
//! let descriptor = storage::Get::new("", "my-key")
//!     .perform(&credentials)
//!     .await?;
//!
//! println!("Presigned URL: {}", descriptor.url);
//! # Ok(())
//! # }
//! ```

pub mod access;
pub mod address;
pub mod checksum;
pub mod s3;

#[cfg(feature = "ucan")]
pub mod ucan;

// Primary exports
pub use access::{AuthorizationError, RequestDescriptor, memory, storage};
pub use address::Address;
pub use checksum::{Checksum, Hasher};
