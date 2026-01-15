//! AWS SigV4 presigned URL generation for S3-compatible storage.
//!
//! This crate provides presigned URL generation for S3-compatible storage services
//! including AWS S3 and Cloudflare R2, using [query string authentication].
//!
//! It has minimal dependencies and works on both native and WebAssembly targets,
//! making it suitable for use in Cloudflare Workers and other constrained environments.
//!
//! [query string authentication]: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
//!
//! # Authorization Strategies
//!
//! This crate provides multiple authorization strategies via the [`Authorizer`] trait:
//!
//! - [`Credentials`] - AWS SigV4 signing with access key and secret
//! - [`Public`] - No signing for public buckets
//! - [`UcanAuthorizer`] - UCAN-based authorization via external access service (requires `ucan` feature)
//!
//! # Example
//!
//! ```no_run
//! use dialog_s3_credentials::{Address, Authorizer, Credentials, RequestInfo, DEFAULT_EXPIRES};
//! use chrono::Utc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create address for S3 bucket
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//!
//! // Create credentials
//! let credentials = Credentials::new(
//!     address,
//!     "AKIAIOSFODNN7EXAMPLE",
//!     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//! )?;
//!
//! // Build the URL and authorize a GET request
//! let url = credentials.build_url("my-key")?;
//! let request = RequestInfo {
//!     method: "GET",
//!     url,
//!     region: credentials.region().to_string(),
//!     checksum: None,
//!     acl: None,
//!     expires: DEFAULT_EXPIRES,
//!     time: Utc::now(),
//!     service: "s3".to_string(),
//! };
//!
//! let auth = credentials.authorize(&request).await?;
//! println!("Presigned URL: {}", auth.url);
//! # Ok(())
//! # }
//! ```

pub mod access;
pub mod address;
pub mod checksum;

#[cfg(feature = "ucan")]
pub mod ucan;

pub use access::{
    Acl, Authorization, AuthorizationError, Authorizer, Credentials, DEFAULT_EXPIRES, Invocation,
    Public, RequestInfo,
};
pub use address::Address;
pub use checksum::{Checksum, Hasher};

#[cfg(feature = "ucan")]
pub use ucan::{DelegationChain, OperatorIdentity, UcanAuthorizer, UcanAuthorizerBuilder};
