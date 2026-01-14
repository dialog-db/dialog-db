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
//! use dialog_s3_credentials::{Credentials, Invocation, Address};
//! use url::Url;
//!
//! // Create credentials
//! let credentials = Credentials {
//!     access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
//!     secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
//! };
//!
//! // Create a simple GET request
//! struct GetRequest {
//!     url: Url,
//!     region: String,
//! }
//!
//! impl Invocation for GetRequest {
//!     fn method(&self) -> &'static str { "GET" }
//!     fn url(&self) -> &Url { &self.url }
//!     fn region(&self) -> &str { &self.region }
//! }
//!
//! let request = GetRequest {
//!     url: Url::parse("https://my-bucket.s3.us-east-1.amazonaws.com/my-key").unwrap(),
//!     region: "us-east-1".into(),
//! };
//!
//! let auth = credentials.authorize(&request).unwrap();
//! println!("Presigned URL: {}", auth.url);
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
