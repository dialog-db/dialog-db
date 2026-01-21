//! Access commands for S3 storage operations.
//!
//! This module defines request types for storage and memory operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.
//! Use a `Signer` to authorize claims and produce `RequestDescriptor`s.

use super::checksum::Checksum;
use chrono::{DateTime, Utc};
use dialog_common::ConditionalSend;
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;
use thiserror::Error;
use url::Url;

#[cfg(target_arch = "wasm32")]
use web_time::{SystemTime, web::SystemTimeExt};

#[cfg(feature = "ucan")]
use std::collections::BTreeMap;
#[cfg(feature = "ucan")]
use ucan::promise::Promised;

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;

pub mod archive;
pub mod memory;
pub mod storage;

/// Wrapper for UCAN invocation arguments that enables generic deserialization.
///
/// # Example
///
/// ```ignore
/// use dialog_s3_credentials::access::{Args, storage};
///
/// let cmd: storage::Get = Args(&args).deserialize()?;
/// ```
#[cfg(feature = "ucan")]
pub struct Args<'a>(pub &'a BTreeMap<String, Promised>);

#[cfg(feature = "ucan")]
impl Args<'_> {
    /// Deserialize arguments into the target type.
    pub fn deserialize<T: serde::de::DeserializeOwned>(self) -> Result<T, AuthorizationError> {
        use ipld_core::ipld::Ipld;

        // Convert BTreeMap<String, Promised> to Ipld::Map
        let ipld_map: BTreeMap<String, Ipld> = self
            .0
            .iter()
            .map(|(k, v)| {
                Ipld::try_from(v)
                    .map(|ipld| (k.clone(), ipld))
                    .map_err(|e| {
                        AuthorizationError::Invocation(format!("Promise not resolved: {}", e))
                    })
            })
            .collect::<Result<_, _>>()?;

        let ipld = Ipld::Map(ipld_map);

        ipld_core::serde::from_ipld(ipld).map_err(|e| {
            AuthorizationError::Invocation(format!("Failed to parse arguments: {}", e))
        })
    }
}

/// Error type for authorization operations.
#[derive(Debug, Error)]
pub enum AuthorizationError {
    /// No delegation found for the subject.
    #[error("No delegation for subject: {0}")]
    NoDelegation(String),

    /// Failed to build the invocation.
    #[error("Invocation error: {0}")]
    Invocation(String),

    /// Access service returned an error.
    #[error("Access service error: {0}")]
    Service(String),

    /// Invalid configuration.
    #[error("Configuration error: {0}")]
    Configuration(String),
}

/// Describes an HTTP request to perform an authorized operation.
///
/// This is the result of authorizing a claim - it contains all the
/// information needed to make the actual HTTP request.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorizedRequest {
    /// The presigned URL to use.
    pub url: Url,
    /// HTTP method (GET, PUT, DELETE).
    pub method: String,
    /// Headers to include in the request.
    pub headers: Vec<(String, String)>,
}

/// Precondition for conditional S3 operations (CAS semantics).
#[derive(Debug, Clone)]
pub enum Precondition {
    /// No precondition - unconditional operation.
    None,
    /// Update only if current ETag matches (If-Match: <etag>).
    IfMatch(String),
    /// Update only if key doesn't exist (If-None-Match: *).
    IfNoneMatch,
}

/// This trait can be implemented by effects that that be translated into
/// S3 requests. Providing convenient way for creating authorizations in
/// form of presigned URLs + headers.
///
pub trait S3Request: ConditionalSend {
    /// The HTTP method for this request.
    fn method(&self) -> &'static str;

    /// The URL path for this request.
    fn path(&self) -> String;

    /// The checksum of the body, if any.
    fn checksum(&self) -> Option<&Checksum> {
        None
    }

    /// The service name for signing. Defaults to "s3".
    fn service(&self) -> &str {
        "s3"
    }

    /// URL signature expiration in seconds. Defaults to 24 hours.
    fn expires(&self) -> u64 {
        DEFAULT_EXPIRES
    }

    /// The timestamp for signing. Defaults to current time.
    fn time(&self) -> DateTime<Utc> {
        current_time()
    }

    /// Query parameters for the request.
    fn params(&self) -> Option<Vec<(String, String)>> {
        None
    }

    /// Precondition for conditional operations.
    fn precondition(&self) -> Precondition {
        Precondition::None
    }

    /// The ACL for this request, if any.
    fn acl(&self) -> Option<Acl> {
        None
    }
}

/// S3 Access Control List (ACL) settings.
///
/// These are canned ACLs supported by S3 and S3-compatible services.
/// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acl {
    /// Owner gets FULL_CONTROL. No one else has access rights.
    Private,
    /// Owner gets FULL_CONTROL. The AllUsers group gets READ access.
    PublicRead,
    /// Owner gets FULL_CONTROL. The AllUsers group gets READ and WRITE access.
    PublicReadWrite,
    /// Owner gets FULL_CONTROL. The AuthenticatedUsers group gets READ access.
    AuthenticatedRead,
    /// Object owner gets FULL_CONTROL. Bucket owner gets READ access.
    BucketOwnerRead,
    /// Both the object owner and the bucket owner get FULL_CONTROL.
    BucketOwnerFullControl,
}

impl Acl {
    /// Get the S3 ACL header value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
            Self::PublicReadWrite => "public-read-write",
            Self::AuthenticatedRead => "authenticated-read",
            Self::BucketOwnerRead => "bucket-owner-read",
            Self::BucketOwnerFullControl => "bucket-owner-full-control",
        }
    }
}

/// Get the current time as a UTC datetime.
pub fn current_time() -> DateTime<Utc> {
    #[cfg(target_arch = "wasm32")]
    {
        DateTime::<Utc>::from(SystemTime::now().to_std())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        DateTime::<Utc>::from(SystemTime::now())
    }
}
