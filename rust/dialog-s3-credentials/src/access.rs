//! Access commands for S3 storage operations.
//!
//! This module defines effect types for authorization of storage and memory operations.
//! Each effect returns a `Result<RequestDescriptor, AuthorizationError>` for making authorized requests.

use super::checksum::Checksum;
use chrono::{DateTime, Utc};
#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

#[cfg(target_arch = "wasm32")]
use web_time::{SystemTime, web::SystemTimeExt};

#[cfg(feature = "ucan")]
use dialog_common::Effect;
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
                    .map_err(|e| AuthorizationError::Invocation(format!("Promise not resolved: {}", e)))
            })
            .collect::<Result<_, _>>()?;

        let ipld = Ipld::Map(ipld_map);

        ipld_core::serde::from_ipld(ipld)
            .map_err(|e| AuthorizationError::Invocation(format!("Failed to parse arguments: {}", e)))
    }
}

/// Unified command enum for all access operations.
///
/// Parsed from UCAN invocation command path and arguments.
#[cfg(feature = "ucan")]
#[derive(Debug)]
pub enum Do {
    Storage(storage::Do),
    Memory(memory::Do),
    Archive(archive::Do),
}

#[cfg(feature = "ucan")]
impl Effect for Do {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

#[cfg(feature = "ucan")]
impl<'a> TryFrom<(&'a [&'a str], Args<'a>)> for Do {
    type Error = AuthorizationError;

    fn try_from((segments, args): (&'a [&'a str], Args<'a>)) -> Result<Self, Self::Error> {
        match segments.first() {
            Some(&"storage") => Ok(Do::Storage((&segments[1..], args).try_into()?)),
            Some(&"memory") => Ok(Do::Memory((&segments[1..], args).try_into()?)),
            Some(&"archive") => Ok(Do::Archive((&segments[1..], args).try_into()?)),
            _ => Err(AuthorizationError::Invocation(format!(
                "Unknown command: {:?}",
                segments
            ))),
        }
    }
}

/// Trait for providers that can execute all access commands.
#[cfg(feature = "ucan")]
pub trait AccessProvider:
    storage::StorageProvider + memory::MemoryProvider + archive::ArchiveProvider
{
}

#[cfg(feature = "ucan")]
impl<T> AccessProvider for T where
    T: storage::StorageProvider + memory::MemoryProvider + archive::ArchiveProvider
{
}

#[cfg(feature = "ucan")]
impl Do {
    /// Perform this command using the given provider.
    pub async fn perform<P: AccessProvider>(
        self,
        provider: &P,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Do::Storage(cmd) => cmd.perform(provider).await,
            Do::Memory(cmd) => cmd.perform(provider).await,
            Do::Archive(cmd) => cmd.perform(provider).await,
        }
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
/// This is the result of authorizing an effect - it contains all the
/// information needed to make the actual HTTP request.
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestDescriptor {
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

/// Request metadata required for S3 authorization.
///
/// This trait captures all information needed to sign an S3 request:
/// - HTTP method, URL, checksum, ACL (request-specific)
/// - Region, service, expires, time (signing parameters)
///
pub trait Claim {
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

    fn params(&self) -> Option<Vec<(&str, &str)>> {
        None
    }

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
