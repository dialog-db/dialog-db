//! S3 request translation for capabilities.
//!
//! This module provides [`Access`] implementations for capability types
//! defined in [`dialog_effects`], enabling them to be translated into
//! presigned S3 URLs.
//!
//! Effect types and their claim types from `dialog_effects` are used directly —
//! this module only provides the S3-specific request translation.
//!
//! # Example
//!
//! ```
//! use dialog_capability::{Subject, did};
//! use dialog_effects::archive::{Archive, Catalog, Get, Put};
//! use dialog_common::Blake3Hash;
//!
//! let subject = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
//!
//! // Build a capability to get content from the "index" catalog
//! let digest = Blake3Hash::hash(b"hello");
//! let get_capability = Subject::from(subject.clone())
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! // Build a capability to put content in the "blob" catalog
//! let content = b"my-value";
//! let digest = Blake3Hash::hash(content);
//! let put_capability = Subject::from(subject)
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("blob"))
//!     .invoke(Put::new(digest, content.to_vec()));
//! ```
//!
//! # Submodules
//!
//! - [`memory`]: `Access` impls for memory capabilities
//! - [`archive`]: `Access` impls for archive capabilities

use super::checksum::Checksum;
use chrono::{DateTime, Utc};
use dialog_common::{ConditionalSend, ConditionalSync};

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;

pub mod archive;
pub mod memory;

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
pub trait Access: ConditionalSend + ConditionalSync {
    /// The HTTP method for this request.
    fn method(&self) -> &'static str;

    /// The URL path for this request.
    fn path(&self) -> String;

    /// The checksum of the body, if any.
    fn checksum(&self) -> Option<Checksum> {
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
    DateTime::<Utc>::from(dialog_common::time::now())
}
