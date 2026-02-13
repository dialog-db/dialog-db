//! Capability-based access control for S3 storage operations.
//!
//! This module defines a hierarchical capability system for authorizing S3 operations.
//! Capabilities are built by chaining attenuations from a subject (resource owner) down
//! to a specific operation (effect).
//!
//! # Capability Hierarchy
//!
//! Capabilities form a chain: `Subject` → `Attenuation` → `Policy` → `Effect`
//!
//! - **Subject**: The resource owner, identified by a DID (e.g., `did:key:z6Mk...`)
//! - **Attenuation**: Narrows scope to a domain (e.g., `Storage`, `Memory`, `Archive`)
//! - **Policy**: Further constrains within that domain (e.g., `Store::new("index")`)
//! - **Effect**: The actual operation to perform (e.g., `Get`, `Set`, `Delete`)
//!
//! # Example: Storage Operations
//!
//! ```
//! use dialog_capability::{Subject, did};
//! use dialog_s3_credentials::capability::storage::{Storage, Store, Get, Set};
//! use dialog_s3_credentials::Checksum;
//!
//! let subject = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
//!
//! // Build a capability to get a value from the "index" store
//! let get_capability = Subject::from(subject.clone())
//!     .attenuate(Storage)              // Domain: storage operations
//!     .attenuate(Store::new("index"))  // Policy: only the "index" store
//!     .invoke(Get::new(b"my-key"));    // Effect: get this specific key
//!
//! // Build a capability to set a value in the "blob" store
//! let checksum = Checksum::Sha256([0u8; 32]);
//! let set_capability = Subject::from(subject)
//!     .attenuate(Storage)
//!     .attenuate(Store::new("blob"))
//!     .invoke(Set::new(b"my-key", checksum));
//! ```
//!
//! # Example: Memory Operations (CAS cells)
//!
//! ```
//! use dialog_capability::{Subject, did};
//! use dialog_s3_credentials::capability::memory::{Memory, Space, Cell, Resolve, Publish};
//! use dialog_s3_credentials::Checksum;
//!
//! let subject = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
//! let user = "did:key:z6Mkuser...";
//!
//! // Resolve (read) a memory cell
//! let resolve = Subject::from(subject.clone())
//!     .attenuate(Memory)              // Domain: memory operations
//!     .attenuate(Space::new(user))    // Policy: user's namespace
//!     .attenuate(Cell::new("main"))   // Policy: specific cell name
//!     .invoke(Resolve);               // Effect: read the cell
//!
//! // Publish (write) to a memory cell with CAS semantics
//! let checksum = Checksum::Sha256([0u8; 32]);
//! let publish = Subject::from(subject)
//!     .attenuate(Memory)
//!     .attenuate(Space::new(user))
//!     .attenuate(Cell::new("main"))
//!     .invoke(Publish {
//!         checksum,
//!         when: Some("previous-etag".to_string()), // CAS: only if etag matches
//!     });
//! ```
//!
//! # Executing Capabilities
//!
//! Once built, capabilities can be executed against a provider (credentials):
//!
//! ```ignore
//! // With S3 credentials
//! let result = get_capability.perform(&mut s3_credentials).await?;
//!
//! // Result is an AuthorizedRequest with presigned URL and headers
//! println!("URL: {}", result.url);
//! println!("Method: {}", result.method);
//! ```
//!
//! # Submodules
//!
//! - [`storage`]: Key-value storage operations (`Get`, `Set`, `Delete`, `List`)
//! - [`memory`]: CAS memory cells (`Resolve`, `Publish`, `Retract`)
//! - [`archive`]: Content-addressed archive (`Get`, `Put`)

use super::checksum::Checksum;
use chrono::{DateTime, Utc};
use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;

pub mod archive;
pub mod memory;
pub mod storage;

/// Error type for authorization operations.
#[derive(Debug, Error)]
pub enum AccessError {
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
pub trait S3Request: ConditionalSend + ConditionalSync {
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
    DateTime::<Utc>::from(dialog_common::time::now())
}
