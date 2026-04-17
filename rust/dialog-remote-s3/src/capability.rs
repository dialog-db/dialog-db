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

use chrono::{DateTime, Utc};
use dialog_common::{Checksum, ConditionalSend, ConditionalSync};
use serde::{Deserialize, Serialize};

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;

pub mod archive;
pub mod memory;

/// Precondition for conditional S3 operations (CAS semantics).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Precondition {
    /// No precondition - unconditional operation.
    None,
    /// Update only if current ETag matches (If-Match: <etag>).
    IfMatch(String),
    /// Update only if key doesn't exist (If-None-Match: *).
    IfNoneMatch,
}

/// A concrete, captured S3 request description.
///
/// Produced by `Request::from(&access)` where `access: Access`; the
/// result is a self-contained value that no longer depends on the
/// capability and can be signed / bound into an
/// [`S3Authorization`](crate::S3Authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// The HTTP method.
    pub method: String,
    /// The URL path.
    pub path: String,
    /// Body checksum, if any.
    pub checksum: Option<Checksum>,
    /// Service name for SigV4 (usually `"s3"`).
    pub service: String,
    /// Signed-URL expiration in seconds.
    pub expires: u64,
    /// Signing timestamp.
    pub time: DateTime<Utc>,
    /// Query parameters.
    pub params: Option<Vec<(String, String)>>,
    /// Conditional-operation precondition.
    pub precondition: Precondition,
}

/// This trait can be implemented by effects that can be translated into
/// S3 requests. Any `Access` value can be converted into a concrete
/// [`Request`] via `Request::from(&access)`.
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
}

impl<T: Access + ?Sized> From<&T> for Request {
    fn from(access: &T) -> Self {
        Self {
            method: access.method().to_string(),
            path: access.path(),
            checksum: access.checksum(),
            service: access.service().to_string(),
            expires: access.expires(),
            time: access.time(),
            params: access.params(),
            precondition: access.precondition(),
        }
    }
}

/// Get the current time as a UTC datetime.
pub fn current_time() -> DateTime<Utc> {
    DateTime::<Utc>::from(dialog_common::time::now())
}
