//! S3 request translation for capabilities.
//!
//! This module defines the [`S3Request`] type and per-capability
//! `From<&Capability<Fx>>` impls that translate capabilities into
//! concrete S3 request descriptions (method, path, headers, etc).
//!
//! The [`IntoRequest`] trait is a blanket alias that lets downstream
//! code write `Capability<Fx>: IntoRequest` instead of the verbose
//! HRTB bound `for<'a> S3Request: From<&'a Capability<Fx>>`.
//!
//! # Example
//!
//! ```
//! use dialog_capability::{Subject, did};
//! use dialog_effects::archive::{Archive, Catalog, Get, Put};
//! use dialog_common::Blake3Hash;
//! use dialog_remote_s3::capability::S3Request;
//!
//! let subject = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
//!
//! // Build a capability to get content from the "index" catalog
//! let digest = Blake3Hash::hash(b"hello");
//! let get = Subject::from(subject.clone())
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let request = S3Request::from(&get);
//! assert_eq!(request.method, "GET");
//! ```
//!
//! # Submodules
//!
//! - [`memory`]: `From<&Capability<Fx>> for S3Request` impls for memory capabilities
//! - [`archive`]: `From<&Capability<Fx>> for S3Request` impls for archive capabilities

use chrono::{DateTime, Utc};
use dialog_common::Checksum;
use serde::{Deserialize, Serialize};

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;
pub const DEFAULT_METHOD: &str = "GET";
pub const SERVICE: &str = "s3";
pub const DEFAULT_PATH: &str = "/";

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
/// Produced by `S3Request::from(&access)` where `access: Access`; the
/// result is a self-contained value that no longer depends on the
/// capability and can be signed / bound into an
/// [`S3Authorization`](crate::S3Authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Request {
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

impl Default for S3Request {
    fn default() -> Self {
        Self {
            method: DEFAULT_METHOD.to_string(),
            path: DEFAULT_PATH.to_string(),
            service: SERVICE.to_string(),
            expires: DEFAULT_EXPIRES,
            time: current_time(),
            checksum: None,
            params: None,
            precondition: Precondition::None,
        }
    }
}

/// Types that can be converted into a [`S3Request`] by reference.
///
/// Blanket-implemented for any `T` where `S3Request: From<&T>` holds. Use
/// as a bound in place of the verbose `for<'a> S3Request: From<&'a T>`
/// HRTB, then call [`to_request`](Self::to_request) to perform the
/// conversion.
pub trait IntoRequest {
    /// Borrow this value and convert it into a [`S3Request`].
    fn to_request(&self) -> S3Request;
}

impl<T: ?Sized> IntoRequest for T
where
    for<'a> S3Request: From<&'a T>,
{
    fn to_request(&self) -> S3Request {
        S3Request::from(self)
    }
}

/// Get the current time as a UTC datetime.
pub fn current_time() -> DateTime<Utc> {
    DateTime::<Utc>::from(dialog_common::time::now())
}
