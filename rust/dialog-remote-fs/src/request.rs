//! FS request translation for capabilities.
//!
//! Mirrors `dialog_remote_s3::request`: each capability type is translated
//! into a concrete [`FsRequest`] description (operation kind + path segments)
//! via per-capability `From<&Capability<Fx>> for FsRequest` impls. The
//! [`IntoRequest`] trait is the convenient bound to write at call sites.

use serde::{Deserialize, Serialize};

pub mod archive;
pub mod memory;

/// The kind of filesystem operation a request describes.
///
/// Mirrors HTTP verbs from `dialog_remote_s3::request::S3Request::method`,
/// but as a typed enum since there's no on-the-wire protocol to obey here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsOp {
    /// Read the file at the request's path; missing path is `Ok(None)`.
    Read,
    /// Write the file at the request's path. CAS preconditions, if any,
    /// are enforced by the provider via the captured `precondition`.
    Write,
    /// Delete the file at the request's path. CAS preconditions, if any,
    /// are enforced by the provider.
    Delete,
}

/// Precondition for conditional FS operations (CAS semantics for memory cells).
///
/// Mirrors `dialog_remote_s3::request::Precondition`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    /// No precondition — unconditional operation.
    None,
    /// Write only if the current edition (BLAKE3 hash of existing content)
    /// matches the given version. Used for CAS publishes.
    IfMatch(String),
    /// Write only if the file does not exist.
    IfNoneMatch,
}

/// A concrete, captured FS request description.
///
/// Produced by `FsRequest::from(&capability)` and embedded in an
/// [`FsAuthorization`](crate::fs::FsAuthorization). Self-contained — no
/// longer depends on the source capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsRequest {
    /// The operation kind.
    pub op: FsOp,
    /// Path segments under the registered directory root, e.g.
    /// `["archive", "index", "1A2B…"]`. The provider joins these with
    /// platform-appropriate separators when navigating the directory
    /// handle.
    pub path: Vec<String>,
    /// CAS precondition, if any (memory cells only — archive operations
    /// are content-addressed and idempotent).
    pub precondition: Precondition,
}

impl FsRequest {
    /// Construct a request from operation + path + no precondition.
    pub fn new(op: FsOp, path: Vec<String>) -> Self {
        Self {
            op,
            path,
            precondition: Precondition::None,
        }
    }

    /// Attach a precondition to this request.
    pub fn with_precondition(mut self, precondition: Precondition) -> Self {
        self.precondition = precondition;
        self
    }
}

/// Types convertible into an [`FsRequest`] by reference.
///
/// Blanket-implemented for any `T` where `FsRequest: From<&T>` holds.
/// Mirrors `dialog_remote_s3::request::IntoRequest`.
pub trait IntoRequest {
    /// Borrow this value and convert it into an [`FsRequest`].
    fn to_request(&self) -> FsRequest;
}

impl<T: ?Sized> IntoRequest for T
where
    for<'a> FsRequest: From<&'a T>,
{
    fn to_request(&self) -> FsRequest {
        FsRequest::from(self)
    }
}
