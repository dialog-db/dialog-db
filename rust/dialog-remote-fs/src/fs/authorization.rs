//! FS authorization material.
//!
//! Mirrors [`dialog_remote_s3::S3Authorization`] structurally even though
//! FS has no credentials: redeeming produces an [`FsPermit`] that names
//! the registered handle and carries the captured request.

use super::{FsAddress, FsPermit};
use crate::request::FsRequest;
use serde::{Deserialize, Serialize};

/// FS authorization material — a captured request, ready to be paired with
/// an [`FsAddress`] for execution.
///
/// Unlike S3's `S3Authorization`, no credential is involved: the host
/// already authorized access by handing the consumer a directory handle
/// through the FS Access API (or via a native path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsAuthorization {
    request: FsRequest,
}

impl FsAuthorization {
    /// Construct an authorization bound to a captured request.
    pub fn new(request: FsRequest) -> Self {
        Self { request }
    }

    /// The captured request this authorization is bound to.
    pub fn request(&self) -> &FsRequest {
        &self.request
    }

    /// Redeem this authorization for a permit against the given address.
    ///
    /// For FS there is no signing step — this is a passthrough constructor
    /// that pairs the captured request with the registered handle id.
    pub fn redeem(&self, address: &FsAddress) -> FsPermit {
        FsPermit::new(address.id().to_string(), self.request.clone())
    }
}
