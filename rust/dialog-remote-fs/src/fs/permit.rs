//! Pre-authorized FS operation handle.
//!
//! Mirrors [`dialog_remote_s3::Permit`]. Where the S3 permit carries a
//! presigned URL + method + headers, the FS permit carries the registered
//! handle id and the captured request — everything a provider needs to
//! navigate the directory and perform the operation.

use super::FsInvocation;
use crate::request::FsRequest;
use dialog_capability::{Capability, Constraint, Effect};
use serde::{Deserialize, Serialize};

/// A pre-authorized FS operation — handle identifier + captured request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsPermit {
    /// Identifier looked up in the provider's handle registry to obtain
    /// the actual directory handle / root path.
    pub handle_id: String,
    /// The captured request (operation, path, precondition).
    pub request: FsRequest,
}

impl FsPermit {
    /// Construct a permit.
    pub fn new(handle_id: String, request: FsRequest) -> Self {
        Self { handle_id, request }
    }

    /// Pair this permit with a capability to produce an [`FsInvocation`]
    /// ready for execution against an [`Fs`](super::Fs) provider.
    pub fn invoke<Fx: Effect>(self, capability: Capability<Fx>) -> FsInvocation<Fx>
    where
        Fx::Of: Constraint,
    {
        FsInvocation::new(self, capability)
    }
}
