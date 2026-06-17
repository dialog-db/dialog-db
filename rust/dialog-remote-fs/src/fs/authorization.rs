//! FS authorization material.
//!
//! FS-remote has no credentials: the host already authorized access by handing
//! the consumer a directory handle through the File System Access API (or via a
//! native path). Authorization is therefore a unit marker, kept for structural
//! parity with credential-based sites like [`dialog_remote_s3::S3Authorization`].

use serde::{Deserialize, Serialize};

/// FS authorization material — a unit marker.
///
/// Present only to satisfy the [`Site::Authorization`](dialog_capability::Site)
/// contract. The [`provider`](crate::fs::provider) resolves the
/// [`FsAddress`](crate::FsAddress) directly and delegates to the registered
/// directory, so no authorization material is needed.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct FsAuthorization;
