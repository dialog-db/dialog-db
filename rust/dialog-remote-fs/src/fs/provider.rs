//! Provider implementations for the [`Fs`](super::Fs) site.
//!
//! Each module provides two layers, mirroring `dialog_remote_s3`:
//! - `Provider<ForkInvocation<Fs, Fx>>` — redeems authorization (no-op for
//!   FS) and dispatches into the per-invocation execution layer
//! - `Provider<FsInvocation<Fx>>` — actual I/O against the registered
//!   directory handle (currently native; WASM lands in a follow-up).

pub mod archive;
pub mod memory;

use crate::FsError;
use crate::handle::{FsHandle, Handle};
use crate::registry;

/// Split a request path into `(parent_handle, file_name)`. Used by Put,
/// Publish, and Retract — all of which need the parent separately to
/// `ensure_dir` before writing the temp file, then rename into place.
pub(crate) async fn split_target<'p>(
    handle_id: &str,
    path: &'p [String],
) -> Result<(Handle, &'p str), FsError> {
    let (file_name, parent_segments) = path.split_last().ok_or_else(|| {
        FsError::Io("empty request path — translation produced no segments".into())
    })?;
    let mut parent = registry::lookup(handle_id)?;
    for segment in parent_segments {
        parent = parent.resolve(segment).await?;
    }
    Ok((parent, file_name.as_str()))
}

/// Navigate the request's path under the registered handle and return the
/// resolved leaf handle. Used by Get and Resolve where we don't need the
/// parent separately.
pub(crate) async fn navigate(handle_id: &str, path: &[String]) -> Result<Handle, FsError> {
    let mut current = registry::lookup(handle_id)?;
    for segment in path {
        current = current.resolve(segment).await?;
    }
    Ok(current)
}
