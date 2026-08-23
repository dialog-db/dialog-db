//! Conversion from [`CheckFailed`] (defined in `crate::invocation`) to
//! [`ContainerError`]. A free function rather than a `From` impl because
//! both types live outside this module's defining crate (orphan rule).

use super::ContainerError;
use crate::invocation::CheckFailed;

/// Convert a `CheckFailed` error to a `ContainerError`.
///
/// The decision travels whole. Each of these is a statement about the
/// caller's material — which link, and how it failed — and a caller
/// answering it to its own clients needs that to stay readable rather
/// than arriving as prose it would have to parse.
pub fn check_failed_to_container_error(err: CheckFailed) -> ContainerError {
    ContainerError::Unauthorized(err)
}
