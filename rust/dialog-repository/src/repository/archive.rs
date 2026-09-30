//! Loading a branch's tree through archive capabilities.
//!
//! - [`local`] -- loads nodes and spilled values from the local archive
//! - [`networked`] -- falls back to a remote site on a local miss

/// Loads nodes and spilled values from the local archive.
pub mod local;
pub use local::*;

/// Loads nodes and spilled values locally, falling back to a remote site
/// and caching locally on a read miss.
pub mod networked;
pub use networked::*;
