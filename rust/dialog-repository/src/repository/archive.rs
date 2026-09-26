//! Archive capabilities and CAS adapters.
//!
//! - [`local`] -- local CAS adapter for search tree storage
//! - [`networked`] -- networked CAS adapter falling back to a remote site

/// Local CAS adapter bridging capabilities with search tree's ContentAddressedStorage.
pub mod local;
pub use local::*;

/// CAS adapter that falls back to a remote site and caches locally on read miss.
pub mod networked;
pub use networked::*;
