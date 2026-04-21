//! Archive capabilities and CAS adapters.
//!
//! - [`RepositoryArchiveExt`] -- extension trait adding `.index()` to archive capabilities
//! - [`local`] -- local CAS adapter for prolly tree storage
//! - [`networked`] -- networked CAS adapter falling back to a remote site
use dialog_capability::Capability;
use dialog_effects::archive::prelude::ArchiveExt;
use dialog_effects::archive::{Archive, Catalog};

/// Local CAS adapter bridging capabilities with prolly tree's ContentAddressedStorage.
pub mod local;
pub use local::*;

/// CAS adapter that falls back to a remote site and caches locally on read miss.
pub mod networked;
pub use networked::*;

/// Extension trait for archive capabilities in the repository context.
///
/// Extends the base `ArchiveExt` from `dialog-effects` with repository-specific
/// helpers.
pub trait RepositoryArchiveExt: ArchiveExt {
    /// The index catalog used for search tree node storage.
    fn index(self) -> Capability<Catalog>;
}

impl RepositoryArchiveExt for Capability<Archive> {
    fn index(self) -> Capability<Catalog> {
        self.catalog("index")
    }
}
