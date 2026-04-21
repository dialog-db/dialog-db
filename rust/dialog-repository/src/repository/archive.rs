//! Archive capabilities and CAS adapters.
//!
//! - [`RepositoryArchiveExt`] -- extension trait adding `.index()` to archive capabilities
//! - [`local`] -- local CAS adapter for search tree storage
use dialog_capability::Capability;
use dialog_effects::archive::prelude::ArchiveExt;
use dialog_effects::archive::{Archive, Catalog};

/// Local CAS adapter bridging capabilities with search tree's ContentAddressedStorage.
pub mod local;
pub use local::*;

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
