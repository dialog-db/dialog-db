//! Archive capabilities and CAS adapters.
//!
//! - [`ArchiveExt`] -- extension trait adding `.index()` to archive capabilities
//! - [`local`] -- local CAS adapter for prolly tree storage
//! - [`networked`] -- CAS adapter with transparent remote replication

/// Local CAS adapter bridging capabilities with prolly tree's ContentAddressedStorage.
pub mod local;
/// Networked index: local reads with remote fallback and caching.
pub mod networked;

use dialog_capability::Capability;
use dialog_effects::archive as fx;
use dialog_effects::archive::prelude::ArchiveExt as _;

/// Extension trait for archive capabilities in the repository context.
pub trait ArchiveExt {
    /// The index catalog used for search tree node storage.
    fn index(self) -> Capability<fx::Catalog>;
}

impl ArchiveExt for Capability<fx::Archive> {
    fn index(self) -> Capability<fx::Catalog> {
        self.catalog("index")
    }
}
