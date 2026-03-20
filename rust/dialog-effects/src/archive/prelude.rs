//! Extension traits for fluent archive capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::archive::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Subject, site::Site};
use dialog_common::Blake3Hash;

use super::{Archive, Catalog, Get, Put};

/// Extension trait to start an archive capability chain.
pub trait SubjectExt {
    /// The resulting archive chain type.
    type Archive;
    /// Begin an archive capability chain.
    fn archive(self) -> Self::Archive;
}

impl SubjectExt for Subject {
    type Archive = Capability<Archive>;
    fn archive(self) -> Capability<Archive> {
        self.attenuate(Archive)
    }
}

impl SubjectExt for Did {
    type Archive = Capability<Archive>;
    fn archive(self) -> Capability<Archive> {
        Subject::from(self).attenuate(Archive)
    }
}

impl<S: Site> SubjectExt for Capability<Subject, S> {
    type Archive = Capability<Archive, S>;
    fn archive(self) -> Capability<Archive, S> {
        self.attenuate(Archive)
    }
}

/// Extension methods for scoping archive to a named catalog.
pub trait ArchiveExt {
    /// The resulting catalog chain type.
    type Catalog;
    /// Scope to a named catalog.
    fn catalog(self, name: impl Into<String>) -> Self::Catalog;
}

impl<S: Site> ArchiveExt for Capability<Archive, S> {
    type Catalog = Capability<Catalog, S>;
    fn catalog(self, name: impl Into<String>) -> Capability<Catalog, S> {
        self.attenuate(Catalog::new(name))
    }
}

/// Extension methods for invoking effects on a catalog.
pub trait CatalogExt {
    /// The resulting get chain type.
    type Get;
    /// The resulting put chain type.
    type Put;
    /// Get content by digest.
    fn get(self, digest: impl Into<Blake3Hash>) -> Self::Get;
    /// Put content by digest.
    fn put(self, digest: impl Into<Blake3Hash>, content: impl Into<Vec<u8>>) -> Self::Put;
}

impl<S: Site> CatalogExt for Capability<Catalog, S> {
    type Get = Capability<Get, S>;
    type Put = Capability<Put, S>;

    fn get(self, digest: impl Into<Blake3Hash>) -> Capability<Get, S> {
        self.invoke(Get::new(digest))
    }

    fn put(self, digest: impl Into<Blake3Hash>, content: impl Into<Vec<u8>>) -> Capability<Put, S> {
        self.invoke(Put::new(digest, content))
    }
}
