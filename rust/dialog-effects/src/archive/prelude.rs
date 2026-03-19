//! Extension traits for fluent archive capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::archive::prelude::*;
//! ```

use dialog_capability::{Capability, Claim, Did, Subject};
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

impl<'a, A: ?Sized> SubjectExt for Claim<'a, A, Subject> {
    type Archive = Claim<'a, A, Archive>;
    fn archive(self) -> Claim<'a, A, Archive> {
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

impl ArchiveExt for Capability<Archive> {
    type Catalog = Capability<Catalog>;
    fn catalog(self, name: impl Into<String>) -> Capability<Catalog> {
        self.attenuate(Catalog::new(name))
    }
}

impl<'a, A: ?Sized> ArchiveExt for Claim<'a, A, Archive> {
    type Catalog = Claim<'a, A, Catalog>;
    fn catalog(self, name: impl Into<String>) -> Claim<'a, A, Catalog> {
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

impl CatalogExt for Capability<Catalog> {
    type Get = Capability<Get>;
    type Put = Capability<Put>;

    fn get(self, digest: impl Into<Blake3Hash>) -> Capability<Get> {
        self.invoke(Get::new(digest))
    }

    fn put(self, digest: impl Into<Blake3Hash>, content: impl Into<Vec<u8>>) -> Capability<Put> {
        self.invoke(Put::new(digest, content))
    }
}

impl<'a, A: ?Sized> CatalogExt for Claim<'a, A, Catalog> {
    type Get = Claim<'a, A, Get>;
    type Put = Claim<'a, A, Put>;

    fn get(self, digest: impl Into<Blake3Hash>) -> Claim<'a, A, Get> {
        self.invoke(Get::new(digest))
    }

    fn put(self, digest: impl Into<Blake3Hash>, content: impl Into<Vec<u8>>) -> Claim<'a, A, Put> {
        self.invoke(Put::new(digest, content))
    }
}
