//! Dialog effects - capability hierarchy types for storage operations.
//!
//! This crate defines the domain-specific capability hierarchies used by Dialog
//! for storage operations. It provides the structural types (attenuations, policies,
//! and effects) that form capability chains.
//!
//! # Capability Domains
//!
//! - [`storage`]: Location-based storage operations (`Storage`, `Location`, `Mount`, `Load`, `Save`)
//! - [`memory`]: CAS memory cells (`Memory`, `Space`, `Cell`, `Resolve`, `Publish`, `Retract`)
//! - [`branch`]: Named lines of revisions (`Branches`, `Branch`, `List`, `Create`, `Delete`)
//! - [`archive`]: Content-addressed archive (`Archive`, `Catalog`, `Get`, `Put`)
//!
//! # Example
//!
//! ```
//! use dialog_effects::prelude::*;
//! use dialog_capability::{did, Subject};
//! use dialog_common::Blake3Hash;
//!
//! // A chain is written in the order its path reads.
//! let digest = Blake3Hash::hash(b"hello");
//! let get_capability = Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
//!     .get()                 // Method: read, under `/use`
//!     .archive()             // Namespace: the archive
//!     .catalog("index")      // Policy: only the "index" catalog
//!     .get(digest);          // Effect: this specific digest
//!
//! assert_eq!(get_capability.ability(), "/use/get/archive/block");
//! ```

#![warn(missing_docs)]
#![warn(clippy::absolute_paths)]
#![warn(clippy::default_trait_access)]
#![warn(clippy::fallible_impl_from)]
#![warn(clippy::panicking_unwrap)]
#![warn(clippy::unused_async)]
#![deny(clippy::partial_pub_fields)]
#![deny(clippy::unnecessary_self_imports)]
#![cfg_attr(not(test), warn(clippy::large_futures))]
#![cfg_attr(not(test), deny(clippy::panic))]

pub mod access;
pub mod archive;
pub mod authority;
pub mod blob;
pub mod branch;
pub mod credential;
pub mod memory;
pub mod rejection;
pub mod space;
pub mod storage;

/// Unified prelude re-exporting all effect prelude traits.
///
/// ```
/// use dialog_effects::prelude::*;
/// ```
pub mod prelude {
    pub use crate::{MethodExt, UseExt, VoidExt};

    pub use crate::archive::prelude::*;
    pub use crate::blob::prelude::*;
    pub use crate::branch::prelude::*;
    pub use crate::credential::prelude::*;
    pub use crate::memory::prelude::*;
}

// Re-export capability primitives for convenience
pub use dialog_capability::{Attenuation, Capability, Did, Effect, Policy, Subject};
pub use rejection::Rejection;
use serde::{Deserialize, Serialize};

/// Everything a holder needs to use a subject's data: every read and
/// every write (`/use/get/...`, `/use/put/...`, `/use/delete/...`). A delegation
/// attenuated to `Use` lets its holder read and write the subject's data
/// without holding `/ucan` (delegation and revocation), which is what a
/// member of a shared space is given. [`Void`] sits beside it for
/// operations that destroy rather than change.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Use;

impl Attenuation for Use {
    type Of = Subject;
}

/// Operations that destroy rather than change (`/void/delete/...`).
///
/// Separate from [`Use`] because retracting a fact and destroying the
/// thing that holds facts are different powers. A member of a shared
/// space holds `/use`, so they may write and retract the subject's
/// data — but deleting the branch itself is not something that grant
/// should carry. Keeping destruction in its own root means it can only
/// ever be conferred deliberately.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Void;

impl Attenuation for Void {
    type Of = Subject;
}

/// What a namespace hangs from.
///
/// A namespace such as [`memory`](crate::memory::Memory) is reached by
/// reading, by writing and by deleting, so it is generic over the
/// method above it. This alias is the bound that generic parameter
/// needs, named once so the four namespaces do not each restate it.
pub trait Method: Attenuation + dialog_capability::Caveat
where
    Self::Of: dialog_capability::Constraint,
{
}

impl<T> Method for T
where
    T: Attenuation + dialog_capability::Caveat,
    T::Of: dialog_capability::Constraint,
{
}

/// Start a capability chain at a method.
///
/// `.r#use().get()` spells the two links out; `.get()` is the same
/// chain in one call, since a read is always under [`Use`]. Both land
/// on `Capability<method::Get>`, which the namespaces hang from.
pub trait MethodExt: Sized {
    /// Everything a holder needs to use the subject's data.
    fn r#use(self) -> Capability<Use>;

    /// Operations that destroy rather than change.
    fn void(self) -> Capability<Void>;

    /// Read, under [`Use`]: `/use/get/...`.
    fn get(self) -> Capability<method::Get> {
        self.r#use().attenuate(method::Get)
    }

    /// Write, under [`Use`]: `/use/put/...`.
    fn put(self) -> Capability<method::Put> {
        self.r#use().attenuate(method::Put)
    }

    /// Empty a value while leaving what held it, under [`Use`]:
    /// `/use/delete/...`.
    fn delete(self) -> Capability<method::Delete> {
        self.r#use().attenuate(method::Delete)
    }

    /// Destroy the thing itself, under [`Void`]: `/void/delete/...`.
    fn discard(self) -> Capability<method::Void> {
        self.void().attenuate(method::Void)
    }
}

impl MethodExt for Subject {
    fn r#use(self) -> Capability<Use> {
        self.attenuate(Use)
    }

    fn void(self) -> Capability<Void> {
        self.attenuate(Void)
    }
}

impl MethodExt for Did {
    fn r#use(self) -> Capability<Use> {
        Subject::from(self).attenuate(Use)
    }

    fn void(self) -> Capability<Void> {
        Subject::from(self).attenuate(Void)
    }
}

/// Start a chain at a method known only as a type.
///
/// For a caller reconstructing a chain from the wire, where the method
/// comes from the effect being matched rather than from anything
/// written in source.
pub trait Chain<M: dialog_capability::Constraint> {
    /// Begin the chain under `M`.
    fn under(self) -> Capability<M>;
}

impl Chain<method::Get> for Subject {
    fn under(self) -> Capability<method::Get> {
        self.get()
    }
}

impl Chain<method::Put> for Subject {
    fn under(self) -> Capability<method::Put> {
        self.put()
    }
}

impl Chain<method::Delete> for Subject {
    fn under(self) -> Capability<method::Delete> {
        self.delete()
    }
}

impl Chain<method::Void> for Subject {
    fn under(self) -> Capability<method::Void> {
        self.discard()
    }
}

/// Attach a method to a root that already exists.
///
/// For a chain written out as `.r#use().get()` rather than `.get()`.
pub trait UseExt {
    /// Read: `/use/get/...`.
    fn get(self) -> Capability<method::Get>;
    /// Write: `/use/put/...`.
    fn put(self) -> Capability<method::Put>;
    /// Empty a value: `/use/delete/...`.
    fn delete(self) -> Capability<method::Delete>;
}

impl UseExt for Capability<Use> {
    fn get(self) -> Capability<method::Get> {
        self.attenuate(method::Get)
    }

    fn put(self) -> Capability<method::Put> {
        self.attenuate(method::Put)
    }

    fn delete(self) -> Capability<method::Delete> {
        self.attenuate(method::Delete)
    }
}

/// Attach the destroying method to [`Void`].
pub trait VoidExt {
    /// Destroy: `/void/delete/...`.
    fn discard(self) -> Capability<method::Void>;
}

impl VoidExt for Capability<Void> {
    fn discard(self) -> Capability<method::Void> {
        self.attenuate(method::Void)
    }
}

/// What a holder does to a subject's data.
///
/// A method is a level of the hierarchy, not a prefix an effect spells
/// out for itself. That is what makes `/use/get` a real thing to
/// delegate -- every read of a subject's data and nothing else --
/// rather than a convention each effect's path has to agree to.
pub mod method {
    use super::{Attenuation, Deserialize, Serialize, Use};

    /// Reading: `/use/get/...`.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
    pub struct Get;

    impl Attenuation for Get {
        type Of = Use;
    }

    /// Writing: `/use/put/...`.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
    pub struct Put;

    impl Attenuation for Put {
        type Of = Use;
    }

    /// Removing a value while leaving what held it: `/use/delete/...`.
    ///
    /// Distinct from [`Void`](self::Void), which destroys the container
    /// itself. Emptying a cell is an ordinary write; discarding the
    /// branch that cell belongs to is not.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
    pub struct Delete;

    impl Attenuation for Delete {
        type Of = Use;
    }

    /// Destroying the thing itself, under [`Void`](super::Void):
    /// `/void/delete/...`.
    ///
    /// Reads as `delete` like its sibling: what it does to the resource
    /// is the same, and the root above it is what says one empties a
    /// value while the other discards what held it.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
    pub struct Void;

    impl Attenuation for Void {
        type Of = super::Void;

        fn attenuation() -> &'static str {
            "delete"
        }
    }
}
