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
//! // Build a capability to get content from the "index" catalog.
//! // The verb comes from the effect and lands above the namespace:
//! // this reads `/use/get/archive/block`.
//! let digest = Blake3Hash::hash(b"hello");
//! let get_capability = Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
//!     .archive()             // Namespace: archive operations
//!     .catalog("index")      // Policy: only the "index" catalog
//!     .get(digest);          // Effect: get this specific digest
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
    pub use crate::archive::prelude::*;
    pub use crate::blob::prelude::*;
    pub use crate::branch::prelude::*;
    pub use crate::credential::prelude::*;
    pub use crate::memory::prelude::*;
}

// Re-export capability primitives for convenience
pub use dialog_capability::{Attenuation, Capability, Effect, Policy, Subject};
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

/// What a namespace hangs from: a verb, or anything else that can carry
/// one.
///
/// A namespace such as [`memory`](crate::memory::Memory) is reached by
/// reading, by writing and by deleting, so it is generic over the verb
/// above it. This alias is the bound that generic parameter needs,
/// named once so the four namespaces do not each restate it.
pub trait Verb: Attenuation + dialog_capability::Caveat
where
    Self::Of: dialog_capability::Constraint,
{
}

impl<T> Verb for T
where
    T: Attenuation + dialog_capability::Caveat,
    T::Of: dialog_capability::Constraint,
{
}

/// Attaches the root and verb a namespace hangs from.
///
/// One impl per verb, so which root a chain gets (`/use` for the
/// ordinary verbs, `/void` for the destroying one) follows from the verb
/// itself rather than from anything a caller writes. The deferred
/// builders in each namespace's prelude call this once the effect --
/// and therefore the verb -- is known.
pub trait AttenuateVerb<V: dialog_capability::Constraint> {
    /// Attach the root, then `V`.
    fn verb(self) -> Capability<V>;
}

impl AttenuateVerb<verb::Get> for Subject {
    fn verb(self) -> Capability<verb::Get> {
        self.attenuate(Use).attenuate(verb::Get)
    }
}

impl AttenuateVerb<verb::Put> for Subject {
    fn verb(self) -> Capability<verb::Put> {
        self.attenuate(Use).attenuate(verb::Put)
    }
}

impl AttenuateVerb<verb::Delete> for Subject {
    fn verb(self) -> Capability<verb::Delete> {
        self.attenuate(Use).attenuate(verb::Delete)
    }
}

impl AttenuateVerb<destroy::Delete> for Subject {
    fn verb(self) -> Capability<destroy::Delete> {
        self.attenuate(Void).attenuate(destroy::Delete)
    }
}

/// The verbs a holder exercises on a subject's data, under [`Use`].
///
/// A verb is a level of the hierarchy, not a prefix an effect spells
/// out for itself. That is what makes `/use/get` a real thing to
/// delegate -- every read of a subject's data and nothing else --
/// rather than a convention each effect's path has to agree to.
pub mod verb {
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
    /// Distinct from [`destroy::Delete`](super::destroy::Delete), which
    /// destroys the container itself. Emptying a cell is an ordinary
    /// write; discarding the branch that cell belongs to is not.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
    pub struct Delete;

    impl Attenuation for Delete {
        type Of = Use;
    }
}

/// The verbs that destroy rather than change, under [`Void`].
///
/// Its own module so the type is named for the path segment it
/// contributes -- `delete` -- rather than given a different word to
/// keep it distinct from [`verb::Delete`]. The two are told apart by
/// the root they hang from, which is the distinction that matters.
pub mod destroy {
    use super::{Attenuation, Deserialize, Serialize, Void};

    /// Destroying the thing itself: `/void/delete/...`.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
    pub struct Delete;

    impl Attenuation for Delete {
        type Of = Void;
    }
}
