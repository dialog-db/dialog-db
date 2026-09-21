//! Branch capability hierarchy.
//!
//! A branch is a Dialog concept, not a storage one: it is a named line
//! of revisions on a replica, described by the `dialog.branch/*` facts
//! recorded in the repository's `meta` branch. These effects name that
//! vocabulary — `/use/get/dialog/branch` rather than anything phrased
//! in terms of the cells a branch happens to be stored in.
//!
//! Naming the domain operation rather than its storage shape is what
//! makes the capability delegable in the terms a holder cares about: a
//! delegation of `/use/get/dialog/branch` lets its holder enumerate
//! branches, and nothing else. A cell-enumeration capability would have
//! handed over the ability to read the contents of every space.
//!
//! # Capability Hierarchy
//!
//! Reads and writes live under [`Use`]; destroying a branch lives under
//! [`Void`], so a member holding `/use` can commit to a branch and
//! retract its facts without being able to delete the branch itself.
//!
//! ```text
//! Subject (repository DID)
//!   ├── Use
//!   │     ├── Get → Branches → List → Result<Vec<String>, BranchError>
//!   │     └── Put → Branches → Branch { name } → Create → Result<(), BranchError>
//!   └── Void
//!         └── Discard (spelled `delete`)
//!               └── Branches
//!                     └── Branch { name: String }
//!                           └── Delete → Effect → Result<(), BranchError>
//! ```

use crate::Method;
use crate::Rejection;
use crate::method;
use dialog_capability::access::AuthorizeError;
use dialog_capability::{Attenuate, Attenuation, Constraint, Effect, Policy};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use thiserror::Error;

use crate::memory::MemoryError;

/// Root policy for branch reads and writes.
///
/// Attaches under [`Use`] and contributes no ability segment of its
/// own: the effects name the whole command
/// (`get/dialog/branch` — verb, then namespace, then resource,
/// as in `get/memory/cell`), as they do in [`memory`](crate::memory).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Branches<V = method::Get>(#[serde(skip)] PhantomData<V>);

impl<V> Branches<V> {
    /// The dialog namespace under `V`.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<V> Default for Branches<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: crate::Method> Attenuation for Branches<V>
where
    V::Of: dialog_capability::Constraint,
{
    type Of = V;

    fn attenuation() -> &'static str {
        "dialog"
    }
}

/// The branch resource, scoped to one name.
///
/// Contributes `branch` to the ability path, completing the command;
/// the branch *name* scopes the capability and travels in the
/// invocation's parameters, as a cell's name does.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Branch<V = method::Get> {
    /// The branch name, as it appears in `dialog.branch/name`.
    pub name: String,
    /// The verb this policy hangs from. A type-level marker: it holds
    /// no data and never reaches the wire.
    #[serde(skip)]
    pub verb: PhantomData<V>,
}

impl<V> Branch<V> {
    /// Scope to the branch with this name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            verb: PhantomData,
        }
    }
}

impl<M: Method> Attenuation for Branch<M>
where
    M::Of: Constraint,
{
    type Of = Branches<M>;

    fn attenuation() -> &'static str {
        "branch"
    }
}

/// List the branches on this replica.
///
/// Answered from the `meta` branch's `dialog.branch/*` facts, so it
/// reports the branches that were recorded — not whatever cells happen
/// to exist. `meta` itself is included: its fact is synthesized into
/// the query overlay rather than stored, which is what keeps a branch
/// registry from having to record itself.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
pub struct List;

// Listing names the resource itself: it asks about the branches as a
// set, so there is no `Branch` link above it to have named one.
impl Attenuation for List {
    type Of = Branches<method::Get>;

    fn attenuation() -> &'static str {
        "branch"
    }
}

impl Effect for List {
    type Output = Result<Vec<String>, BranchError>;
}

/// Create a branch and record it in the `meta` branch.
///
/// Creating a branch that already exists is not an error: the recorded
/// fact is the same one, so a repeated create converges rather than
/// conflicting.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
pub struct Create;

impl Policy for Create {
    type Of = Branch<method::Put>;
}

impl Effect for Create {
    type Output = Result<(), BranchError>;
}

/// Delete a branch: retract its `dialog.branch/*` facts and retract the
/// memory cells holding its state.
///
/// Lives under [`Void`], not [`Use`]: holding every read and write of a
/// subject's data is not the same as being able to destroy the thing
/// that holds it.
///
/// The two halves cannot be one compare-and-swap, so the fact is
/// authoritative and is retracted last. A failure part-way therefore
/// leaves cells a later [`Create`] overwrites harmlessly, rather than a
/// branch that lists but cannot be opened.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
pub struct Delete;

impl Policy for Delete {
    type Of = Branch<method::Void>;
}

impl Effect for Delete {
    type Output = Result<(), BranchError>;
}

pub mod prelude;

/// Errors that can occur during branch operations.
#[derive(Debug, Error)]
pub enum BranchError {
    /// The branch does not exist.
    #[error("No such branch: {name}")]
    NotFound {
        /// The branch name that was not found.
        name: String,
    },

    /// The operation is not allowed on this branch.
    ///
    /// `meta` is the registry every other branch is recorded in, so it
    /// is not itself something to create or delete.
    #[error("Branch {name} cannot be {operation}: {reason}")]
    Refused {
        /// The branch name.
        name: String,
        /// The operation that was refused.
        operation: &'static str,
        /// Why it was refused.
        reason: &'static str,
    },

    /// A memory cell operation failed.
    #[error(transparent)]
    Memory(#[from] MemoryError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use dialog_capability::{Subject, did};

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    fn it_builds_list_claim_path() {
        let claim = Subject::from(did!("key:zRepo")).get().branches().list();

        assert_eq!(claim.ability(), "/use/get/dialog/branch");
    }

    #[dialog_common::test]
    fn it_builds_create_claim_path() {
        let claim = Subject::from(did!("key:zRepo"))
            .put()
            .branches()
            .branch("main")
            .create();

        assert_eq!(claim.ability(), "/use/put/dialog/branch");
    }

    /// Deletion is rooted at `/void`, not `/use`: destroying a branch
    /// is a different power from reading and writing one, and the
    /// ability path is where that difference is enforced.
    #[dialog_common::test]
    fn it_builds_delete_claim_path() {
        let claim = Subject::from(did!("key:zRepo"))
            .discard()
            .branches()
            .branch("main")
            .delete();

        assert_eq!(claim.ability(), "/void/delete/dialog/branch");
    }

    /// A grant of everything under `/use` never reaches deletion, which
    /// is the whole reason `/void` is a separate root.
    #[dialog_common::test]
    fn it_keeps_deletion_out_of_the_use_root() {
        let subject = Subject::from(did!("key:zRepo"));
        let write = subject.clone().put().branches().branch("main").create();
        let destroy = subject.discard().branches().branch("main").delete();

        assert!(write.ability().starts_with("/use/"));
        assert!(destroy.ability().starts_with("/void/"));
    }

    /// The branch name scopes the capability without changing the
    /// ability path: two branches differ in what they authorize, not in
    /// how the command reads.
    #[dialog_common::test]
    fn it_scopes_by_name_without_changing_the_path() {
        let subject = Subject::from(did!("key:zRepo"));
        let main = subject.clone().discard().branches().branch("main").delete();
        let feature = subject.discard().branches().branch("feature").delete();

        assert_eq!(main.ability(), feature.ability());
        assert_ne!(main.name(), feature.name());
    }
}
