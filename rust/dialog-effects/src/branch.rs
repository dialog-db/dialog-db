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
//!   │     ├── Get → Branches → List → Result<Vec<BranchRecord>, BranchError>
//!   │     └── Put → Branches
//!   │                 ├── Branch { name } → Create → Result<(), BranchError>
//!   │                 └── Switch { branch } → Result<(), BranchError>
//!   └── Void
//!         └── Discard (spelled `delete`)
//!               └── Branches
//!                     └── Branch { name: String }
//!                           └── Delete → Effect → Result<(), BranchError>
//! ```

use crate::Rejection;
use crate::method;
use crate::{Method, Void};
use dialog_capability::access::AuthorizeError;
use dialog_capability::identity::{Entity, Revision};
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
    type Output = Result<Vec<BranchRecord>, BranchError>;
}

/// A branch as the registry records it: the fields of the repository's
/// `Branch` concept, which converts to and from this.
///
/// A plain record rather than the concept itself because the concept is
/// declared above this crate, where the query layer lives.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchRecord {
    /// The branch entity, derived from `(replica, name)`.
    pub this: Entity,
    /// The branch name on its replica.
    pub name: String,
    /// The replica the branch lives on.
    pub replica: Entity,
}

/// Create a branch and record it in the `meta` branch.
///
/// A branch points at a revision the way a git ref points at a commit:
/// `revision` is stored in the branch's cell as it is, so any revision
/// can be pointed at, including one minted on another branch. The
/// branch's own first commit then mints onto it, scoped to this
/// branch, with the pointed-at revision as its parent.
///
/// `None` creates an empty branch: recorded, with no revision yet.
///
/// Creating a branch that already points at this revision converges
/// rather than conflicting. Creating over a branch that points
/// elsewhere is refused -- a create never moves an existing branch.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Attenuate)]
pub struct Create {
    /// The revision the new branch points at, or `None` for an empty
    /// branch. Omitted from the parameters when absent, so an empty
    /// create reads on the wire exactly as it always has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<Revision>,
}

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
/// Deleting names the revision the branch is expected to point at, and
/// is refused unless it points at exactly that one. A branch that moved
/// since the caller last looked -- a commit landed, a pull advanced it
/// -- is not the branch the caller decided to delete, so it is left
/// alone rather than destroyed along with work the caller never saw.
///
/// The two halves cannot be one compare-and-swap, so the cells go first
/// and the fact follows. A failure part-way therefore leaves a branch
/// that is gone but still listed, rather than one that lists and is
/// still there.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Delete {
    /// The revision the branch must point at for the delete to proceed.
    pub revision: Revision,
}

impl Policy for Delete {
    type Of = Branch<Void>;
}

impl Effect for Delete {
    type Output = Result<(), BranchError>;
}

/// Switch the replica to a branch: record it as the active one.
///
/// Takes the branch's entity rather than a name because the branch
/// need not be on this replica. It is pointed at, not looked up, so
/// switching to a branch that exists elsewhere, or not yet anywhere,
/// is not refused.
///
/// Its command extends [`Create`]'s (`/use/put/dialog/branch/switch`),
/// so a holder of `/use/put/dialog/branch` can switch too, while a
/// delegation of just the switch hands over nothing else.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Switch {
    /// The entity of the branch to switch to.
    pub branch: Entity,
}

impl Attenuation for Switch {
    type Of = Branches<method::Put>;

    fn attenuation() -> &'static str {
        "branch/switch"
    }
}

impl Effect for Switch {
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
    use dialog_capability::identity::{Entity, Revision, TreeReference};
    use dialog_capability::{Subject, did};

    /// A revision to point at or to expect. Its content is irrelevant to
    /// these tests, which are about paths and parameters.
    fn head() -> Revision {
        Revision::new(
            TreeReference::default(),
            "did:key:zMain".parse::<Entity>().expect("valid entity"),
            did!("key:zIssuer"),
        )
    }

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    fn it_builds_list_claim_path() {
        let claim = Subject::from(did!("key:zRepo")).reader().branches().list();

        assert_eq!(claim.ability(), "/use/get/dialog/branch");
    }

    #[dialog_common::test]
    fn it_builds_create_claim_path() {
        let claim = Subject::from(did!("key:zRepo"))
            .writer()
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
            .voider()
            .branches()
            .branch("main")
            .delete(head());

        assert_eq!(claim.ability(), "/void/dialog/branch");
    }

    /// A grant of everything under `/use` never reaches deletion, which
    /// is the whole reason `/void` is a separate root.
    #[dialog_common::test]
    fn it_keeps_deletion_out_of_the_use_root() {
        let subject = Subject::from(did!("key:zRepo"));
        let write = subject.clone().writer().branches().branch("main").create();
        let destroy = subject.voider().branches().branch("main").delete(head());

        assert!(write.ability().starts_with("/use/"));
        assert!(destroy.ability().starts_with("/void/"));
    }

    /// Switching extends the create command, so a grant to write
    /// branches covers it by prefix.
    #[dialog_common::test]
    fn it_builds_switch_claim_path() {
        let claim = Subject::from(did!("key:zRepo"))
            .writer()
            .branches()
            .switch("did:key:zFeature".parse::<Entity>().expect("valid entity"));

        assert_eq!(claim.ability(), "/use/put/dialog/branch/switch");
    }

    /// The branch name scopes the capability without changing the
    /// ability path: two branches differ in what they authorize, not in
    /// how the command reads.
    #[dialog_common::test]
    fn it_scopes_by_name_without_changing_the_path() {
        let subject = Subject::from(did!("key:zRepo"));
        let main = subject
            .clone()
            .voider()
            .branches()
            .branch("main")
            .delete(head());
        let feature = subject.voider().branches().branch("feature").delete(head());

        assert_eq!(main.ability(), feature.ability());
        assert_ne!(main.name(), feature.name());
    }
}
