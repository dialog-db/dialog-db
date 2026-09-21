//! Extension traits for fluent branch capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::branch::prelude::*;
//! ```

use crate::destroy;
use crate::verb;
use dialog_capability::{Capability, Did, Policy, Subject};

use super::{Branch, Branches, Create, Delete, List};
use crate::AttenuateVerb;

/// Extension trait to start a branch capability chain.
pub trait BranchSubjectExt {
    /// The resulting branches chain type.
    type Branches;
    /// Begin a branch capability chain.
    fn branches(self) -> Self::Branches;
}

impl BranchSubjectExt for Subject {
    type Branches = BranchesScope;
    fn branches(self) -> BranchesScope {
        BranchesScope { subject: self }
    }
}

impl BranchSubjectExt for Did {
    type Branches = BranchesScope;
    fn branches(self) -> BranchesScope {
        BranchesScope {
            subject: Subject::from(self),
        }
    }
}

/// A branch chain that has not chosen its verb yet.
///
/// See the note in [`memory::prelude`](crate::memory::prelude) for why
/// the builder defers.
#[derive(Debug, Clone)]
pub struct BranchesScope {
    subject: Subject,
}

impl BranchesScope {
    /// Build the namespace chain under `V`.
    fn under<V>(self) -> Capability<Branches<V>>
    where
        V: crate::Verb,
        V::Of: dialog_capability::Constraint,
        Subject: AttenuateVerb<V>,
    {
        AttenuateVerb::verb(self.subject).attenuate(Branches::<V>::new())
    }
}

/// Extension methods for scoping to a named branch, and for asking
/// about the set of branches as a whole.
pub trait BranchesExt {
    /// The resulting branch chain type.
    type Branch;
    /// The resulting list chain type.
    type List;
    /// Scope to the branch with this name.
    fn branch(self, name: impl Into<String>) -> Self::Branch;
    /// List the branches on this replica.
    fn list(self) -> Self::List;
}

impl BranchesExt for BranchesScope {
    type Branch = BranchScope;
    type List = Capability<List>;

    fn branch(self, name: impl Into<String>) -> BranchScope {
        BranchScope {
            subject: self.subject,
            name: name.into(),
        }
    }

    fn list(self) -> Capability<List> {
        self.under::<verb::Get>().invoke(List)
    }
}

/// A named-branch chain that has not chosen its verb yet.
#[derive(Debug, Clone)]
pub struct BranchScope {
    subject: Subject,
    name: String,
}

impl BranchScope {
    /// Build the branch chain under `V`.
    fn under<V>(self) -> Capability<Branch<V>>
    where
        V: crate::Verb,
        V::Of: dialog_capability::Constraint,
        Subject: AttenuateVerb<V>,
    {
        AttenuateVerb::verb(self.subject)
            .attenuate(Branches::<V>::new())
            .attenuate(Branch::<V>::new(self.name))
    }
}

/// Extension methods for invoking effects on a named branch.
pub trait BranchExt {
    /// The resulting create chain type.
    type Create;
    /// The resulting delete chain type.
    type Delete;
    /// Create the branch and record it in the `meta` branch.
    fn create(self) -> Self::Create;
    /// Delete the branch: retract its facts and its memory cells.
    ///
    /// Rooted at `/void`, not `/use` — the verb decides the root, so a
    /// caller asking to destroy a branch gets the destroying chain
    /// without having to know that is where it lives.
    fn delete(self) -> Self::Delete;
}

impl BranchExt for BranchScope {
    type Create = Capability<Create>;
    type Delete = Capability<Delete>;

    fn create(self) -> Capability<Create> {
        self.under::<verb::Put>().invoke(Create)
    }

    fn delete(self) -> Capability<Delete> {
        self.under::<destroy::Delete>().invoke(Delete)
    }
}

/// Field accessors on a capability scoped to a branch.
pub trait BranchNameExt {
    /// Get the branch name from the capability chain.
    fn name(&self) -> &str;
}

impl BranchNameExt for Capability<Create> {
    fn name(&self) -> &str {
        &Branch::of(self).name
    }
}

impl BranchNameExt for Capability<Delete> {
    fn name(&self) -> &str {
        &Branch::of(self).name
    }
}
