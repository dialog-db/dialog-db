//! Extension traits for fluent branch capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::branch::prelude::*;
//! ```
//!
//! A chain is written in the order the path reads:
//!
//! ```text
//! subject.reader().branches().list()          = /use/get/dialog/branch
//! subject.writer().branches().branch(n).create()
//!                                          = /use/put/dialog/branch
//! subject.writer().branches().switch(b)   = /use/put/dialog/branch/switch
//! subject.voider().branches().branch(n).delete()
//!                                          = /void/dialog/branch
//! ```

use dialog_capability::identity::{Entity, Revision};
use dialog_capability::{Capability, Constrained, Constraint, Policy};

use super::{Branch, Branches, Create, Delete, List, Switch};
use crate::{Method, Void, method};

/// Scope a method to the branch namespace.
pub trait BranchesExt {
    /// The resulting branches chain type.
    type Branches;
    /// Scope to the branches of this subject.
    fn branches(self) -> Self::Branches;
}

impl<M: Method> BranchesExt for Capability<M>
where
    M::Of: Constraint,
{
    type Branches = Capability<Branches<M>>;
    fn branches(self) -> Self::Branches {
        self.attenuate(Branches::new())
    }
}

/// Scope the branches to one name.
pub trait BranchExt {
    /// The resulting branch chain type.
    type Branch;
    /// Scope to the branch with this name.
    fn branch(self, name: impl Into<String>) -> Self::Branch;
}

impl<M: Method> BranchExt for Capability<Branches<M>>
where
    M::Of: Constraint,
{
    type Branch = Capability<Branch<M>>;
    fn branch(self, name: impl Into<String>) -> Self::Branch {
        self.attenuate(Branch::new(name))
    }
}

/// List the branches on this replica.
pub trait ListBranchesExt {
    /// List them.
    fn list(self) -> Capability<List>;
}

impl ListBranchesExt for Capability<Branches<method::Get>> {
    fn list(self) -> Capability<List> {
        self.invoke(List)
    }
}

/// Switch the replica to a branch.
pub trait SwitchBranchExt {
    /// Make the branch with this entity the replica's active one.
    fn switch(self, branch: Entity) -> Capability<Switch>;
}

impl SwitchBranchExt for Capability<Branches<method::Put>> {
    fn switch(self, branch: Entity) -> Capability<Switch> {
        self.invoke(Switch { branch })
    }
}

/// Create a branch.
pub trait CreateBranchExt {
    /// Create it empty, recording the branch in the registry. Refine
    /// with [`revision`](CreateRevisionExt::revision) to have it point
    /// at a revision instead.
    fn create(self) -> Capability<Create>;
}

impl CreateBranchExt for Capability<Branch<method::Put>> {
    fn create(self) -> Capability<Create> {
        self.invoke(Create::default())
    }
}

/// Point a branch being created at a revision.
pub trait CreateRevisionExt {
    /// Have the new branch point at `revision`.
    fn revision(self, revision: Revision) -> Capability<Create>;
}

impl CreateRevisionExt for Capability<Create> {
    fn revision(self, revision: Revision) -> Capability<Create> {
        let Constrained { capability, .. } = self.into_inner();
        Capability::new(Constrained {
            constraint: Create {
                revision: Some(revision),
            },
            capability,
        })
    }
}

/// Delete a branch.
pub trait DeleteBranchExt {
    /// Delete it, provided it still points at `revision` (or at nothing,
    /// for `None`): retract its memory cells and its facts.
    fn delete(self, revision: impl Into<Option<Revision>>) -> Capability<Delete>;
}

impl DeleteBranchExt for Capability<Branch<Void>> {
    fn delete(self, revision: impl Into<Option<Revision>>) -> Capability<Delete> {
        self.invoke(Delete {
            revision: revision.into(),
        })
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
