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
//! subject.get().branches().list()          = /use/get/dialog/branch
//! subject.put().branches().branch(n).create()
//!                                          = /use/put/dialog/branch
//! subject.delete().branches().branch(n).delete()
//!                                          = /void/delete/dialog/branch
//! ```

use dialog_capability::{Capability, Constraint, Policy};

use super::{Branch, Branches, Create, Delete, List};
use crate::{Method, method};

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

/// Create a branch.
pub trait CreateBranchExt {
    /// Create it, recording the branch in the registry.
    fn create(self) -> Capability<Create>;
}

impl CreateBranchExt for Capability<Branch<method::Put>> {
    fn create(self) -> Capability<Create> {
        self.invoke(Create)
    }
}

/// Delete a branch.
pub trait DeleteBranchExt {
    /// Delete it: retract its facts and its memory cells.
    fn delete(self) -> Capability<Delete>;
}

impl DeleteBranchExt for Capability<Branch<method::Void>> {
    fn delete(self) -> Capability<Delete> {
        self.invoke(Delete)
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
