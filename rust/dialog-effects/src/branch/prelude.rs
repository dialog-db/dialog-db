//! Extension traits for fluent branch capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::branch::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Policy, Subject};

use super::{Branch, Branches, Create, Delete, Discard, Doomed, List};
use crate::{Use, Void};

/// Extension trait to start a branch capability chain.
///
/// [`branches`](Self::branches) begins the [`Use`] side (reads and
/// writes); [`discard`](Self::discard) begins the [`Void`] side
/// (destruction). They are separate entry points because they are
/// separate powers.
pub trait BranchSubjectExt {
    /// The resulting branches chain type.
    type Branches;
    /// The resulting discard chain type.
    type Discard;
    /// Begin a branch read/write capability chain.
    fn branches(self) -> Self::Branches;
    /// Begin a branch destruction capability chain.
    fn discard(self) -> Self::Discard;
}

impl BranchSubjectExt for Subject {
    type Branches = Capability<Branches>;
    type Discard = Capability<Discard>;

    fn branches(self) -> Capability<Branches> {
        self.attenuate(Use).attenuate(Branches)
    }

    fn discard(self) -> Capability<Discard> {
        self.attenuate(Void).attenuate(Discard)
    }
}

impl BranchSubjectExt for Did {
    type Branches = Capability<Branches>;
    type Discard = Capability<Discard>;

    fn branches(self) -> Capability<Branches> {
        Subject::from(self).attenuate(Use).attenuate(Branches)
    }

    fn discard(self) -> Capability<Discard> {
        Subject::from(self).attenuate(Void).attenuate(Discard)
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

impl BranchesExt for Capability<Branches> {
    type Branch = Capability<Branch>;
    type List = Capability<List>;

    fn branch(self, name: impl Into<String>) -> Capability<Branch> {
        self.attenuate(Branch::new(name))
    }

    fn list(self) -> Capability<List> {
        self.invoke(List)
    }
}

/// Extension methods for scoping a destruction chain to one branch.
pub trait DiscardExt {
    /// The resulting doomed-branch chain type.
    type Branch;
    /// Scope to the branch with this name.
    fn branch(self, name: impl Into<String>) -> Self::Branch;
}

impl DiscardExt for Capability<Discard> {
    type Branch = Capability<Doomed>;

    fn branch(self, name: impl Into<String>) -> Capability<Doomed> {
        self.attenuate(Doomed::new(name))
    }
}

/// Extension methods for invoking effects on a named branch.
pub trait BranchExt {
    /// The resulting create chain type.
    type Create;
    /// Create the branch and record it in the `meta` branch.
    fn create(self) -> Self::Create;
}

impl BranchExt for Capability<Branch> {
    type Create = Capability<Create>;

    fn create(self) -> Capability<Create> {
        self.invoke(Create)
    }
}

/// Extension methods for invoking the destruction effect.
pub trait DoomedExt {
    /// The resulting delete chain type.
    type Delete;
    /// Delete the branch: retract its facts and its memory cells.
    fn delete(self) -> Self::Delete;
}

impl DoomedExt for Capability<Doomed> {
    type Delete = Capability<Delete>;

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
        &Doomed::of(self).name
    }
}
