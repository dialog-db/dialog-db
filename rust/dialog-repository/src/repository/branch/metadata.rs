//! Branch metadata concepts.
//!
//! [`Branch::metadata`] turns a branch handle + the operator's
//! identity into a typed [`BranchMetadata`] — the [`Replica`], the
//! [`Branch`](BranchConcept) concept, and a [`BranchRevision`] when
//! the branch has a commit. `BranchMetadata` implements [`Statement`],
//! so it folds straight into a [`Changes`](dialog_artifacts::Changes)
//! batch.
//!
//! The query layer composes these per-branch bundles (plus a
//! [`Session`](crate::schema::Session)) into the overlay every
//! `branch.query()` is evaluated against — see
//! [`QueryLayer::metadata`](super::session::QueryLayer::metadata).

use base58::ToBase58;
use dialog_artifacts::{Statement, Update};
use dialog_capability::Capability;
use dialog_effects::authority::{Operator, OperatorExt as _};

use crate::Branch;
use crate::schema::Branch as BranchConcept;
use crate::schema::branch::{Edition, Revision as RevisionEntity, Tree};
use crate::schema::{BranchRevision, Replica};

/// The schema-shaped metadata for a single branch.
///
/// Bundles the [`Replica`] (`(profile, subject)`-derived), the
/// [`Branch`](BranchConcept) concept (`(replica, name)`-derived), and
/// the [`BranchRevision`] when the branch has a committed revision.
///
/// Implements [`Statement`]: asserting a `BranchMetadata` asserts all
/// three (replica, branch, optional revision) into the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchMetadata {
    /// This device's view of the repository the branch lives in.
    pub replica: Replica,
    /// The branch concept — name + replica, content-derived entity.
    pub branch: BranchConcept,
    /// The current revision, present once the branch has a commit.
    pub revision: Option<BranchRevision>,
}

impl Statement for BranchMetadata {
    fn assert(self, update: &mut impl Update) {
        self.replica.assert(update);
        self.branch.assert(update);
        if let Some(revision) = self.revision {
            revision.assert(update);
        }
    }

    fn retract(self, update: &mut impl Update) {
        self.replica.retract(update);
        self.branch.retract(update);
        if let Some(revision) = self.revision {
            revision.retract(update);
        }
    }
}

impl Branch {
    /// The schema [`BranchMetadata`] for this branch under `operator`'s
    /// identity.
    ///
    /// `operator` supplies the profile DID: [`Replica`] is
    /// `(profile, subject)`-derived — and the [`Branch`](BranchConcept)
    /// and [`BranchRevision`] entities inherit that derivation — but a
    /// branch handle carries only its subject, not a profile. The
    /// `Capability<Operator>` (from
    /// [`Identify`](dialog_effects::authority::Identify)) carries both
    /// the profile and operator DIDs.
    pub fn metadata(&self, operator: &Capability<Operator>) -> BranchMetadata {
        let replica = Replica::new(operator.profile().clone(), self.of().clone());
        let branch = BranchConcept::new(&replica, self.name());
        let revision = self.revision().map(|revision| {
            let tree_bytes: &[u8] = revision.tree.hash();
            BranchRevision {
                this: branch.this.clone(),
                tree: Tree(ToBase58::to_base58(tree_bytes)),
                edition: Edition(u128::from(revision.edition.value())),
                revision: RevisionEntity(revision.entity()),
            }
        });
        BranchMetadata {
            replica,
            branch,
            revision,
        }
    }
}
