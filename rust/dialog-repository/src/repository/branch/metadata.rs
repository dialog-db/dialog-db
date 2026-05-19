//! Branch metadata synthesis.
//!
//! Builds a [`VolatileLayer`] of schema-shaped facts describing one
//! branch — [`Origin`], [`Branch`], optional [`BranchRevision`].
//!
//! Called from [`SelectQuery::perform`](super::session::SelectQuery::perform)
//! at the start of every query, once per primary/layered branch. The
//! profile DID comes from [`Identify`](dialog_effects::authority::Identify),
//! not from the caller — so the synthesized facts use the operator's
//! actual identity and `Branch.origin.this` matches what a remote
//! replica with the same profile would compute.
//!
//! These facts live only in the per-query overlay; nothing here writes
//! to the branch's tree, so [`Transaction`](crate::repository::branch::Transaction)
//! has no way to assert or retract them.

use base58::ToBase58;
use dialog_artifacts::DialogArtifactsError;
use dialog_capability::Did;

use crate::Branch;
use crate::layer::VolatileLayer;
use crate::schema::Branch as BranchConcept;
use crate::schema::BranchRevision;
use crate::schema::Origin;
use crate::schema::branch::{Moment, Period, Tree};

/// Build the metadata [`VolatileLayer`] for a single branch under a
/// given profile.
///
/// Asserts:
/// - one [`Origin`] for `(profile, branch.of())`,
/// - one [`Branch`] for `(origin, branch.name())`,
/// - one [`BranchRevision`] iff the branch has a revision.
///
/// Async because the underlying [`VolatileLayer`] needs an awaited
/// transaction commit to materialize the facts. Doesn't take an env
/// — volatile storage is in-process.
pub(crate) async fn synthesize(
    branch: &Branch,
    profile: &Did,
) -> Result<VolatileLayer, DialogArtifactsError> {
    let origin = Origin::new(profile.clone(), branch.of().clone());
    let branch_concept = BranchConcept::new(&origin, branch.name());

    let layer = VolatileLayer::new();
    let mut tx = layer
        .transaction()
        .assert(origin)
        .assert(branch_concept.clone());

    if let Some(revision) = branch.revision() {
        let tree_bytes: &[u8] = revision.tree.hash();
        let tree_b58 = ToBase58::to_base58(tree_bytes);
        tx = tx.assert(BranchRevision {
            this: branch_concept.this.clone(),
            tree: Tree(tree_b58),
            period: Period(revision.period as u128),
            moment: Moment(revision.moment as u128),
        });
    }

    tx.commit().apply().await?;
    Ok(layer)
}
