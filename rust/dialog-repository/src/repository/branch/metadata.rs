//! Branch metadata layer.
//!
//! Builds a [`VolatileLayer`] populated with facts describing a
//! [`Branch`]: a typed [`crate::schema::Branch`] concept (with
//! content-derived entity), an optional
//! [`crate::schema::TrackingBranch`] for local-upstream tracking,
//! plus the legacy `dialog.meta/*` facts on synthetic
//! [`id:branch`](branch_entity) / [`id:repository`](repository_entity) /
//! [`id:upstream`](upstream_entity) entities.
//!
//! The schema-shaped facts are the recommended query target — they use
//! the [`xyz.tonk.branch/*`](crate::schema::domain::branch) namespace
//! and content-derived entities, so two devices opening the same branch
//! converge on the same entity URIs. The `dialog.meta/*` facts remain
//! for backward compatibility with existing query callers.

use crate::layer::VolatileLayer;
use base58::ToBase58;
use dialog_artifacts::{DialogArtifactsError, Entity};
use dialog_query::the;

use crate::schema::Branch as BranchConcept;
use crate::schema::prelude::*;
use crate::{Branch, Upstream};

/// The conventional synthetic entity for the branch handle itself
/// in the legacy `dialog.meta/*` fact set. New code should query
/// via [`crate::schema::Branch`] instead (which uses a
/// content-derived entity).
pub fn branch_entity() -> Entity {
    "id:branch".parse().expect("valid entity")
}

/// The conventional synthetic entity for the hosting repository in
/// the legacy `dialog.meta/*` fact set.
pub fn repository_entity() -> Entity {
    "id:repository".parse().expect("valid entity")
}

/// The conventional synthetic entity for the tracked upstream in
/// the legacy `dialog.meta/*` fact set. New code should query via
/// [`crate::schema::TrackingBranch`] (for `Upstream::Local`) instead.
pub fn upstream_entity() -> Entity {
    "id:upstream".parse().expect("valid entity")
}

/// Build the metadata layer for `branch`.
///
/// Emits two parallel fact sets:
///
/// - **Schema-shaped** facts using [`crate::schema::Branch`] (with
///   `origin = subject DID's entity`, `name = branch.name()`) and —
///   when the branch has a `Upstream::Local` upstream —
///   [`crate::schema::TrackingBranch`]. These are the recommended
///   query target; the entities are content-derived so multiple
///   replicas converge on the same URIs.
///
/// - **Legacy `dialog.meta/*`** facts on the synthetic
///   [`branch_entity`] / [`repository_entity`] / [`upstream_entity`]
///   URIs. Kept for compatibility with existing callers; new code
///   should prefer the schema concepts.
///
/// Async because the underlying [`VolatileLayer`] requires an awaited
/// [`VolatileTransaction::commit`](crate::layer::VolatileTransaction::commit)
/// to materialize the facts into its prolly tree.
pub async fn branch_metadata(branch: &Branch) -> Result<VolatileLayer, DialogArtifactsError> {
    let branch_id = branch_entity();
    let repo_id = repository_entity();

    // Schema-shaped branch identity: origin = subject DID's entity.
    // We don't have a profile DID at this layer (the Branch handle
    // only carries subject + name), so subject stands in for the
    // replica's origin. Two replicas of the same repository
    // therefore produce the same schema::Branch entity, which is
    // the right behavior for "same logical branch" queries.
    let subject_entity: Entity = branch.of().this();
    let branch_concept = BranchConcept::new(&subject_entity, branch.name());

    let layer = VolatileLayer::new();
    let mut tx = layer
        .transaction()
        // Schema-shaped fact set.
        .assert(branch_concept.clone())
        // Legacy dialog.meta/* fact set on synthetic entities.
        .assert(
            the!("dialog.meta/name")
                .of(branch_id.clone())
                .is(branch.name().to_string()),
        )
        .assert(
            the!("dialog.meta/repository")
                .of(branch_id.clone())
                .is(repo_id.clone()),
        )
        .assert(
            the!("dialog.meta/did")
                .of(repo_id)
                .is(branch.of().to_string()),
        );

    if let Some(revision) = branch.revision() {
        let hash_bytes: &[u8] = revision.tree.hash();
        let hash_b58 = ToBase58::to_base58(hash_bytes);

        tx = tx
            .assert(
                the!("dialog.meta/revision-hash")
                    .of(branch_id.clone())
                    .is(hash_b58),
            )
            .assert(
                the!("dialog.meta/period")
                    .of(branch_id.clone())
                    .is(revision.period as u128),
            )
            .assert(
                the!("dialog.meta/moment")
                    .of(branch_id.clone())
                    .is(revision.moment as u128),
            )
            .assert(
                the!("dialog.meta/issuer")
                    .of(branch_id.clone())
                    .is(revision.issuer.to_string()),
            )
            .assert(
                the!("dialog.meta/authority")
                    .of(branch_id.clone())
                    .is(revision.authority.to_string()),
            );
    }

    if let Some(upstream) = branch.upstream() {
        let upstream_id = upstream_entity();
        tx = tx.assert(
            the!("dialog.meta/upstream")
                .of(branch_id.clone())
                .is(upstream_id.clone()),
        );

        let kind = match &upstream {
            Upstream::Local { .. } => "local",
            Upstream::Remote { .. } => "remote",
        };
        tx = tx
            .assert(
                the!("dialog.meta/kind")
                    .of(upstream_id.clone())
                    .is(kind.to_string()),
            )
            .assert(
                the!("dialog.meta/branch")
                    .of(upstream_id.clone())
                    .is(upstream.branch().to_string()),
            );

        if let Upstream::Remote { remote, .. } = &upstream {
            tx = tx.assert(
                the!("dialog.meta/remote")
                    .of(upstream_id)
                    .is(remote.clone()),
            );
        }

        // Schema-shaped tracking only works for local upstreams here:
        // a remote upstream would need a schema::Remote entity, which
        // depends on the remote's address — not available from just
        // `&Branch`. Local upstreams share the same origin
        // (subject DID), so the upstream branch entity is derivable.
        if let Upstream::Local { branch: name, .. } = &upstream {
            let upstream_concept = BranchConcept::new(&subject_entity, name.clone());
            tx = tx.assert(branch_concept.set_upstream(&upstream_concept));
        }
    }

    tx.commit().apply().await?;
    Ok(layer)
}
