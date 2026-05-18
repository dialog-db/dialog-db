//! Branch metadata layer + command.
//!
//! [`BranchMetadata`] is the command returned by [`Branch::metadata`]: it
//! takes the profile DID (so the schema's `(profile, subject)`-derived
//! [`Replica`] entity is correct) and resolves remote upstream
//! addresses via the env at perform-time, then builds a [`VolatileLayer`]
//! you can layer onto a query session.
//!
//! The layer carries two parallel fact sets:
//!
//! - **Schema-shaped** facts using [`crate::schema::Branch`] (content-
//!   derived entity from the proper replica), and — when the branch
//!   tracks an upstream — [`crate::schema::TrackingBranch`] pointing
//!   at the right upstream branch entity. For `Upstream::Remote`, the
//!   upstream branch's origin is a [`crate::schema::Remote`] built
//!   from the remote's loaded address; if the remote isn't registered
//!   the schema tracking link is silently omitted (the legacy facts
//!   still describe it).
//!
//! - **Legacy `dialog.meta/*`** facts on the synthetic
//!   [`branch_entity`] / [`repository_entity`] / [`upstream_entity`]
//!   URIs. Kept unchanged for callers that haven't migrated to
//!   schema-shaped queries.

use crate::layer::VolatileLayer;
use base58::ToBase58;
use dialog_artifacts::{DialogArtifactsError, Entity};
use dialog_capability::{Did, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::Resolve;
use dialog_query::the;

use crate::RepositoryMemoryExt as _;
use crate::schema::Branch as BranchConcept;
use crate::schema::Remote as RemoteConcept;
use crate::schema::Replica;
use crate::schema::domain::remote::Address as RemoteAddressAttr;
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
/// [`crate::schema::TrackingBranch`] (now also emitted for
/// `Upstream::Remote` provided the remote is registered) instead.
pub fn upstream_entity() -> Entity {
    "id:upstream".parse().expect("valid entity")
}

/// Command for building a branch's metadata [`VolatileLayer`].
///
/// Created by [`Branch::metadata`]. Execute with `.perform(&env)`.
/// The env access is what lets the command resolve remote upstream
/// addresses (so an `Upstream::Remote` can produce a proper
/// [`crate::schema::TrackingBranch`] whose `upstream` entity matches
/// the schema::Remote-rooted branch entity).
pub struct BranchMetadata<'a> {
    branch: &'a Branch,
    profile: Did,
}

impl<'a> BranchMetadata<'a> {
    /// Build a metadata command for `branch`, scoped to `profile`.
    ///
    /// The `profile` DID is what makes the schema-shaped entities
    /// correct: a [`crate::schema::Replica`] is identified by
    /// `(profile, subject)`, and a [`crate::schema::Branch`] inherits
    /// that identity via its `origin`. Two devices holding the same
    /// profile converge on the same branch entity; two profiles
    /// viewing the same repository do not.
    pub fn new(branch: &'a Branch, profile: Did) -> Self {
        Self { branch, profile }
    }

    /// Execute the command, producing a committed [`VolatileLayer`]
    /// with the schema-shaped + legacy fact sets.
    ///
    /// The env is used to load any remote's address cell when the
    /// branch tracks an `Upstream::Remote`. If the remote isn't
    /// registered the schema [`crate::schema::TrackingBranch`] for
    /// it is omitted — the legacy `dialog.meta/upstream` facts still
    /// describe the tracking.
    pub async fn perform<Env>(self, env: &Env) -> Result<VolatileLayer, DialogArtifactsError>
    where
        Env: Provider<Resolve> + ConditionalSync + 'static,
    {
        let Self { branch, profile } = self;

        let subject = branch.of().clone();
        // A transient Replica — never asserted, only used to compute
        // the right origin entity for the Branch concept. The name
        // is empty because `Replica.this` doesn't depend on it (only
        // `(profile, subject)`), and we don't have a meaningful
        // display name available at this layer.
        let replica = Replica::new(profile.clone(), subject.clone(), "");
        let branch_concept = BranchConcept::new(&replica, branch.name());

        let branch_id = branch_entity();
        let repo_id = repository_entity();

        let layer = VolatileLayer::new();
        let mut tx = layer
            .transaction()
            // Schema-shaped branch identity.
            .assert(branch_concept.clone())
            // Legacy synthetic-entity facts.
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

            // Schema-shaped tracking link. The upstream branch's
            // origin depends on the upstream kind:
            //   Local  → same replica as the local branch
            //   Remote → a schema::Remote built from the remote's
            //            loaded RemoteAddress
            match &upstream {
                Upstream::Local { branch: name, .. } => {
                    let upstream_branch = BranchConcept::new(&replica, name.clone());
                    tx = tx.assert(branch_concept.set_upstream(&upstream_branch));
                }
                Upstream::Remote {
                    remote: remote_name,
                    branch: branch_name,
                    ..
                } => {
                    // Load the remote's address to get its subject +
                    // site. If the remote isn't registered we just
                    // skip the schema tracking link — the legacy
                    // `dialog.meta/upstream` facts still describe it.
                    if let Ok(remote_repo) = branch
                        .subject()
                        .remote(remote_name.clone())
                        .load()
                        .perform(env)
                        .await
                    {
                        let address = remote_repo.address();
                        let remote_concept = RemoteConcept::new(
                            &replica,
                            address.subject.clone(),
                            RemoteAddressAttr::encode(&address.address),
                            remote_name.clone(),
                        );
                        let upstream_branch =
                            BranchConcept::new(&remote_concept, branch_name.clone());
                        tx = tx.assert(branch_concept.set_upstream(&upstream_branch));
                    }
                }
            }
        }

        tx.commit().apply().await?;
        Ok(layer)
    }
}
