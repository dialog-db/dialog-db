//! Branch metadata layer.
//!
//! Builds an [`Layer`] populated with synthetic facts describing a
//! [`Branch`]: its name, current revision, hosting repository, and upstream
//! tracking state — all exposed under the `dialog.meta/*` attribute
//! namespace.
//!
//! These facts let callers run normal [`Query`](dialog_query::Query)s against
//! branch state without reaching for branch-specific accessors, mirroring how
//! Datomic exposes its own metadata as queryable triples (`:db/ident` and
//! friends).
//!
//! All synthetic entities live under the `id:` URI scheme:
//! - `id:branch` — the branch handle
//! - `id:repository` — the hosting repository
//! - `id:upstream` — the tracked upstream (when configured)

use base58::ToBase58;
use dialog_artifacts::Entity;
use dialog_query::layer::Layer;
use dialog_query::the;

use crate::{Branch, Upstream};

fn branch_entity() -> Entity {
    "id:branch".parse().expect("valid entity")
}

fn repository_entity() -> Entity {
    "id:repository".parse().expect("valid entity")
}

fn upstream_entity() -> Entity {
    "id:upstream".parse().expect("valid entity")
}

/// Build the metadata layer for `branch`.
///
/// Always emits `dialog.meta/name` for the branch and `dialog.meta/did` for
/// the repository. Adds revision and upstream facts when present.
pub fn branch_metadata(branch: &Branch) -> Layer {
    let branch_id = branch_entity();
    let repo_id = repository_entity();

    let mut layer = Layer::new()
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

        layer = layer
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
        layer = layer.assert(
            the!("dialog.meta/upstream")
                .of(branch_id.clone())
                .is(upstream_id.clone()),
        );

        let kind = match &upstream {
            Upstream::Local { .. } => "local",
            Upstream::Remote { .. } => "remote",
        };
        layer = layer
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
            layer = layer.assert(
                the!("dialog.meta/remote")
                    .of(upstream_id)
                    .is(remote.clone()),
            );
        }
    }

    layer
}
