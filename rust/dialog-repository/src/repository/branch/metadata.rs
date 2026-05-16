//! Branch metadata overlay.
//!
//! Builds an [`Overlay`] populated with synthetic facts describing a
//! [`Branch`]: its name, current revision, tree hash, hosting repository,
//! and upstream tracking state — all exposed under the `dialog.meta/*`
//! attribute namespace and rooted at the well-known entity `dialog:branch`.
//!
//! These facts let callers run normal [`Query`](dialog_query::Query)s against
//! branch state without reaching for branch-specific accessors, mirroring how
//! Datomic exposes its own metadata as queryable triples (`:db/ident` and
//! friends).
//!
//! # Entities and attributes
//!
//! All facts are asserted on the singleton entity `dialog:branch` for the
//! branch being queried. Upstream and repository identity link out to their
//! own synthetic entities:
//!
//! | Entity                       | Attribute                       | Value (`Type`)        |
//! |------------------------------|---------------------------------|-----------------------|
//! | `dialog:branch`              | `dialog.meta/name`              | branch name (string)  |
//! | `dialog:branch`              | `dialog.meta/revision-hash`     | tree hash (string)    |
//! | `dialog:branch`              | `dialog.meta/period`            | logical period (uint) |
//! | `dialog:branch`              | `dialog.meta/moment`            | logical moment (uint) |
//! | `dialog:branch`              | `dialog.meta/repository`        | repo entity           |
//! | `dialog:branch`              | `dialog.meta/upstream`          | upstream entity       |
//! | `dialog:repository`          | `dialog.meta/did`               | repo DID (string)     |
//! | `dialog:upstream`            | `dialog.meta/kind`              | "local"/"remote"      |
//! | `dialog:upstream`            | `dialog.meta/branch`            | upstream branch name  |
//! | `dialog:upstream`            | `dialog.meta/remote`            | remote name (remote)  |

use base58::ToBase58;
use dialog_artifacts::{Entity, Value};
use dialog_query::overlay::Overlay;

use crate::{Branch, Upstream};

const BRANCH_ENTITY: &str = "dialog:branch";
const REPO_ENTITY: &str = "dialog:repository";
const UPSTREAM_ENTITY: &str = "dialog:upstream";

/// Build the metadata overlay for `branch`.
///
/// Always emits `dialog.meta/name` and `dialog.meta/repository`. Adds
/// revision and upstream facts when present on the branch.
pub fn branch_metadata(branch: &Branch) -> Overlay {
    let mut overlay = Overlay::new();

    // Branch identity.
    overlay = overlay
        .fact(
            "dialog.meta/name",
            BRANCH_ENTITY,
            Value::String(branch.name().to_string()),
        )
        .expect("dialog.meta/name is a valid attribute and dialog:branch is a valid entity");

    // Link to the repository entity, then assert its DID separately.
    overlay = overlay
        .fact(
            "dialog.meta/repository",
            BRANCH_ENTITY,
            Value::Entity(
                REPO_ENTITY
                    .parse::<Entity>()
                    .expect("dialog:repository is a valid entity"),
            ),
        )
        .expect("dialog.meta/repository is a valid attribute");

    overlay = overlay
        .fact(
            "dialog.meta/did",
            REPO_ENTITY,
            Value::String(branch.of().to_string()),
        )
        .expect("dialog.meta/did is a valid attribute");

    // Revision-derived facts. A fresh branch with no commits has no revision.
    if let Some(revision) = branch.revision() {
        let hash_bytes: &[u8] = revision.tree.hash();
        let hash_b58 = ToBase58::to_base58(hash_bytes);

        overlay = overlay
            .fact(
                "dialog.meta/revision-hash",
                BRANCH_ENTITY,
                Value::String(hash_b58),
            )
            .expect("dialog.meta/revision-hash is a valid attribute");

        overlay = overlay
            .fact(
                "dialog.meta/period",
                BRANCH_ENTITY,
                Value::UnsignedInt(revision.period as u128),
            )
            .expect("dialog.meta/period is a valid attribute");

        overlay = overlay
            .fact(
                "dialog.meta/moment",
                BRANCH_ENTITY,
                Value::UnsignedInt(revision.moment as u128),
            )
            .expect("dialog.meta/moment is a valid attribute");

        overlay = overlay
            .fact(
                "dialog.meta/issuer",
                BRANCH_ENTITY,
                Value::String(revision.issuer.to_string()),
            )
            .expect("dialog.meta/issuer is a valid attribute");

        overlay = overlay
            .fact(
                "dialog.meta/authority",
                BRANCH_ENTITY,
                Value::String(revision.authority.to_string()),
            )
            .expect("dialog.meta/authority is a valid attribute");
    }

    // Upstream tracking — only present when configured.
    if let Some(upstream) = branch.upstream() {
        overlay = overlay
            .fact(
                "dialog.meta/upstream",
                BRANCH_ENTITY,
                Value::Entity(
                    UPSTREAM_ENTITY
                        .parse::<Entity>()
                        .expect("dialog:upstream is a valid entity"),
                ),
            )
            .expect("dialog.meta/upstream is a valid attribute");

        let kind = match &upstream {
            Upstream::Local { .. } => "local",
            Upstream::Remote { .. } => "remote",
        };
        overlay = overlay
            .fact(
                "dialog.meta/kind",
                UPSTREAM_ENTITY,
                Value::String(kind.to_string()),
            )
            .expect("dialog.meta/kind is a valid attribute");

        overlay = overlay
            .fact(
                "dialog.meta/branch",
                UPSTREAM_ENTITY,
                Value::String(upstream.branch().to_string()),
            )
            .expect("dialog.meta/branch is a valid attribute");

        if let Upstream::Remote { remote, .. } = &upstream {
            overlay = overlay
                .fact(
                    "dialog.meta/remote",
                    UPSTREAM_ENTITY,
                    Value::String(remote.clone()),
                )
                .expect("dialog.meta/remote is a valid attribute");
        }
    }

    overlay
}
