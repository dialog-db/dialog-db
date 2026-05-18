//! [`TrackingBranch`] — a local branch that tracks a remote branch.

// The `#[derive(Concept)]` macro generates helper types and
// associated functions without doc comments. Suppress the
// crate-level `missing_docs` lint for this module so the macros
// compile under `-D warnings`.
#![allow(missing_docs)]

use dialog_artifacts::Entity;
use dialog_query::Concept;

use crate::schema::Branch;
use crate::schema::domain::branch::{Origin, Upstream};

/// A local branch's tracking relationship with a remote branch.
///
/// A `TrackingBranch` is an *attribute on the local branch entity*
/// that names the remote branch it tracks. `this` is reused from
/// the local branch itself (no hash); `upstream` points at the
/// remote branch, and `origin` points at the local replica that
/// owns the tracking relationship. Asserting a `TrackingBranch`
/// fact says: "the branch whose entity is `this` is tracking the
/// branch whose entity is `upstream`, as recorded by the replica
/// whose entity is `origin`."
///
/// # Why a separate concept
///
/// Semantically this is just an optional `Upstream` attribute on
/// [`Branch`] — a branch either tracks something or it doesn't.
/// Dialog concepts require every field to be present, so
/// "optional" can't be expressed as `Option<Upstream>` on `Branch`
/// itself. Modeling it as a separate concept lets the optionality
/// fall out of presence/absence of the fact: if a `TrackingBranch`
/// fact is asserted for a given branch entity, that branch tracks
/// something; if not, it doesn't. Queries pick it up via an
/// optional-match / left-join pattern rather than a null check.
///
/// # Why `Upstream` instead of reusing `Origin`
///
/// `Upstream` is the direction-explicit "I track this" relation.
/// `Origin` is the ownership "I belong to this" relation. The
/// local branch already has its own `Origin` pointing at the
/// replica; this concept carries both — `upstream` for the
/// relation it represents, and `origin` (reused from the local
/// branch's own origin) so queries can filter tracking links by
/// the replica they belong to without joining through the local
/// branch concept.
///
/// # Constructing
///
/// [`TrackingBranch::new`] takes the local branch and the upstream
/// (remote) branch it tracks:
///
/// ```no_run
/// # use dialog_repository::schema::{Branch, TrackingBranch};
/// # fn example(local: Branch, upstream: Branch) -> TrackingBranch {
/// TrackingBranch::new(&local, &upstream)
/// # }
/// ```
///
/// Or via the [`Branch::set_upstream`] shortcut:
///
/// ```no_run
/// # use dialog_repository::schema::{Branch, TrackingBranch};
/// # fn example(local: Branch, upstream: Branch) -> TrackingBranch {
/// local.set_upstream(&upstream)
/// # }
/// ```
///
/// [`Branch::set_upstream`]: crate::schema::Branch::set_upstream
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TrackingBranch {
    /// The local branch's entity.
    pub this: Entity,
    /// The upstream (remote) branch this one is tracking.
    pub upstream: Upstream,
    /// The replica that owns this tracking relationship —
    /// mirrors `local.origin`, which is always the replica for a
    /// local branch. Stored so tracking-branch queries can scope
    /// to a single replica in one shot.
    pub origin: Origin,
}

impl TrackingBranch {
    /// Build a tracking-branch link.
    ///
    /// `this` is set to `local`'s entity (no hash — this concept
    /// attaches a relationship attribute to an existing branch),
    /// `upstream` points at the `upstream` branch being tracked,
    /// and `origin` is reused from `local.origin` (which, for a
    /// local branch, is the replica).
    pub fn new(local: &Branch, upstream: &Branch) -> Self {
        Self {
            this: local.this.clone(),
            upstream: Upstream::from(upstream.this.clone()),
            origin: local.origin.clone(),
        }
    }
}

impl Branch {
    /// Record that this branch tracks `upstream`.
    ///
    /// Shortcut for [`TrackingBranch::new(self, upstream)`][TrackingBranch::new].
    pub fn set_upstream(&self, upstream: &Branch) -> TrackingBranch {
        TrackingBranch::new(self, upstream)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::schema::domain::remote::Address;
    use crate::schema::{Remote, Replica};
    use dialog_varsig::did;

    fn setup() -> (Branch, Branch) {
        let replica = Replica::new(did!("test:p"), did!("test:r"), "home");
        let remote = Remote::new(
            &replica,
            did!("test:repo"),
            Address(b"addr".to_vec()),
            "origin",
        );
        let local = replica.branch("main");
        let tracked = remote.branch("main");
        (local, tracked)
    }

    #[dialog_common::test]
    async fn this_is_local_branch_entity() {
        let (local, upstream) = setup();
        let link = TrackingBranch::new(&local, &upstream);
        assert_eq!(link.this, local.this);
    }

    #[dialog_common::test]
    async fn upstream_is_remote_branch_entity() {
        let (local, upstream) = setup();
        let link = TrackingBranch::new(&local, &upstream);
        assert_eq!(link.upstream.0, upstream.this);
    }

    #[dialog_common::test]
    async fn set_upstream_matches_new() {
        let (local, upstream) = setup();
        let via_method = local.set_upstream(&upstream);
        let via_new = TrackingBranch::new(&local, &upstream);
        assert_eq!(via_method, via_new);
    }

    #[dialog_common::test]
    async fn origin_mirrors_local_branch_origin() {
        let (local, upstream) = setup();
        let link = TrackingBranch::new(&local, &upstream);
        assert_eq!(link.origin, local.origin);
    }
}
