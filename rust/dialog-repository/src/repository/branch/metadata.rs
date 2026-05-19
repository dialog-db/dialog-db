//! Schema-metadata synthesis for a query session.
//!
//! `synthesize` builds the per-query overlay [`Changes`] from the
//! operator's [`Identify`] result and the branches the session has in
//! scope. Asserted facts:
//!
//! - one [`Origin`] per branch (`(profile, branch.of)`-derived);
//! - one [`Branch`](BranchConcept) per branch (`(origin, name)`-derived);
//! - one [`BranchRevision`] per branch that has a committed revision;
//! - one [`Session`] on the fixed `db:session` entity;
//! - one [`session::Branch`] attribute on `db:session` per layered
//!   branch (cardinality-many — separate from the `Session` concept).
//!
//! Called by `SelectQuery::perform` once at the start of each query.
//! Nothing here writes to any branch's tree; the returned `Changes`
//! is only used as an in-memory overlay via
//! [`Provider<Select> for Changes`](dialog_artifacts::Changes).

use base58::ToBase58;
use dialog_artifacts::Changes;
use dialog_capability::Did;
use dialog_query::the;

use crate::Branch;
use crate::schema::Branch as BranchConcept;
use crate::schema::BranchRevision;
use crate::schema::DidExt as _;
use crate::schema::Origin;
use crate::schema::Session;
use crate::schema::branch::{Moment, Period, Tree};
use crate::schema::session;

/// Build the metadata [`Changes`] overlay for a query session.
///
/// `primary` is the session's primary branch (the one
/// [`Branch::query`](crate::Branch::query) was called on); `branches`
/// are any additional branches joined via `.join(&b)`. `profile` and
/// `operator` come from [`Identify`](dialog_effects::authority::Identify)
/// on the perform-env.
///
/// Asserts schema-shaped facts for every branch (Origin / Branch /
/// optional BranchRevision) and one `Session` describing the query's
/// (profile, operator, branches) context. See the module docs for the
/// full fact set.
pub(crate) fn synthesize(
    primary: Option<&Branch>,
    branches: &[&Branch],
    profile: &Did,
    operator: &Did,
) -> Changes {
    let mut changes = Changes::new();
    let session_entity = Session::entity();
    let mut branch_entities = Vec::new();

    // Per-branch facts: Origin + Branch + optional BranchRevision.
    let mut emit_branch = |b: &Branch| {
        let origin = Origin::new(profile.clone(), b.of().clone());
        let branch_concept = BranchConcept::new(&origin, b.name());
        branch_entities.push(branch_concept.this.clone());
        changes.assert(origin);
        if let Some(revision) = b.revision() {
            let tree_bytes: &[u8] = revision.tree.hash();
            changes.assert(BranchRevision {
                this: branch_concept.this.clone(),
                tree: Tree(ToBase58::to_base58(tree_bytes)),
                period: Period(revision.period as u128),
                moment: Moment(revision.moment as u128),
            });
        }
        changes.assert(branch_concept);
    };
    if let Some(b) = primary {
        emit_branch(b);
    }
    for b in branches {
        emit_branch(b);
    }

    // Session — one fact set for the whole query.
    changes.assert(Session {
        this: session_entity.clone(),
        profile: session::Profile(profile.this()),
        operator: session::Operator(operator.this()),
    });

    // `dialog.session/branch` — cardinality-many, one per branch.
    // Asserted as a free-standing attribute (not part of the Session
    // concept, which only holds the cardinality-one fields).
    for branch_entity in branch_entities {
        changes.assert(
            the!("dialog.session/branch")
                .of(session_entity.clone())
                .is(branch_entity),
        );
    }

    changes
}
