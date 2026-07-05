//! Skip links: logarithmic shortcuts through the revision DAG.
//!
//! A revision advanced from a single parent records, next to its DAG edge,
//! a table of *skip links* — claims whose causes point 2^k first-parent
//! steps back. [`common_ancestor`](super::common_ancestor) uses them to
//! leap over long linear runs of history instead of walking every edge,
//! turning the descent from a far-ahead head down to the other head's
//! causal depth from O(gap) reads into O(log gap).
//!
//! Two rules keep the accelerated traversal *exact* (it finds the same
//! maximal common ancestor a stepwise walk would):
//!
//! - **A leap never crosses a merge.** A merge revision pulls ancestry in
//!   through every parent; leaping over one would lose the lineage entering
//!   through its other parents. Merge revisions therefore record no skip
//!   table, and the binary-lifting construction below inherits that: a
//!   chain of skips can end *at* a merge (targets are expanded normally)
//!   but never passes through one, so every leapt-over region is strictly
//!   linear — no ancestry enters or exits it except at its ends.
//! - **A leap never descends past the horizon** — the lower of the two
//!   head editions being compared. Editions strictly decrease along causal
//!   paths, so every ancestor of the lower head sits at or below its
//!   edition; a leap that stays above the horizon can only jump over
//!   revisions the other side can never reach anyway.
//!
//! The table is built by binary lifting: level 1 points at the parent's
//! parent, and level `k` points at where the level-`k-1` target's own
//! level-`k-1` link points. Construction therefore only reads the recorded
//! tables of ancestors — O(log depth) lookups per commit — and stops as
//! soon as a link is missing, which is exactly what happens at a merge or
//! at genesis.

use crate::{DialogArtifactsError, Value};

use super::{History, Version};

/// The attribute under which a revision's skip links are recorded. The
/// claim's entity is the revision entity, its value is the skip level `k`
/// (an unsigned integer), and its cause is the version 2^k first-parent
/// steps back.
pub const SKIP_ATTRIBUTE: &str = "dialog.db/skip";

/// Skip levels are capped well past any real history depth (2^32 revisions).
const MAX_SKIP_LEVEL: u32 = 32;

/// The first parent of the revision identified by `version`: the single
/// cause of its DAG edge. `None` for a genesis revision (no cause) or a
/// merge revision (several) — skip chains terminate at both.
pub async fn parent_of<H: History>(
    history: &H,
    version: &Version,
) -> Result<Option<Version>, DialogArtifactsError> {
    let mut parents = history
        .revision_at(version)
        .await?
        .into_iter()
        .flat_map(|claim| claim.cause.versions().to_vec());
    match (parents.next(), parents.next()) {
        (Some(parent), None) => Ok(Some(parent)),
        _ => Ok(None),
    }
}

/// The recorded level-`level` skip link of the revision identified by
/// `version`, if any.
async fn skip_of<H: History>(
    history: &H,
    version: &Version,
    level: u32,
) -> Result<Option<Version>, DialogArtifactsError> {
    let level = Value::UnsignedInt(u128::from(level));
    Ok(history
        .skips_at(version)
        .await?
        .into_iter()
        .find(|claim| claim.is == level)
        .and_then(|claim| claim.cause.versions().first().copied()))
}

/// Compute the skip table for a new revision whose single parent is
/// `parent`, by binary lifting over the tables its ancestors recorded:
/// the level-`k` link is the level-`k-1` link of the level-`k-1` target.
///
/// Returns `(level, target)` pairs, one claim's worth each. The table is
/// empty when the parent is a merge or genesis revision (see the module
/// docs for why chains must not cross merges); it regrows logarithmically
/// on the commits that follow.
pub async fn extend_skips<H: History>(
    history: &H,
    parent: &Version,
) -> Result<Vec<(u32, Version)>, DialogArtifactsError> {
    let mut table = Vec::new();
    // `prev` is this revision's level-(k-1) target; for k = 1 that is the
    // parent itself (level 0 — one step back — is the DAG edge).
    let mut prev = *parent;
    for level in 1..=MAX_SKIP_LEVEL {
        let hop = if level == 1 {
            parent_of(history, &prev).await?
        } else {
            skip_of(history, &prev, level - 1).await?
        };
        let Some(hop) = hop else {
            break;
        };
        table.push((level, hop));
        prev = hop;
    }
    Ok(table)
}
