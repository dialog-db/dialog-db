//! Skip links: logarithmic shortcuts through the revision DAG.
//!
//! A revision advanced from a single parent carries, inside its
//! [`RevisionRecord`](super::RevisionRecord), a table of *skip links* —
//! versions 2^k first-parent steps back. [`common_ancestor`](super::common_ancestor)
//! uses them to leap over long linear runs of history instead of walking
//! every edge, turning the descent from a far-ahead head down to the other
//! head's causal depth from O(gap) reads into O(log gap).
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
//! tables of ancestors — O(log depth) record lookups per commit — and
//! stops as soon as a link is missing, which is exactly what happens at a
//! merge or at genesis.

use crate::DialogArtifactsError;

use super::{History, Version};

/// Skip levels are capped well past any real history depth (2^33 revisions).
const MAX_SKIP_LEVEL: usize = 32;

/// Compute the skip table for a new revision whose single parent is
/// `parent`, by binary lifting over the tables its ancestors recorded.
///
/// Entry `i` of the result leaps 2^(i+1) first-parent steps back — the
/// shape [`RevisionRecord::skips`](super::RevisionRecord::skips) carries.
/// The table is empty when the parent is a merge or genesis revision (see
/// the module docs for why chains must not cross merges); it regrows
/// logarithmically on the commits that follow. A parent whose record has
/// not been replicated simply terminates the table — skips are an
/// accelerator, never a correctness requirement.
pub async fn extend_skips<H: History>(
    history: &H,
    parent: &Version,
) -> Result<Vec<Version>, DialogArtifactsError> {
    let mut table = Vec::new();
    // `prev` is this revision's level-(k-1) target; for k = 1 that is the
    // parent itself (level 0 — one step back — is the DAG edge).
    let mut prev = *parent;
    for level in 1..=MAX_SKIP_LEVEL {
        let Some(record) = history.revision_record(&prev).await? else {
            break;
        };
        let hop = if level == 1 {
            // S_1(me) = the parent's own single parent; a merge (several
            // parents) or genesis (none) terminates the chain.
            match record.parents.as_slice() {
                [parent] => *parent,
                _ => break,
            }
        } else {
            // S_k(me) = the level-(k-1) link of my level-(k-1) target.
            match record.skips.get(level - 2) {
                Some(hop) => *hop,
                None => break,
            }
        };
        table.push(hop);
        prev = hop;
    }
    Ok(table)
}
