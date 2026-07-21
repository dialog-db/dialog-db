//! Skip links: logarithmic shortcuts through the revision DAG.
//!
//! A revision advanced from a single parent carries, inside its
//! [`RevisionRecord`](super::RevisionRecord), a table of *skip links* —
//! anchor versions deep in its first-parent run.
//! [`common_ancestor`](super::common_ancestor) uses them to leap over long
//! linear runs of history instead of walking every edge, turning the
//! descent from a far-ahead head down to the other head's causal depth
//! from O(gap) reads into O(log gap).
//!
//! # The anchor table
//!
//! Anchors are chosen by the 2-adic valuation of editions (`v2(e)` = how
//! many times 2 divides `e`): the level-`k` anchor of a revision is the
//! most recent first-parent ancestor whose edition is divisible by
//! `2^(k+1)`. Editions increment by one along a first-parent run, so
//! anchors sit at exponentially coarser marks the deeper they reach —
//! the same ladder a deterministic skip list builds.
//!
//! The table stores the DISTINCT anchors, ordered by strictly decreasing
//! edition (equivalently, strictly increasing valuation): one entry
//! whose edition is divisible by `2^(k+1)` serves every level up to its
//! valuation, so the level-`k` anchor is the first entry with
//! `v2(edition) >= k + 1`. A table therefore holds at most `log2(run
//! length)` entries.
//!
//! # Carrying the table forward
//!
//! This shape is what makes construction O(1): the child's table is a
//! **pure function of the parent's version and the parent's own table**.
//! The parent becomes the most recent anchor for every level its
//! edition's valuation reaches, superseding any parent-table entry of
//! equal-or-lower valuation; entries of strictly higher valuation are
//! carried forward unchanged:
//!
//! ```text
//! child_table = [parent, if v2(parent.edition) >= 1]
//!            ++ parent_table.filter(v2(entry.edition) > v2(parent.edition))
//! ```
//!
//! Extending the table thus reads ONE record (the parent's — warm in the
//! branch record memo on a held handle), where the previous
//! binary-lifting shape re-read log2(depth) ancestor records from the
//! tree on every commit and dominated the commit profile at depth.
//!
//! # Exactness rules
//!
//! Two rules keep the accelerated traversal *exact* (it finds the same
//! maximal common ancestor a stepwise walk would):
//!
//! - **A leap never crosses a merge.** A merge revision pulls ancestry in
//!   through every parent; leaping over one would lose the lineage
//!   entering through its other parents. Merge revisions record no table,
//!   and the carry rule inherits that: the child of a merge starts a
//!   fresh table that may anchor AT the merge (chains can end at one) but
//!   carries nothing from beyond it, so every leapt-over region is
//!   strictly linear — no ancestry enters or exits it except at its ends.
//! - **A leap never descends past the horizon** — the lower of the two
//!   head editions being compared. Editions strictly decrease along
//!   causal paths, so every ancestor of the lower head sits at or below
//!   its edition; a leap that stays above the horizon can only jump over
//!   revisions the other side can never reach anyway.

use crate::DialogArtifactsError;

use super::{History, Version};

/// The 2-adic valuation of an edition: how many times 2 divides it. The
/// genesis edition 0 is divisible by every power, so it anchors every
/// level a table can hold — a run rooted at genesis can always leap
/// straight to it.
fn valuation(edition: u64) -> u32 {
    if edition == 0 {
        u64::BITS
    } else {
        edition.trailing_zeros()
    }
}

/// Compute the skip table for a new revision whose single parent is
/// `parent`, as a pure function of the parent's version and record fields
/// — no history reads (see the module docs for the carry rule).
///
/// `parent_parents` and `parent_skips` are the parent record's DAG edge
/// and table. A merge parent (several entries) or genesis parent (none)
/// starts a fresh chain: the child may anchor at the parent itself, but
/// nothing beyond it is carried — a leap must never cross a merge.
pub fn carry_skips(
    parent: &Version,
    parent_parents: &[Version],
    parent_skips: &[Version],
) -> Vec<Version> {
    let parent_valuation = valuation(parent.edition.value());
    let mut table = Vec::new();
    if parent_valuation >= 1 {
        table.push(*parent);
    }
    // Only a single-parent parent's table lies within this run; a merge
    // records no table anyway, and genesis has none.
    if parent_parents.len() == 1 {
        table.extend(
            parent_skips
                .iter()
                .filter(|entry| valuation(entry.edition.value()) > parent_valuation)
                .copied(),
        );
    }
    table
}

/// Compute the skip table for a new revision whose single parent is
/// `parent`, reading the parent's record from `history` — exactly one
/// record lookup, memo-warm on a held branch handle.
///
/// A parent whose record has not been replicated yields an empty table —
/// skips are an accelerator, never a correctness requirement.
pub async fn extend_skips<H: History>(
    history: &H,
    parent: &Version,
) -> Result<Vec<Version>, DialogArtifactsError> {
    let Some(record) = history.revision_record(parent).await? else {
        return Ok(Vec::new());
    };
    Ok(carry_skips(parent, &record.parents, &record.skips))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{Edition, Origin};

    fn version(edition: u64) -> Version {
        Version::new(Origin::from([7u8; 32]), Edition::new(edition))
    }

    /// The carry rule maintains the anchor invariant along a linear run:
    /// entries in strictly decreasing edition and strictly increasing
    /// valuation, with the level-k anchor recoverable as the first entry
    /// whose edition is divisible by 2^(k+1) — byte-for-byte what a
    /// from-scratch construction over the whole run would produce.
    #[test]
    fn it_carries_the_anchor_table_along_a_run() {
        // Simulate a run rooted at genesis: each revision's table is
        // carried from its parent's.
        let mut tables: Vec<Vec<Version>> = vec![Vec::new()]; // genesis
        for edition in 1..=64u64 {
            let parent = version(edition - 1);
            // A linear run: genesis has no parent, every later revision
            // has exactly one.
            let grandparent = [version(edition.saturating_sub(2))];
            let parents: &[Version] = if edition == 1 { &[] } else { &grandparent };
            let table = carry_skips(&parent, parents, &tables[(edition - 1) as usize]);

            // Invariant: strictly decreasing editions, strictly
            // increasing valuations.
            for window in table.windows(2) {
                assert!(window[0].edition > window[1].edition, "editions decrease");
                assert!(
                    valuation(window[0].edition.value()) < valuation(window[1].edition.value()),
                    "valuations increase"
                );
            }
            // Semantics: the level-k anchor is the most recent ancestor
            // with edition divisible by 2^(k+1).
            for k in 0..6u32 {
                let modulus = 1u64 << (k + 1);
                let expected = (0..edition).rev().find(|e| e % modulus == 0);
                let anchor = table
                    .iter()
                    .find(|entry| valuation(entry.edition.value()) > k)
                    .map(|entry| entry.edition.value());
                assert_eq!(
                    anchor, expected,
                    "level-{k} anchor at edition {edition}: table {table:?}"
                );
            }
            tables.push(table);
        }
        // The table stays logarithmic: 64 editions -> at most 7 entries.
        assert!(tables.iter().all(|table| table.len() <= 7));
    }

    /// The child of a merge starts a fresh chain: it may anchor AT the
    /// merge (a chain can end at one) but carries nothing from beyond
    /// it — a leap never crosses a merge.
    #[test]
    fn it_resets_the_chain_at_a_merge() {
        let merge = version(8); // divisible by 8: would anchor 3 levels
        let merge_parents = [version(7), version(5)];
        // Whatever table the merge's own record might carry is ignored.
        let stale = vec![version(6), version(4)];
        let table = carry_skips(&merge, &merge_parents, &stale);
        assert_eq!(
            table,
            vec![merge],
            "anchor at the merge only, nothing beyond it"
        );

        let odd_merge = version(9);
        assert!(
            carry_skips(&odd_merge, &merge_parents, &stale).is_empty(),
            "an odd-edition merge anchors nothing"
        );
    }

    /// A run rooted at genesis can always leap straight back to it:
    /// edition 0 is divisible by every power, so genesis survives every
    /// carry as the deepest anchor.
    #[test]
    fn it_keeps_genesis_as_the_deepest_anchor() {
        let genesis = version(0);
        let mut table = carry_skips(&genesis, &[], &[]);
        assert_eq!(table, vec![genesis]);
        for edition in 1..=32u64 {
            let parent = version(edition);
            let parents = [version(edition - 1)];
            table = carry_skips(&parent, &parents, &table);
            assert_eq!(
                table.last(),
                Some(&genesis),
                "genesis stays the deepest anchor at edition {edition}"
            );
        }
    }
}
