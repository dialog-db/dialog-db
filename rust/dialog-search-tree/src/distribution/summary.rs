//! Per-piece summaries for the compressed forced-run quiet check.
//!
//! A forced run's pieces are content-addressed stored nodes, so everything
//! the run-wide plan verification needs from an UNTOUCHED piece is a pure
//! function of the piece's bytes: entry count and summed weight, edge keys
//! and the last entry's weight (boundary seams and their coin verdicts are
//! formed against these), the piece-local coin outcomes, the trailing
//! bank, the heaviest interior vetoed stretch, and the best interior
//! election candidate of each backstop kind. Summaries are memoized per
//! node hash, so a piece is streamed once per content change instead of
//! once per quiet check — the difference between the check costing
//! O(run entries) and O(run pieces) on the hot path.
//!
//! Piece-local coin evaluation is exact in the regimes the compressed
//! check accepts: the weight bank resets at every accepted seam, so when a
//! piece's left boundary seam is accepted (the frame-ceiling regime) the
//! bank entering the piece is zero, and when every seam is vetoed (the
//! stretch regime) there are no coin verdicts at all. The pacing ramp
//! (frame-prefix context) is NOT modeled; the caller must fall back to the
//! full stream when it is armed.
//!
//! The memo is thread-local and bounded like the key-hash memo: entries
//! are keyed by `(node hash, max_separator, anchor_selector)` — the only
//! manifest knobs the summarized quantities read (coin verdicts also read
//! `max_segment`, but through `D::leaf_cut`, whose manifest is the same
//! one; a manifest change changes the key) — and the map is cleared
//! wholesale at capacity.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use dialog_common::Blake3Hash;

use super::cap;
use crate::{Distribution, Manifest};

/// Everything the compressed quiet check needs from one run piece.
#[derive(Debug, Clone)]
pub(crate) struct PieceSummary {
    /// Number of entries.
    pub count: usize,
    /// Summed entry weight ([`Entry::weight`](crate::Entry::weight)).
    pub weight: usize,
    /// The first entry's key bytes.
    pub first_key: Vec<u8>,
    /// The last entry's key bytes.
    pub last_key: Vec<u8>,
    /// The last entry's weight — the boundary seam's own coin charge reads
    /// it together with `trailing_bank`.
    pub last_weight: usize,
    /// Whether every interior seam is vetoed (the stretch regime).
    pub all_vetoed: bool,
    /// Whether any interior accepted seam's coin verdict is a cut, under
    /// piece-local banks and no ramp. A stored piece is whole, so a cut
    /// here means the plan does not reproduce the stored partition.
    pub interior_coin_cut: bool,
    /// The bank flowing into the seam after the piece's last entry: the
    /// summed weights of the left partners of the trailing maximal run of
    /// vetoed interior seams.
    pub trailing_bank: usize,
    /// The heaviest maximal interior vetoed stretch, measured as the
    /// stretch election measures it (summed entry weights over the
    /// stretch's full key range). Over `max_segment` the stretch backstop
    /// could cut inside the piece, which the summary cannot decide.
    pub max_stretch_weight: usize,
    /// Best interior vetoed-seam candidate of the stretch backstop
    /// ([`cap::is_forced_candidate`]): `(separator_len, right-key hash,
    /// right-key offset)`.
    pub stretch_interior: Option<(usize, Blake3Hash, usize)>,
    /// Best interior accepted-seam candidate of the frame ceiling
    /// ([`cap::is_frame_candidate`]), same shape.
    pub frame_interior: Option<(usize, Blake3Hash, usize)>,
}

impl PieceSummary {
    /// Builds a summary from a piece's keys (in entry order) and per-entry
    /// weights, mirroring `cut_plan`'s seam walk at piece scope: vetoes
    /// and banks left to right, coin verdicts at accepted seams, stretch
    /// extents and both backstops' candidate minima.
    pub(crate) fn build<D>(keys: &[&[u8]], weights: &[usize], manifest: &Manifest) -> Self
    where
        D: Distribution,
    {
        let count = keys.len();
        let weight = weights.iter().sum();
        let first_key = keys.first().map(|key| key.to_vec()).unwrap_or_default();
        let last_key = keys.last().map(|key| key.to_vec()).unwrap_or_default();
        let last_weight = weights.last().copied().unwrap_or_default();
        let selector = cap::AnchorSelector::from_manifest(manifest);

        let mut all_vetoed = true;
        let mut interior_coin_cut = false;
        let mut bank = 0usize;
        let mut max_stretch_weight = 0usize;
        // The open stretch's summed weight over its full key range
        // `[start..=current]`, maintained incrementally: opening at seam
        // `(at - 1, at)` seeds both partners' weights, each further vetoed
        // seam adds its right partner.
        let mut stretch_weight: Option<usize> = None;
        let mut stretch_interior: Option<(usize, Blake3Hash, usize)> = None;
        let mut frame_interior: Option<(usize, Blake3Hash, usize)> = None;

        let consider = |slot: &mut Option<(usize, Blake3Hash, usize)>,
                        candidate: (usize, Blake3Hash, usize)| {
            let wins = match slot {
                None => true,
                Some(current) => cap::anchor_precedes(
                    selector,
                    (candidate.0, &candidate.1, candidate.2),
                    (current.0, &current.1, current.2),
                ),
            };
            if wins {
                *slot = Some(candidate);
            }
        };

        for at in 1..count {
            let left = keys[at - 1];
            let right = keys[at];
            if D::vetoes(left, right, manifest) {
                bank += weights[at - 1];
                stretch_weight = Some(match stretch_weight {
                    None => weights[at - 1] + weights[at],
                    Some(sum) => sum + weights[at],
                });
                if cap::is_forced_candidate(left, right, manifest) {
                    consider(
                        &mut stretch_interior,
                        (
                            cap::shortest_separator_len(left, right),
                            super::hash_memo::hash(right),
                            at,
                        ),
                    );
                }
            } else {
                all_vetoed = false;
                if let Some(sum) = stretch_weight.take() {
                    max_stretch_weight = max_stretch_weight.max(sum);
                }
                if D::leaf_cut(left, bank + weights[at - 1], manifest) {
                    interior_coin_cut = true;
                }
                bank = 0;
                if cap::is_frame_candidate(left, right, manifest) {
                    consider(
                        &mut frame_interior,
                        (
                            cap::shortest_separator_len(left, right),
                            super::hash_memo::hash(right),
                            at,
                        ),
                    );
                }
            }
        }
        if let Some(sum) = stretch_weight {
            max_stretch_weight = max_stretch_weight.max(sum);
        }

        Self {
            count,
            weight,
            first_key,
            last_key,
            last_weight,
            all_vetoed,
            interior_coin_cut,
            trailing_bank: bank,
            max_stretch_weight,
            stretch_interior,
            frame_interior,
        }
    }
}

/// Entries retained before the memo resets. Summaries are small (two edge
/// keys plus a handful of scalars), so this bounds the memo well under the
/// node cache's footprint.
const CAPACITY: usize = 1 << 14;

type MemoKey = (Blake3Hash, u32, u32);

thread_local! {
    /// The per-thread summary memo, keyed by node hash plus the manifest
    /// knobs the summary reads.
    static MEMO: RefCell<HashMap<MemoKey, Arc<PieceSummary>>> = RefCell::new(HashMap::new());
}

/// The memo key for `hash` under `manifest`.
fn key(hash: &Blake3Hash, manifest: &Manifest) -> MemoKey {
    (
        hash.clone(),
        manifest.max_separator,
        manifest.anchor_selector,
    )
}

/// The memoized summary for the piece stored under `hash`, if present.
pub(crate) fn memoized(hash: &Blake3Hash, manifest: &Manifest) -> Option<Arc<PieceSummary>> {
    MEMO.with(|memo| memo.borrow().get(&key(hash, manifest)).cloned())
}

/// Memoizes `summary` for the piece stored under `hash`, returning the
/// shared handle.
pub(crate) fn memoize(
    hash: &Blake3Hash,
    manifest: &Manifest,
    summary: PieceSummary,
) -> Arc<PieceSummary> {
    MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= CAPACITY {
            memo.clear();
        }
        let summary = Arc::new(summary);
        memo.insert(key(hash, manifest), summary.clone());
        summary
    })
}
