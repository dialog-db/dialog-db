//! Per-piece summaries for the compressed forced-run quiet check.
//!
//! A forced run's pieces are content-addressed stored nodes, so everything
//! the run-wide plan verification needs from an UNTOUCHED piece is a pure
//! function of the piece's bytes: entry count and summed weight, edge keys
//! and the last entry's weight (boundary seams and their coin verdicts are
//! formed against these), the piece-local coin outcomes, the trailing
//! bank, the heaviest interior vetoed stretch, and the best interior
//! election candidate of each backstop kind. A stored piece keeps its
//! summary with its own bytes (see `PersistentNode::summary`), so a piece is
//! streamed once per content change instead of once per quiet check — the
//! difference between the check costing O(run entries) and O(run pieces) on
//! the hot path.
//!
//! Piece-local coin evaluation is exact in the regimes the compressed
//! check accepts: the weight bank resets at every accepted seam, so when a
//! piece's left boundary seam is accepted (the frame-ceiling regime) the
//! bank entering the piece is zero, and when every seam is vetoed (the
//! stretch regime) there are no coin verdicts at all.
//!
//! A summary is read back only under the manifest knobs it was built with
//! ([`Knobs`]): `max_separator` and `anchor_selector` are the only ones the
//! summarized quantities read directly (coin verdicts also read
//! `max_segment`, but through `D::leaf_cut`, whose manifest is the same
//! one).

use dialog_common::Blake3Hash;

use super::cap;
use crate::{Distribution, Hashed, Manifest};

/// The manifest knobs a summary depends on: `max_separator` and
/// `anchor_selector`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Knobs(u32, u32);

impl From<&Manifest> for Knobs {
    fn from(manifest: &Manifest) -> Self {
        Self(manifest.max_separator, manifest.anchor_selector)
    }
}

/// Everything the compressed quiet check needs from one run piece.
#[derive(Debug, Clone)]
pub(crate) struct PieceSummary {
    /// Number of entries.
    pub count: usize,
    /// Summed entry weight ([`Entry::weight`](crate::Entry::weight)).
    pub weight: usize,
    /// The first entry's key bytes.
    pub first_key: Vec<u8>,
    /// The hash of the first entry's key: the anchor identity of the seam
    /// at the piece's left edge, which the run-wide election orders by.
    pub first_hash: Blake3Hash,
    /// The last entry's key bytes.
    pub last_key: Vec<u8>,
    /// The hash of the last entry's key: what the coin at the seam closing
    /// the piece draws from.
    pub last_hash: Blake3Hash,
    /// The last entry's weight — the boundary seam's own coin charge reads
    /// it together with `trailing_bank`.
    pub last_weight: usize,
    /// Whether every interior seam is vetoed (the stretch regime).
    pub all_vetoed: bool,
    /// Whether any interior accepted seam's coin verdict is a cut, under
    /// piece-local banks. A stored piece is whole, so a cut
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
    pub(crate) fn build<D>(keys: &[Hashed<'_>], weights: &[usize], manifest: &Manifest) -> Self
    where
        D: Distribution,
    {
        let count = keys.len();
        let weight = weights.iter().sum();
        // The edge hashes are taken through the keys' own cells, so a piece
        // built from live entries leaves them there for the next ask.
        let edge = |key: Option<&Hashed<'_>>| match key {
            Some(key) => (key.to_vec(), key.hash()),
            None => (Vec::new(), Blake3Hash::hash(&[])),
        };
        let (first_key, first_hash) = edge(keys.first());
        let (last_key, last_hash) = edge(keys.last());
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
            let left = keys[at - 1].bytes();
            let right = keys[at].bytes();
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
                            keys[at].hash(),
                            at,
                        ),
                    );
                }
            } else {
                all_vetoed = false;
                if let Some(sum) = stretch_weight.take() {
                    max_stretch_weight = max_stretch_weight.max(sum);
                }
                if D::leaf_cut(keys[at - 1], bank + weights[at - 1], manifest) {
                    interior_coin_cut = true;
                }
                bank = 0;
                if cap::is_frame_candidate(left, right, manifest) {
                    consider(
                        &mut frame_interior,
                        (
                            cap::shortest_separator_len(left, right),
                            keys[at].hash(),
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
            first_hash,
            last_key,
            last_hash,
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
