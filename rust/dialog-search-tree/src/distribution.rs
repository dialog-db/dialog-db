use dialog_common::Blake3Hash;

/// The rank of a node in the prolly tree.
pub type Rank = u64;

/// Strategy for assigning ranks to keys and separators, and for deriving the
/// separators themselves.
///
/// The distribution decides which entries end leaf segments (the leaf coin,
/// [`rank`](Self::rank)), which seams punch boundaries through index levels
/// (the seam coin, [`seam_rank`](Self::seam_rank)), and what byte string an
/// index link stores to route across a seam ([`separator`](Self::separator)).
/// Together these determine the shape of the tree as a pure function of its
/// key set, which is what keeps it history-independent.
///
/// A separator follows the lower-bound convention: the separator carried by a
/// link is the shortest byte string that sorts strictly above everything in
/// the left-adjacent subtree and at or below everything in the link's own
/// subtree. The global leftmost link at every level carries the empty
/// separator (negative infinity). Because a separator is always a prefix of
/// its own subtree's minimum leaf key, it can be maintained from the edited
/// side of a seam alone (see [`reseparate`](Self::reseparate)).
///
/// The default, [`Geometric`], hashes keys and separators with blake3 and
/// stores shortest-distinguishing separators, producing the canonical
/// production shape. Tests may inject an alternative distribution to force
/// exact tree shapes.
pub trait Distribution {
    /// The leaf coin: computes the rank of an entry key from its bytes. An
    /// entry whose rank exceeds [`BOTTOM_RANK`](crate::BOTTOM_RANK) ends its
    /// leaf segment.
    fn rank(key: &[u8]) -> Rank;

    /// The seam coin: computes the rank of a seam from its separator bytes.
    /// A child whose separator rank exceeds the level threshold starts a new
    /// index node at that level. The same separator string serves every level
    /// along a vertical boundary, so a high rank punches through several
    /// levels at once, exactly like the key-rank recursion it replaces.
    ///
    /// The default applies the key coin to the separator bytes, which is the
    /// right choice for any hash-based distribution (the two coins stay
    /// independent because their inputs never collide: a separator sorts
    /// strictly between two keys).
    fn seam_rank(separator: &[u8]) -> Rank {
        Self::rank(separator)
    }

    /// Derives the separator for a fresh seam from the two keys adjacent to
    /// it: `left` is the last leaf key before the seam and `right` the first
    /// leaf key after it, with `left < right`. Defaults to the canonical
    /// shortest-distinguishing prefix of `right`.
    fn separator(left: &[u8], right: &[u8]) -> Vec<u8> {
        shortest_separator(left, right)
    }

    /// Re-derives a child's separator after an edit may have changed the
    /// child's minimum leaf key, without access to the left neighbor.
    ///
    /// `min` is the child's (possibly new) minimum leaf key and `floor` its
    /// previous separator. The previous separator encodes everything needed
    /// about the unloaded left neighbor: it sorts strictly above the
    /// neighbor's maximum, and routing guarantees `min >= floor` (every key
    /// an edit delivers to the child was routed by `key >= separator`). The
    /// canonical result is the shortest prefix of `min` that is `>= floor`,
    /// the default.
    ///
    /// An override must stay consistent with [`separator`](Self::separator):
    /// for any valid seam, `reseparate(min, separator(left, min))` must
    /// reproduce `separator(left, min)`, or canonical form breaks.
    fn reseparate(min: &[u8], floor: &[u8]) -> Vec<u8> {
        raise_to_floor(min, floor)
    }
}

/// The default [`Distribution`]: geometric coins over blake3 hashes with
/// shortest-distinguishing separators (see [`geometric`]).
#[derive(Clone, Debug, Default)]
pub struct Geometric;

impl Distribution for Geometric {
    fn rank(key: &[u8]) -> Rank {
        geometric::rank(&Blake3Hash::hash(key))
    }
}

/// The shortest prefix of `right` that sorts strictly above `left`, given
/// `left < right`: the canonical shortest-distinguishing separator of a seam
/// (RocksDB's `FindShortestSeparator`, taken as a prefix of the right-hand
/// key so the lower-bound convention holds).
///
/// `left < right` guarantees `right` is not a prefix of `left`, so the byte
/// at the divergence point always exists in `right`.
pub fn shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
    debug_assert!(
        left < right,
        "separator requires ordered seam keys: {left:02x?} < {right:02x?}"
    );
    let lcp = left
        .iter()
        .zip(right.iter())
        .take_while(|(a, b)| a == b)
        .count();
    right[..=lcp.min(right.len() - 1)].to_vec()
}

/// The shortest prefix of `min` that sorts at or above `floor`: the canonical
/// separator of a seam whose right-hand minimum is `min`, re-derived from the
/// seam's previous separator `floor` instead of the (unavailable) left key.
///
/// Correctness relies on two invariants the tree maintains: `floor` sorts
/// strictly above the left neighbor's maximum and diverges from it exactly at
/// its own last byte, and `min >= floor` (routing sends a key into the seam's
/// right side only when the key is at or above the stored separator). Under
/// those, the result equals `shortest_separator(left_max, min)` byte for
/// byte, so incremental maintenance and a fresh build converge.
pub fn raise_to_floor(min: &[u8], floor: &[u8]) -> Vec<u8> {
    let lcp = min
        .iter()
        .zip(floor.iter())
        .take_while(|(a, b)| a == b)
        .count();
    if lcp == floor.len() {
        // The floor is a prefix of (or equal to) min: it remains the shortest
        // prefix of min at or above itself. In particular an empty floor (the
        // global leftmost seam) stays empty.
        floor.to_vec()
    } else if lcp < min.len() && min[lcp] > floor[lcp] {
        min[..=lcp].to_vec()
    } else {
        // min < floor: outside the maintained invariant. The full minimum is
        // always a correct (if untruncated) separator, so degrade gracefully
        // rather than misroute.
        debug_assert!(false, "reseparate invariant violated: min < floor");
        min.to_vec()
    }
}

/// Geometric distribution for computing node ranks.
pub mod geometric {
    use dialog_common::Blake3Hash;

    use super::Rank;

    /// The branch factor of the trees built from this distribution: the
    /// average number of children per node.
    pub const BRANCH_FACTOR: u64 = 254;

    /// Computes the rank of a node from its hash using a geometric distribution.
    pub fn rank(hash: &Blake3Hash) -> Rank {
        compute_geometric_rank(hash, BRANCH_FACTOR)
    }

    /// Compute the rank of a hash using a threshold-based geometric
    /// distribution.
    ///
    /// The first 8 bytes of the hash are interpreted as a little-endian `u64`
    /// prefix, uniformly distributed in `[0, u64::MAX]`. The rank is
    /// determined by how many geometrically decreasing thresholds
    /// (`u64::MAX / m`, `u64::MAX / m²`, ...) the prefix falls below:
    ///
    /// ```text
    ///   rank = 1  if  prefix >= threshold_1                (probability: 1 - 1/m)
    ///   rank = 2  if  threshold_2 <= prefix < threshold_1  (probability: 1/m - 1/m²)
    ///   rank = 3  if  threshold_3 <= prefix < threshold_2  (probability: 1/m² - 1/m³)
    ///   ...
    /// ```
    ///
    /// This gives an exact `1/m` split probability at each level, so the
    /// effective branch factor matches the declared one.
    ///
    /// The loop terminates on its own: integer division drives the threshold
    /// to zero after `floor(log_m(2^64))` steps, and no prefix is below zero,
    /// so ranks naturally top out at `floor(log_m(2^64)) + 1` (9 for m=254,
    /// enough for trees with ~10^19 entries).
    pub(crate) fn compute_geometric_rank(hash: &Blake3Hash, m: u64) -> Rank {
        debug_assert!(m >= 2, "branch factor must be at least 2, got {m}");

        // Destructuring the first 8 bytes of the (fixed-size) hash makes the
        // prefix extraction infallible: there is no slice conversion to fail.
        // Little-endian is an arbitrary but deterministic choice; uniformity
        // is unaffected, but the same bytes must always produce the same rank
        // for the tree structure to be consistent.
        let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = *hash.as_bytes();
        let prefix = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]);

        let mut rank: Rank = 1;
        let mut threshold = u64::MAX / m;

        while prefix < threshold {
            rank += 1;
            threshold /= m;
        }

        rank
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::geometric::compute_geometric_rank;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Fixed seed so the statistical tests are deterministic; the assertions
    /// then verify exact, reproducible outcomes rather than racing sigma
    /// tolerances against an unseeded RNG.
    fn test_rng() -> StdRng {
        StdRng::seed_from_u64(0x_D1A1_06DB)
    }

    fn hash_for(prefix: u64) -> Blake3Hash {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&prefix.to_le_bytes());
        // Bytes 8..32 must not affect the result; make them non-zero to
        // prove it.
        bytes[8..].fill(0xFF);
        Blake3Hash::from(bytes)
    }

    /// The threshold comparisons are exact, deterministic golden values: a
    /// prefix at a threshold stays at the lower rank (the comparison is
    /// strict), a prefix one below it is promoted, a prefix of zero falls
    /// through every nonzero threshold and a prefix of `u64::MAX` is never
    /// promoted.
    #[dialog_common::test]
    async fn it_has_correct_rank_boundaries() -> Result<()> {
        let factor = 254u64;

        // For m=254 the thresholds are u64::MAX / 254^k for k = 1..=8
        // (254^8 < 2^64), so the maximum rank is 9.
        assert_eq!(compute_geometric_rank(&hash_for(0), factor), 9);
        assert_eq!(compute_geometric_rank(&hash_for(u64::MAX), factor), 1);

        let threshold_1 = u64::MAX / factor;
        let threshold_2 = threshold_1 / factor;

        assert_eq!(compute_geometric_rank(&hash_for(threshold_1), factor), 1);
        assert_eq!(
            compute_geometric_rank(&hash_for(threshold_1 - 1), factor),
            2
        );
        assert_eq!(compute_geometric_rank(&hash_for(threshold_2), factor), 2);
        assert_eq!(
            compute_geometric_rank(&hash_for(threshold_2 - 1), factor),
            3
        );

        Ok(())
    }

    /// `P(rank >= 2)` must be approximately `1/m` so that segments average
    /// `m` entries.
    #[dialog_common::test]
    async fn it_splits_with_branch_factor_probability() -> Result<()> {
        let factor = 254u64;
        let rounds = 1_000_000u32;
        let mut rng = test_rng();

        let mut promoted = 0u32;
        for _ in 0..rounds {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            if compute_geometric_rank(&Blake3Hash::from(bytes), factor) >= 2 {
                promoted += 1;
            }
        }

        let p_promoted = f64::from(promoted) / f64::from(rounds);
        let expected_p = 1.0 / factor as f64;

        assert!(
            (p_promoted - expected_p).abs() / expected_p < 0.2,
            "P(rank >= 2) = {p_promoted:.6} should be close to 1/{factor} = {expected_p:.6}"
        );

        Ok(())
    }

    /// The promotion probability must also be `1/m` at every level above the
    /// first, i.e. `P(rank >= k+1 | rank >= k) ≈ 1/m`.
    ///
    /// This is the regression test for the bit-batch implementation this
    /// module replaced: there, batches straddling byte boundaries were
    /// zero-filled, which inflated the conditional promotion probabilities to
    /// 1/2, 1/4, 1/8 instead of 1/m, producing much taller trees whose upper
    /// levels averaged only 2-4 children.
    #[dialog_common::test]
    async fn it_has_geometric_promotion_at_every_level() -> Result<()> {
        let factor = 16u64;
        let rounds = 2_000_000u32;
        let mut rng = test_rng();

        let mut at_least = [0u32; 4];
        for _ in 0..rounds {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            let rank = compute_geometric_rank(&Blake3Hash::from(bytes), factor);
            for (level, count) in at_least.iter_mut().enumerate() {
                if rank >= (level + 1) as u64 {
                    *count += 1;
                }
            }
        }

        for level in 1..at_least.len() - 1 {
            let conditional = f64::from(at_least[level + 1]) / f64::from(at_least[level]);
            let expected = 1.0 / factor as f64;
            assert!(
                (conditional - expected).abs() / expected < 0.15,
                "promotion from rank {} to {} should happen with probability ~1/{factor}, got {conditional:.6}",
                level + 1,
                level + 2,
            );
        }

        Ok(())
    }

    /// The same hash must always produce the same rank, and only the first
    /// 8 bytes of the hash participate.
    #[dialog_common::test]
    async fn it_is_deterministic_and_uses_only_the_prefix() -> Result<()> {
        let mut rng = test_rng();
        for _ in 0..1000 {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            let hash = Blake3Hash::from(bytes);

            let rank = compute_geometric_rank(&hash, 254);
            assert_eq!(rank, compute_geometric_rank(&hash, 254));

            let mut tail_mutated = bytes;
            tail_mutated[8..].fill(0xAB);
            assert_eq!(
                rank,
                compute_geometric_rank(&Blake3Hash::from(tail_mutated), 254)
            );
        }

        Ok(())
    }
}
