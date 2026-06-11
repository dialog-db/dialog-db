use crate::KeyType;
use dialog_storage::HashType;

use super::{Distribution, Rank};

/// Implements a geometric distribution for prolly tree chunk boundaries using
/// a threshold-based approach on the hash prefix.
///
/// # How it works
///
/// Each key is hashed with Blake3, and the first 8 bytes of the hash are
/// interpreted as a little-endian `u64` value (the "prefix"). This prefix is
/// uniformly distributed in `[0, u64::MAX]`.
///
/// To determine the rank, we use a decreasing sequence of thresholds:
///
/// ```text
///   threshold_1 = u64::MAX / BRANCH_FACTOR
///   threshold_2 = u64::MAX / BRANCH_FACTOR²
///   threshold_3 = u64::MAX / BRANCH_FACTOR³
///   ...
/// ```
///
/// The rank is the number of thresholds the prefix falls below, plus one:
///
/// ```text
///   rank = 1  if  prefix >= threshold_1           (probability: 1 - 1/Q)
///   rank = 2  if  threshold_2 <= prefix < threshold_1  (probability: 1/Q - 1/Q²)
///   rank = 3  if  threshold_3 <= prefix < threshold_2  (probability: 1/Q² - 1/Q³)
///   ...
/// ```
///
/// # Probability analysis
///
/// For a given branch factor Q:
///
/// - `P(rank >= 2) = P(prefix < u64::MAX / Q) ≈ 1/Q` — exactly the desired
///   split probability for average chunk size Q.
/// - `P(rank >= k) ≈ 1/Q^(k-1)` — exponential decay ensures higher levels
///   are exponentially rarer, giving O(log_Q(N)) tree height for N entries.
///
/// # Depth capacity
///
/// Repeated integer division by Q reaches zero after `floor(log_Q(2^64))`
/// steps, so ranks naturally top out at `floor(log_Q(2^64)) + 1`; no explicit
/// cap is needed. At Q=254 the maximum rank is 9, supporting trees with up to
/// ~10^19 entries. If deeper trees were ever needed, the prefix could be
/// extended to u128 (16 bytes).
#[derive(Clone, Debug)]
pub struct GeometricDistribution;

impl<Key, Hash> Distribution<Key, Hash> for GeometricDistribution
where
    Key: KeyType,
    Hash: HashType,
{
    const BRANCH_FACTOR: u64 = 254;

    fn rank(key: &Key) -> Rank {
        let key_hash = blake3::hash(key.bytes());
        compute_geometric_rank(
            key_hash.as_bytes(),
            <GeometricDistribution as Distribution<Key, Hash>>::BRANCH_FACTOR,
        )
    }
}

/// Compute the rank of a hash using the threshold-based geometric distribution.
///
/// # Algorithm
///
/// 1. Extract the first 8 bytes of `hash` as a little-endian `u64` prefix.
///    Since Blake3 output is uniformly distributed, this prefix is uniformly
///    distributed in `[0, u64::MAX]`.
///
/// 2. Initialize `threshold = u64::MAX / m` and `rank = 1`.
///
/// 3. While `prefix < threshold`:
///    - Increment rank (the prefix fell below this level's threshold).
///    - Divide threshold by `m` for the next level.
///
/// The loop terminates on its own: integer division drives the threshold to
/// zero after `floor(log_m(2^64))` steps, and no prefix is below zero.
///
/// The probability of each successive rank level is `1/m` of the previous,
/// giving a geometric distribution with parameter `p = 1/m`. Rank 1 is the
/// most common (probability `1 - 1/m`), and each successive rank is `m`
/// times rarer.
pub(crate) fn compute_geometric_rank(hash: &[u8; 32], m: u64) -> Rank {
    debug_assert!(m >= 2, "branch factor must be at least 2, got {m}");

    // Destructuring the first 8 bytes of the (fixed-size) hash makes the
    // prefix extraction infallible: there is no slice conversion to fail.
    // Little-endian is an arbitrary but deterministic choice; uniformity is
    // unaffected, but the same bytes must always produce the same rank for
    // the tree structure to be consistent.
    let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = *hash;
    let prefix = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]);

    // Start with rank 1 (the "base" level — most keys land here).
    let mut rank: Rank = 1;

    // The first threshold: prefix values below this represent the ~1/m
    // fraction of keys that should be promoted to rank 2 or higher.
    let mut threshold = u64::MAX / m;

    // Each iteration checks if the prefix falls below the current threshold.
    // If so, the key is "promoted" to a higher rank, and the threshold is
    // divided by m again for the next level. This creates a geometric
    // distribution: P(rank >= k) ≈ 1/m^(k-1).
    while prefix < threshold {
        rank += 1;
        threshold /= m;
    }

    rank
}

#[cfg(test)]
mod tests {
    use super::compute_geometric_rank;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    /// Fixed seed so the statistical tests are deterministic; the assertions
    /// then verify exact, reproducible outcomes rather than racing sigma
    /// tolerances against an unseeded RNG.
    fn test_rng() -> StdRng {
        StdRng::seed_from_u64(0x_D1A1_06DB)
    }

    /// Verify that the distribution matches theoretical expectations.
    ///
    /// For a geometric distribution with "success" probability p = 1 - 1/m
    /// (where "success" means stopping at the current rank), the expected
    /// mean is `1 / (1 - 1/m) = m / (m - 1)`.
    ///
    /// For m=254: expected mean ≈ 254/253 ≈ 1.00395
    /// For m=64:  expected mean ≈ 64/63  ≈ 1.01587
    #[test]
    fn it_has_expected_distribution() {
        let factor = 64u64;
        let rounds = 500_000u32;
        let mut rng = test_rng();

        let mut sum = 0u64;
        for _ in 0..rounds {
            let mut buffer = [0u8; 32];
            rng.fill(&mut buffer);
            sum += compute_geometric_rank(&buffer, factor);
        }
        let average = sum as f64 / f64::from(rounds);
        let probability = 1.0 - 1.0 / factor as f64;
        let expected = 1.0 / probability;
        println!("Average: {average}, expected: {expected}");

        assert!(
            (average - expected).abs() < 0.02,
            "Average {average} should be close to expected {expected}"
        );
    }

    /// Verify exact probability of each rank level for the production branch factor.
    ///
    /// With BRANCH_FACTOR=254, we expect:
    ///   P(rank = 1) ≈ 253/254 ≈ 0.99606
    ///   P(rank = 2) ≈ 1/254 - 1/254² ≈ 0.003922
    ///   P(rank >= 2) ≈ 1/254 ≈ 0.003937
    ///
    /// This test empirically validates these probabilities with 1M samples.
    #[test]
    fn it_has_exact_branch_factor_probability() {
        let factor = 254u64;
        let rounds = 1_000_000u32;
        let mut rng = test_rng();

        let mut promoted = 0u32;
        for _ in 0..rounds {
            let mut buffer = [0u8; 32];
            rng.fill(&mut buffer);
            if compute_geometric_rank(&buffer, factor) >= 2 {
                promoted += 1;
            }
        }

        // P(rank >= 2) should be approximately 1/254
        let p_promoted = f64::from(promoted) / f64::from(rounds);
        let expected_p = 1.0 / factor as f64;

        println!("P(rank >= 2) = {p_promoted:.6}, expected ≈ {expected_p:.6}");

        assert!(
            (p_promoted - expected_p).abs() / expected_p < 0.2,
            "P(rank >= 2) = {p_promoted:.6} should be close to 1/{factor} = {expected_p:.6}"
        );
    }

    /// Verify the promotion probability is also `1/m` at every level above
    /// the first, i.e. `P(rank >= k+1 | rank >= k) ≈ 1/m`.
    ///
    /// This is the regression test for the bit-batch implementation this
    /// module replaced: there, batches straddling byte boundaries were
    /// zero-filled, which inflated the conditional promotion probabilities
    /// to 1/2, 1/4, 1/8 instead of 1/m, producing much taller trees whose
    /// upper levels averaged only 2-4 children.
    #[test]
    fn it_has_geometric_promotion_at_every_level() {
        let factor = 16u64;
        let rounds = 2_000_000u32;
        let mut rng = test_rng();

        let mut at_least = [0u32; 4];
        for _ in 0..rounds {
            let mut buffer = [0u8; 32];
            rng.fill(&mut buffer);
            let rank = compute_geometric_rank(&buffer, factor);
            for (level, count) in at_least.iter_mut().enumerate() {
                if rank >= (level + 1) as u64 {
                    *count += 1;
                }
            }
        }

        // Conditional promotion probability between consecutive levels.
        for level in 1..at_least.len() - 1 {
            let conditional = f64::from(at_least[level + 1]) / f64::from(at_least[level]);
            let expected = 1.0 / factor as f64;
            println!(
                "P(rank >= {} | rank >= {}) = {conditional:.6}, expected ≈ {expected:.6}",
                level + 2,
                level + 1,
            );
            assert!(
                (conditional - expected).abs() / expected < 0.15,
                "promotion from rank {} to {} should happen with probability ~1/{factor}, got {conditional:.6}",
                level + 1,
                level + 2,
            );
        }
    }

    /// Verify that the threshold approach gives exact rank boundaries.
    ///
    /// These are deterministic golden values: a prefix exactly at a threshold
    /// stays at the lower rank (the comparison is strict), a prefix one below
    /// it is promoted, a prefix of zero falls through every nonzero threshold,
    /// and a prefix of `u64::MAX` is never promoted.
    #[test]
    fn it_has_correct_rank_boundaries() {
        let factor = 254u64;

        let buffer_for = |prefix: u64| {
            let mut buffer = [0u8; 32];
            buffer[0..8].copy_from_slice(&prefix.to_le_bytes());
            // Bytes 8..32 must not affect the result; make them non-zero to
            // prove it.
            buffer[8..].fill(0xFF);
            buffer
        };

        // All-zero prefix falls through every nonzero threshold. For m=254
        // the thresholds are u64::MAX / 254^k for k = 1..=8 (254^8 < 2^64),
        // so the maximum rank is 9.
        assert_eq!(compute_geometric_rank(&buffer_for(0), factor), 9);

        // Maximum prefix is above every threshold.
        assert_eq!(compute_geometric_rank(&buffer_for(u64::MAX), factor), 1);

        let threshold_1 = u64::MAX / factor;
        let threshold_2 = threshold_1 / factor;

        // The comparison is strict: a prefix equal to the threshold is NOT
        // promoted.
        assert_eq!(compute_geometric_rank(&buffer_for(threshold_1), factor), 1);
        // One below the first threshold is promoted exactly once.
        assert_eq!(
            compute_geometric_rank(&buffer_for(threshold_1 - 1), factor),
            2
        );
        // The same boundary behavior holds at the second level.
        assert_eq!(compute_geometric_rank(&buffer_for(threshold_2), factor), 2);
        assert_eq!(
            compute_geometric_rank(&buffer_for(threshold_2 - 1), factor),
            3
        );
    }

    /// Verify that the same hash always produces the same rank, and that
    /// only the first 8 bytes of the hash participate.
    #[test]
    fn it_is_deterministic_and_uses_only_the_prefix() {
        let mut rng = test_rng();
        for _ in 0..1000 {
            let mut buffer = [0u8; 32];
            rng.fill(&mut buffer);

            let rank = compute_geometric_rank(&buffer, 254);
            assert_eq!(rank, compute_geometric_rank(&buffer, 254));

            // Mutating the tail must not change the rank.
            let mut tail_mutated = buffer;
            tail_mutated[8..].fill(0xAB);
            assert_eq!(rank, compute_geometric_rank(&tail_mutated, 254));
        }
    }

    /// Verify that the distribution works correctly for various branch factors,
    /// ensuring the algorithm generalizes beyond the production value of 254.
    #[test]
    fn it_works_for_various_branch_factors() {
        let rounds = 200_000u32;

        for factor in [4u64, 16, 32, 128, 254, 1000] {
            let mut rng = test_rng();
            let mut promoted = 0u32;
            for _ in 0..rounds {
                let mut buffer = [0u8; 32];
                rng.fill(&mut buffer);
                let rank = compute_geometric_rank(&buffer, factor);
                if rank >= 2 {
                    promoted += 1;
                }
            }
            let p_promoted = f64::from(promoted) / f64::from(rounds);
            let expected_p = 1.0 / factor as f64;

            println!("factor={factor}: P(rank>=2) = {p_promoted:.6}, expected = {expected_p:.6}");

            assert!(
                (p_promoted - expected_p).abs() / expected_p < 0.25,
                "factor={factor}: P(rank>=2) = {p_promoted:.6}, expected = {expected_p:.6}"
            );
        }
    }
}
