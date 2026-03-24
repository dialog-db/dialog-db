use dialog_storage::HashType;

use crate::KeyType;

use super::{Distribution, Rank};

/// Maximum rank that can be derived from a u64 hash prefix with branch factor 254.
///
/// With a 64-bit prefix and branch factor Q, the maximum useful rank is
/// `floor(log_Q(2^64))`. For Q=254:
///   - 254^1  ≈ 2.5 × 10^2   (rank 2 threshold)
///   - 254^2  ≈ 6.5 × 10^4   (rank 3 threshold)
///   - 254^7  ≈ 7.2 × 10^16  (rank 8 threshold)
///   - 254^8  ≈ 1.8 × 10^19  > 2^64, so rank 8 is the max
///
/// This supports trees with up to ~10^19 entries, far exceeding practical needs.
const MAX_RANK: Rank = 8;

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
/// # Why this is better than the bit-batch approach
///
/// The previous implementation grouped hash bits into k-bit batches where
/// `k = ceil(log₂(BRANCH_FACTOR))`. For BRANCH_FACTOR=254, this gave k=7,
/// making each trial probability `1/2^7 = 1/128` — not `1/254`. The effective
/// branch factor was ~128, not the declared 254.
///
/// The threshold approach gives an **exact** `1/Q` probability at each level
/// (up to integer rounding of `u64::MAX / Q`, which is negligible for any
/// practical branch factor).
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
/// Using a u64 prefix (8 bytes of hash) provides depth capacity of
/// `floor(log_Q(2^64))`. At Q=254 this gives 8 levels, supporting
/// trees with up to ~10^19 entries. If deeper trees were ever needed,
/// the prefix could be extended to u128 (16 bytes → 16 levels).
#[derive(Clone, Debug)]
pub struct GeometricDistribution;

impl<Key, Hash> Distribution<Key, Hash> for GeometricDistribution
where
    Key: KeyType,
    Hash: HashType,
{
    const BRANCH_FACTOR: u32 = 254;

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
/// 1. Extract the first 8 bytes of `bytes` as a little-endian `u64` prefix.
///    Since Blake3 output is uniformly distributed, this prefix is uniformly
///    distributed in `[0, u64::MAX]`.
///
/// 2. Initialize `threshold = u64::MAX / m` and `rank = 1`.
///
/// 3. While `prefix < threshold` and `rank < MAX_RANK`:
///    - Increment rank (the prefix fell below this level's threshold).
///    - Divide threshold by `m` for the next level.
///
/// The probability of each successive rank level is exactly `1/m` of the
/// previous, giving a geometric distribution with parameter `p = 1/m`.
///
/// # Arguments
///
/// * `bytes` - A hash digest (at least 8 bytes). Only the first 8 bytes are used.
/// * `m`     - The branch factor. Must be >= 2. Determines the average number
///             of entries per tree node.
///
/// # Returns
///
/// A rank in `[1, MAX_RANK + 1]`. Rank 1 is the most common (probability
/// `1 - 1/m`), and each successive rank is `m` times rarer.
pub(crate) fn compute_geometric_rank(bytes: &[u8], m: u32) -> Rank {
    // Extract the first 8 bytes of the hash as a u64 prefix.
    // Little-endian is used for consistency with the hash output byte order
    // (the choice of endianness doesn't affect uniformity, but must be
    // deterministic for consistent tree structure).
    let prefix = u64::from_le_bytes(
        bytes[0..8]
            .try_into()
            .expect("hash must be at least 8 bytes"),
    );

    // Start with rank 1 (the "base" level — most keys land here).
    let mut rank: Rank = 1;

    // The first threshold: prefix values below this represent the ~1/m fraction
    // of keys that should be promoted to rank 2 or higher.
    let mut threshold = u64::MAX / u64::from(m);

    // Each iteration checks if the prefix falls below the current threshold.
    // If so, the key is "promoted" to a higher rank, and the threshold is
    // divided by m again for the next level. This creates a geometric
    // distribution: P(rank >= k) ≈ 1/m^(k-1).
    while prefix < threshold && rank < MAX_RANK {
        rank += 1;
        threshold /= u64::from(m);
    }

    rank
}

#[cfg(test)]
mod tests {
    use super::{MAX_RANK, compute_geometric_rank};
    use rand::{Rng, thread_rng as rng};

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
        let factor = 64;
        let rounds = 500_000;

        let mut sum = 0u32;
        for _ in 0..rounds {
            let mut buffer = [0u8; 32];
            rng().fill(&mut buffer);
            sum += compute_geometric_rank(&buffer, factor);
        }
        let average = f64::from(sum) / f64::from(rounds);
        let probability = 1.0 - 1.0 / f64::from(factor);
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
        let factor = 254u32;
        let rounds = 1_000_000u32;

        let mut rank_counts = vec![0u32; (MAX_RANK + 2) as usize];
        for _ in 0..rounds {
            let mut buffer = [0u8; 32];
            rng().fill(&mut buffer);
            let rank = compute_geometric_rank(&buffer, factor);
            rank_counts[rank as usize] += 1;
        }

        // P(rank >= 2) should be approximately 1/254
        let promoted: u32 = rank_counts[2..].iter().sum();
        let p_promoted = f64::from(promoted) / f64::from(rounds);
        let expected_p = 1.0 / f64::from(factor);

        println!("P(rank >= 2) = {p_promoted:.6}, expected ≈ {expected_p:.6}");
        println!("Rank distribution (first 5 levels):");
        for (rank, &count) in rank_counts.iter().enumerate().take(5) {
            let p = f64::from(count) / f64::from(rounds);
            println!("  rank {rank}: {count} ({p:.6})");
        }

        // Allow 20% relative tolerance for statistical variation
        assert!(
            (p_promoted - expected_p).abs() / expected_p < 0.2,
            "P(rank >= 2) = {p_promoted:.6} should be close to 1/{factor} = {expected_p:.6}"
        );
    }

    /// Verify that the threshold approach gives correct rank boundaries.
    ///
    /// A prefix of 0 should yield the maximum rank (it's below all thresholds),
    /// and a prefix of u64::MAX should yield rank 1 (it's above all thresholds).
    #[test]
    fn it_has_correct_rank_boundaries() {
        let factor = 254u32;

        // All-zero hash prefix → minimum possible prefix → maximum rank.
        // The loop exits when rank reaches MAX_RANK, so max possible rank = MAX_RANK.
        let min_prefix = [0u8; 32];
        let rank = compute_geometric_rank(&min_prefix, factor);
        assert_eq!(
            rank, MAX_RANK,
            "all-zero prefix should reach MAX_RANK = {}",
            MAX_RANK
        );

        // All-0xFF hash prefix → maximum possible prefix → rank 1
        let max_prefix = [0xFF; 32];
        let rank = compute_geometric_rank(&max_prefix, factor);
        assert_eq!(rank, 1, "all-0xFF prefix should be rank 1");

        // A prefix just below threshold_1 should be rank 2
        let threshold_1 = u64::MAX / u64::from(factor);
        let just_below = (threshold_1 - 1).to_le_bytes();
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&just_below);
        // Fill the rest with 0xFF to not affect the result
        buf[8..].fill(0xFF);
        let rank = compute_geometric_rank(&buf, factor);
        assert!(rank >= 2, "prefix just below threshold should be rank >= 2");

        // A prefix just above threshold_1 should be rank 1
        let just_above = (threshold_1 + 1).to_le_bytes();
        buf[0..8].copy_from_slice(&just_above);
        let rank = compute_geometric_rank(&buf, factor);
        assert_eq!(rank, 1, "prefix just above threshold should be rank 1");
    }

    /// Verify that the distribution works correctly for various branch factors,
    /// ensuring the algorithm generalizes beyond the production value of 254.
    #[test]
    fn it_works_for_various_branch_factors() {
        let rounds = 200_000u32;

        for factor in [4u32, 16, 32, 128, 254, 1000] {
            let mut promoted = 0u32;
            for _ in 0..rounds {
                let mut buffer = [0u8; 32];
                rng().fill(&mut buffer);
                let rank = compute_geometric_rank(&buffer, factor);
                if rank >= 2 {
                    promoted += 1;
                }
            }
            let p_promoted = f64::from(promoted) / f64::from(rounds);
            let expected_p = 1.0 / f64::from(factor);

            println!("factor={factor}: P(rank>=2) = {p_promoted:.6}, expected = {expected_p:.6}");

            // Allow 25% relative tolerance for statistical variation
            assert!(
                (p_promoted - expected_p).abs() / expected_p < 0.25,
                "factor={factor}: P(rank>=2) = {p_promoted:.6}, expected = {expected_p:.6}"
            );
        }
    }
}
