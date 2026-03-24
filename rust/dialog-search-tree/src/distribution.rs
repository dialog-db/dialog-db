/// The rank of a node in the prolly tree.
pub type Rank = u32;

/// Geometric distribution for computing node ranks.
pub mod geometric {
    use dialog_common::Blake3Hash;

    use super::Rank;

    /// The branch factor of the [`Tree`]s that constitute [`Artifact`] indexes
    pub const BRANCH_FACTOR: u32 = 254;

    /// Maximum rank that can be derived from a u64 hash prefix with branch
    /// factor 254.
    ///
    /// With a 64-bit prefix and branch factor Q, the maximum useful rank is
    /// `floor(log_Q(2^64))`. For Q=254 this gives 8 levels, supporting
    /// trees with up to ~10^19 entries.
    const MAX_RANK: Rank = 8;

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
    /// (`u64::MAX / m`, `u64::MAX / m²`, ...) the prefix falls below.
    ///
    /// This gives an exact `1/m` split probability at each level, so the
    /// effective branch factor matches the declared one.
    pub(crate) fn compute_geometric_rank(hash: &Blake3Hash, m: u32) -> Rank {
        let bytes = hash.as_bytes();

        let prefix = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .expect("hash must be at least 8 bytes"),
        );

        let mut rank: Rank = 1;
        let mut threshold = u64::MAX / u64::from(m);

        while prefix < threshold && rank < MAX_RANK {
            rank += 1;
            threshold /= u64::from(m);
        }

        rank
    }
}
