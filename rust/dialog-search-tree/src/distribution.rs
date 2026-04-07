/// The rank of a node in the prolly tree.
pub type Rank = u32;

/// Geometric distribution for computing node ranks.
pub mod geometric {
    use dialog_common::Blake3Hash;
    use std::mem::size_of;

    use super::Rank;

    /// The branch factor of the [`Tree`]s that constitute [`Artifact`] indexes
    pub const BRANCH_FACTOR: u32 = 254;

    /// Compute the maximum rank derivable from a u64 hash prefix for a given
    /// branch factor Q. This is `floor(log_Q(2^64))` — the number of times we
    /// can divide `u64::MAX` by Q before the threshold reaches zero.
    ///
    /// For Q=254 this gives 8, supporting trees with up to ~10^19 entries.
    const fn max_rank_for(branch_factor: u32) -> Rank {
        let m = branch_factor as u64;
        let mut threshold = u64::MAX / m;
        let mut rank = 1;
        while threshold / m > 0 {
            threshold /= m;
            rank += 1;
        }
        rank
    }

    /// Computes the rank of a node from its hash using a geometric distribution.
    pub fn rank(hash: &Blake3Hash) -> Rank {
        compute_geometric_rank(hash, BRANCH_FACTOR)
    }

    /// Compute the rank of a hash using a threshold-based geometric
    /// distribution.
    ///
    /// The first set of bytes of the hash are interpreted as a little-endian `u64`
    /// prefix, uniformly distributed in `[0, u64::MAX]`. The rank is
    /// determined by how many geometrically decreasing thresholds
    /// (`u64::MAX / m`, `u64::MAX / m²`, ...) the prefix falls below.
    ///
    /// This gives an exact `1/m` split probability at each level, so the
    /// effective branch factor matches the declared one.
    pub(crate) fn compute_geometric_rank(hash: &Blake3Hash, m: u32) -> Rank {
        let bytes = hash.as_bytes();

        let prefix = u64::from_le_bytes(
            bytes[0..size_of::<u64>()]
                .try_into()
                .expect("hash must be at least u64 size"),
        );

        let mut rank: Rank = 1;
        let mut threshold = u64::MAX / u64::from(m);
        let max_rank = max_rank_for(m);

        while prefix < threshold && rank < max_rank {
            rank += 1;
            threshold /= u64::from(m);
        }

        rank
    }
}
