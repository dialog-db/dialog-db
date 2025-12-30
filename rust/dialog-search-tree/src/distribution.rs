/// The rank of a node in the prolly tree.
pub type Rank = u32;

/// Geometric distribution for computing node ranks.
pub mod geometric {
    use dialog_common::Blake3Hash;

    use super::Rank;

    /// The branch factor of the [`Tree`]s that constitute [`Artifact`] indexes
    pub const BRANCH_FACTOR: u32 = 254;

    /// Computes the rank of a node from its hash using a geometric distribution.
    pub fn rank(hash: &Blake3Hash) -> Rank {
        compute_geometric_rank(hash, BRANCH_FACTOR)
    }

    pub(crate) fn compute_geometric_rank(hash: &Blake3Hash, m: u32) -> Rank {
        // Convert the series of fair trials into a series with desired probability
        // Since we start with a random 256-bit slice (which can be thought of as a
        // series of 256 fair Bernoulli trials), we need to group these trials with
        // p = 1 / 2 into trials with p = 1 / m.
        //
        // To simulate a trial with probability p = 1 / m, consider a group of k
        // fair trials, where k is chosen such that 1 / 2^k ≈ 1 / m. The smallest k
        // such that 2^k ≥ m will be k = ⌈log_2(m)⌉. Compute ⌈log_2(m)⌉ =
        // ceil(log_2(m)).
        let bytes = hash.bytes();

        let k = (m + 1).ilog2();
        // Number of batches  of k bits
        let batch_count = 256 / k;
        // Mask to extract k bits
        let mask = (1u8 << k) - 1;
        // For each batch of k bits, we treat the batch as a "success" if all bits
        // are 0 (which happens with probability 1 / 2^k). The number of batches
        // until the first "success" is the desired geometrically distributed random
        // variable.
        for i in 0..batch_count {
            let byte_index = (k * i) / 8;
            let bit_index = (k * i) % 8;
            // Extract k bits
            let batch = (bytes[byte_index as usize] >> bit_index) & mask;
            // batch != 0 means we are looking for the failure probability 1 / m
            // whereas batch == 0 means we are looking for the success probability
            // 1 / m
            if batch != 0 {
                return i + 1; // +1 because geometric distribution starts at 1
            }
        }
        batch_count + 1
    }
}
