use dialog_storage::HashType;

use crate::KeyType;

use super::{Distribution, Rank};

/// Simulate a geometric distribution with probability p = 1 - (1 / m) using a
/// series of fair Bernoulli trials (p = 1 / 2). The number of trials is limited
/// to 256 independent trials.
///
/// https://textile.notion.site/Flipping-bits-and-coins-with-hashes-205770b56418498fba4fef8cb037412d
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

pub(crate) fn compute_geometric_rank(bytes: &[u8], m: u32) -> Rank {
    // Convert the series of fair trials into a series with desired probability
    // Since we start with a random 256-bit slice (which can be thought of as a
    // series of 256 fair Bernoulli trials), we need to group these trials with
    // p = 1 / 2 into trials with p = 1 / m.
    //
    // To simulate a trial with probability p = 1 / m, consider a group of k
    // fair trials, where k is chosen such that 1 / 2^k ≈ 1 / m. The smallest k
    // such that 2^k ≥ m will be k = ⌈log_2(m)⌉. Compute ⌈log_2(m)⌉ =
    // ceil(log_2(m)).

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

#[cfg(test)]
mod tests {
    use super::compute_geometric_rank;
    use rand::{Rng, thread_rng as rng};

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

        assert!((average - expected).abs() < 0.01)
    }
}
