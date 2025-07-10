use dialog_common::{Blake3Hash, Blake3Hashed};

use crate::{KeyBuffer, Node, ValueBuffer};

use super::{Distribution, Rank};

/// Simulate a geometric distribution with probability p = 1 - (1 / m) using a
/// series of fair Bernoulli trials (p = 1 / 2). The number of trials is limited
/// to 256 independent trials.
///
/// https://textile.notion.site/Flipping-bits-and-coins-with-hashes-205770b56418498fba4fef8cb037412d
#[derive(Clone)]
pub struct GeometricDistribution;

impl<'a, const BRANCH_FACTOR: u32, Key, Value> Distribution<'a, BRANCH_FACTOR, Key, Value>
    for GeometricDistribution
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    fn rank(
        key: &'a Key::Ref,
        _value: &'a Value::Ref,
        _candidate: &'a Node<'a, Key, Value>,
    ) -> Rank {
        // TODO: Should be able to memoize this hash on the key
        compute_geometric_rank(key.hash(), BRANCH_FACTOR)
    }
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

#[cfg(test)]
mod tests {
    use super::compute_geometric_rank;
    use dialog_common::Blake3Hash;
    use rand::{Rng, thread_rng as rng};

    #[test]
    fn it_has_expected_distribution() {
        let factor = 64;
        let rounds = 500_000;

        let mut sum = 0u32;
        for _ in 0..rounds {
            let mut buffer = [0u8; 32];
            rng().fill(&mut buffer);
            sum += compute_geometric_rank(&Blake3Hash::hash(&buffer), factor);
        }
        let average = f64::from(sum) / f64::from(rounds);
        let probability = 1.0 - 1.0 / f64::from(factor);
        let expected = 1.0 / probability;
        println!("Average: {average}, expected: {expected}");

        assert!((average - expected).abs() < 0.01)
    }
}
