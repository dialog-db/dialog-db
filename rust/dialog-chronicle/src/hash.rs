//! A 64-bit hash of bytes whose output this crate defines, so the index a
//! file keeps on disk reads the same in every process and every release.
//!
//! It names keys in the file's index (a token's bytes, a node's lineage),
//! where a collision costs a lookup and never a wrong answer: every value an
//! index key yields is checked against the bytes it stands for.

const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const P0: u64 = 0x2D35_8DCC_AA6C_78A5;
const P1: u64 = 0x8BB8_4B93_962E_ACC9;
const P2: u64 = 0x4B33_A62E_D433_D4A3;

/// The two halves of the 128-bit product of `a` and `b`, xored.
fn fold(a: u64, b: u64) -> u64 {
    let product = u128::from(a) * u128::from(b);
    product as u64 ^ (product >> 64) as u64
}

/// Folds sixteen bytes, two little-endian words, into `state`.
fn absorb(state: u64, block: &[u8; 16]) -> u64 {
    let (low, high) = block.split_at(8);
    let low = u64::from_le_bytes(low.try_into().expect("eight bytes"));
    let high = u64::from_le_bytes(high.try_into().expect("eight bytes"));
    fold(low ^ P1, high ^ state)
}

/// The hash of `bytes`: sixteen bytes at a time, each block folded into
/// the state by one wide multiply, the last block padded with zeros, then
/// the length.
pub(crate) fn hash(bytes: &[u8]) -> u64 {
    let mut state = SEED;
    let (blocks, rest) = bytes.as_chunks::<16>();
    for block in blocks {
        state = absorb(state, block);
    }
    if !rest.is_empty() {
        let mut block = [0u8; 16];
        block[..rest.len()].copy_from_slice(rest);
        state = absorb(state, &block);
    }
    fold(state ^ P2, bytes.len() as u64 ^ P0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hash is part of the file format: these values must never
    /// change, or a file's index stops finding what it holds.
    #[dialog_common::test]
    fn it_hashes_as_the_format_says() {
        // Computed independently of this code, from the definition above.
        assert_eq!(hash(b""), 0x16C5_6ECB_6524_155D);
        assert_eq!(hash(b"a"), 0xBD22_77B2_7BBC_20D3);
        assert_eq!(hash(b"mb.artist/name"), 0x31F8_8A83_EE29_8CB1);
    }

    #[dialog_common::test]
    fn it_tells_lengths_and_trailing_zeros_apart() {
        assert_ne!(hash(b"a"), hash(b"a\0"));
        assert_ne!(hash(&[0; 8]), hash(&[0; 16]));
    }
}
