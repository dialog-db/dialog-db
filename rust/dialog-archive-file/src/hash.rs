//! A 64-bit hash of bytes whose output this crate defines, so the index a
//! file keeps on disk reads the same in every process and every release.
//!
//! It names keys in the file's index (a token's bytes, a node's lineage),
//! where a collision costs a lookup and never a wrong answer: every value an
//! index key yields is checked against the bytes it stands for.

const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const MULTIPLY: u64 = 0xBF58_476D_1CE4_E5B9;

/// Mixes the bits of `value` (the finalizer of SplitMix64).
fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(MULTIPLY);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// The hash of `bytes`: eight bytes at a time, little-endian, each word
/// mixed into the state, then the length.
pub(crate) fn hash(bytes: &[u8]) -> u64 {
    let mut state = SEED;
    let (words, rest) = bytes.as_chunks::<8>();
    for word in words {
        state = mix(state ^ u64::from_le_bytes(*word)).wrapping_add(SEED);
    }
    if !rest.is_empty() {
        let mut word = [0u8; 8];
        word[..rest.len()].copy_from_slice(rest);
        state = mix(state ^ u64::from_le_bytes(word)).wrapping_add(SEED);
    }
    mix(state ^ bytes.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hash is part of the file format: these values must never
    /// change, or a file's index stops finding what it holds.
    #[test]
    fn it_hashes_as_the_format_says() {
        // Computed independently of this code, from the definition above.
        assert_eq!(hash(b""), 0xE220_A839_7B1D_CDAF);
        assert_eq!(hash(b"a"), 0xC503_7D9B_6EB8_DA20);
        assert_eq!(hash(b"mb.artist/name"), 0xD555_3284_2966_42DE);
    }

    #[test]
    fn it_tells_lengths_and_trailing_zeros_apart() {
        assert_ne!(hash(b"a"), hash(b"a\0"));
        assert_ne!(hash(&[0; 8]), hash(&[0; 16]));
    }
}
