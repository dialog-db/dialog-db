//! RSA signature algorithm configuration.

#[cfg(feature = "rsa")]
use super::SignatureAlgorithm;
#[cfg(feature = "rsa")]
use super::hash::{Multihasher, Sha2_256};
#[cfg(feature = "rsa")]
use std::marker::PhantomData;

/// The RSA signature algorithm.
///
/// The `const L` type parameter represents the key length in bytes.
#[cfg(feature = "rsa")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Rsa<const L: usize, H: Multihasher>(PhantomData<H>);

/// The RS256 signature algorithm.
///
/// The `const L` type parameter represents the key length in bytes.
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
pub type Rs256<const L: usize> = Rsa<L, Sha2_256>;

#[cfg(feature = "rsa")]
impl SignatureAlgorithm for Rs256<256> {
    fn prefix(&self) -> u64 {
        0x1205
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0x12]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [0x1205, 0x12, 0x0100] {
            Some((Rsa(PhantomData), bytes.get(3..)?))
        } else {
            None
        }
    }
}

#[cfg(feature = "rsa")]
impl SignatureAlgorithm for Rs256<512> {
    fn prefix(&self) -> u64 {
        0x1205
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0x12]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [0x1205, 0x12, 0x0200] {
            Some((Rsa(PhantomData), bytes.get(3..)?))
        } else {
            None
        }
    }
}
