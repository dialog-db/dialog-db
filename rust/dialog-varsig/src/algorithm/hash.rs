//! Multihash algorithms.
//!
//! This is separate from the `multihash-codetable` crate
//! becuase we don't need any of the actual hashing functionality.

/// Multihash Prefix
pub trait Multihasher {
    /// Multihash tag for this hasher.
    const MULTIHASH_TAG: u64;
}

/// SHA2-256 hash algorithm.
#[cfg(feature = "sha2_256")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha2_256;

#[cfg(feature = "sha2_256")]
impl Multihasher for Sha2_256 {
    const MULTIHASH_TAG: u64 = 0x12;
}

/// SHA2-384 hash algorithm.
#[cfg(feature = "sha2_384")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha2_384;

#[cfg(feature = "sha2_384")]
impl Multihasher for Sha2_384 {
    const MULTIHASH_TAG: u64 = 0x15;
}

/// SHA2-512 hash algorithm.
#[cfg(feature = "sha2_512")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha2_512;

#[cfg(feature = "sha2_512")]
impl Multihasher for Sha2_512 {
    const MULTIHASH_TAG: u64 = 0x13;
}

/// Shake256 hash algorithm.
#[cfg(feature = "shake_256")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Shake256;

#[cfg(feature = "shake_256")]
impl Multihasher for Shake256 {
    const MULTIHASH_TAG: u64 = 0x19;
}

/// Blake2b hash algorithm.
#[cfg(feature = "blake2b")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Blake2b;

#[cfg(feature = "blake2b")]
impl Multihasher for Blake2b {
    const MULTIHASH_TAG: u64 = 0xb220;
}

/// Blake3 hash algorithm.
#[cfg(feature = "blake3")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Blake3;

#[cfg(feature = "blake3")]
impl Multihasher for Blake3 {
    const MULTIHASH_TAG: u64 = 0x1e;
}

/// Keccak256 hash algorithm.
#[cfg(feature = "keccak256")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Keccak256;

#[cfg(feature = "keccak256")]
impl Multihasher for Keccak256 {
    const MULTIHASH_TAG: u64 = 0x1b;
}

/// Keccak384 hash algorithm.
#[cfg(feature = "keccak384")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Keccak384;

#[cfg(feature = "keccak384")]
impl Multihasher for Keccak384 {
    const MULTIHASH_TAG: u64 = 0x1c;
}

/// Keccak512 hash algorithm.
#[cfg(feature = "keccak512")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Keccak512;

#[cfg(feature = "keccak512")]
impl Multihasher for Keccak512 {
    const MULTIHASH_TAG: u64 = 0x1d;
}

/// SHA3-256 hash algorithm.
#[cfg(feature = "sha3_256")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha3_256;

#[cfg(feature = "sha3_256")]
impl Multihasher for Sha3_256 {
    const MULTIHASH_TAG: u64 = 0x16;
}

/// SHA3-384 hash algorithm.
#[cfg(feature = "sha3_384")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha3_384;

#[cfg(feature = "sha3_384")]
impl Multihasher for Sha3_384 {
    const MULTIHASH_TAG: u64 = 0x15;
}

/// SHA3-512 hash algorithm.
#[cfg(feature = "sha3_512")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha3_512;

#[cfg(feature = "sha3_512")]
impl Multihasher for Sha3_512 {
    const MULTIHASH_TAG: u64 = 0x14;
}
