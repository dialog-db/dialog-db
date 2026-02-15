//! Signature algorithm configuration.

pub mod curve;
pub mod ecdsa;
pub mod eddsa;
pub mod hash;
pub mod rsa;
pub mod webauthn;

use std::fmt::Debug;

/// Describes a signature algorithm as multicodec tags.
///
/// This trait captures the metadata needed to encode/decode a signature
/// algorithm in a Varsig header (prefix tag, config tags, reconstruction
/// from tags). It does NOT know about signers, verifiers, or signature types.
pub trait SignatureAlgorithm: Sized + Debug + Default + Clone {
    /// The prefix for the signature type.
    ///
    /// For example, `EdDSA` would be `0xED`.
    fn prefix(&self) -> u64;

    /// The configuration as a series of [`u64`] tags.
    ///
    /// NOTE: these will be automatically converted to LEB128 by the serializer.
    fn config_tags(&self) -> Vec<u64>;

    /// Try to create a codec from a series of bytes.
    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])>;
}
