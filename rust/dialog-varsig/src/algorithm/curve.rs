//! This module defines various elliptic curves headers.

/// `secp256k1` curve
#[cfg(feature = "secp256k1")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Secp256k1;

/// `secp256r1` curve
#[cfg(feature = "secp256r1")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Secp256r1;

/// NIST alias for the `secp256r1` curve
#[cfg(feature = "secp256r1")]
pub type P256 = Secp256r1;

/// `secp384r1` curve
#[cfg(feature = "secp384r1")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Secp384r1;

/// NIST alias for the `secp384r1` curve
#[cfg(feature = "secp384r1")]
pub type P384 = Secp384r1;

/// `secp521r1` curve
#[cfg(feature = "secp521r1")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Secp521r1;

/// NIST alias for the `secp521r1` curve
#[cfg(feature = "secp521r1")]
pub type P521 = Secp521r1;

/// The Twisted Edwards 25519 curve
#[cfg(feature = "edwards25519")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Edwards25519;

/// The Twisted Edwards 448 curve
#[cfg(feature = "edwards448")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Edwards448;
