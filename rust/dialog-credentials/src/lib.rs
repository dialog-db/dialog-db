//! Concrete key and signing types for dialog-capability.
//!
//! This crate provides credential implementations that satisfy the
//! [`Principal`] and [`Issuer`] traits from `dialog-capability`.
//!
//! Ed25519 (`ed25519` feature) and ES256 / P-256 (`es256` feature) are both
//! available and on by default. The algorithm-agnostic [`AnySigner`],
//! [`AnyVerifier`], and [`AnySignature`] enums let callers hold an identity
//! without committing to a signature algorithm.
//!
//! [`Principal`]: dialog_capability::Principal
//! [`Issuer`]: dialog_capability::Issuer

pub mod credential;
pub mod key;

#[cfg(feature = "ed25519")]
pub mod ed25519;
#[cfg(feature = "ed25519")]
pub use ed25519::*;

#[cfg(feature = "es256")]
pub mod es256;
#[cfg(feature = "es256")]
pub use es256::*;

// The algorithm-agnostic `Any*` types span both supported algorithms, so they
// require both features. Both are on by default.
#[cfg(all(feature = "ed25519", feature = "es256"))]
pub mod any;
#[cfg(all(feature = "ed25519", feature = "es256"))]
pub use any::*;

pub use credential::*;
