//! Concrete key and signing types for dialog-capability.
//!
//! This crate provides credential implementations that satisfy the
//! [`Principal`] and [`Issuer`] traits from `dialog-capability`.
//!
//! The crate's default identity types are algorithm-agnostic: [`Signer`],
//! [`Verifier`], and [`Signature`] are enums that hold whichever algorithm a
//! credential was created with. ed25519 is always available (the `ed25519`
//! feature, on by default); other algorithms are feature-gated and add arms to
//! those enums when enabled. Today the only additional algorithm is ES256 /
//! P-256 (the `es256` feature).
//!
//! [`Principal`]: dialog_capability::Principal
//! [`Issuer`]: dialog_capability::Issuer

pub mod credential;
pub mod key;
pub mod signature;

#[cfg(feature = "ed25519")]
pub mod ed25519;
#[cfg(feature = "ed25519")]
pub use ed25519::*;

#[cfg(feature = "es256")]
pub mod es256;
#[cfg(feature = "es256")]
pub use es256::*;

// The algorithm-agnostic identity types. Available whenever ed25519 is (the
// always-on default); enabling further algorithms adds arms to the enums.
#[cfg(feature = "ed25519")]
pub use signature::{Algorithm, AlgorithmTag, DidFromStrError, Signature, Signer, Verifier};

pub use credential::*;
