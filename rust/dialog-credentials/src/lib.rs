//! Concrete key and signing types for dialog-capability.
//!
//! This crate provides credential implementations that satisfy the
//! [`Principal`] and [`Authority`] traits from `dialog-capability`.
//!
//! Currently the only implementation is Ed25519 (enabled by the `ed25519`
//! feature, which is on by default).
//!
//! [`Principal`]: dialog_capability::Principal
//! [`Authority`]: dialog_capability::Authority

pub mod key;

#[cfg(feature = "ed25519")]
pub mod ed25519;
#[cfg(feature = "ed25519")]
pub use ed25519::*;
