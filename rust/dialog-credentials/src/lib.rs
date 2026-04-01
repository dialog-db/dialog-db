//! Concrete key and signing types for dialog-capability.
//!
//! This crate provides credential implementations that satisfy the
//! [`Principal`] and [`Issuer`] traits from `dialog-capability`.
//!
//! Currently the only implementation is Ed25519 (enabled by the `ed25519`
//! feature, which is on by default).
//!
//! [`Principal`]: dialog_capability::Principal
//! [`Issuer`]: dialog_capability::Issuer

pub mod credential;
pub mod key;

#[cfg(feature = "ed25519")]
pub mod ed25519;
#[cfg(feature = "ed25519")]
pub use ed25519::*;

pub use credential::*;
