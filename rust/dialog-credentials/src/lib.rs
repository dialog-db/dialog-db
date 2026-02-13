//! Concrete key and signing types for dialog-capability.
//!
//! This crate provides credential implementations that satisfy the
//! [`Principal`] and [`Authority`] traits from `dialog-capability`.
//!
//! Implementations:
//! - **Ed25519** (enabled by the `ed25519` feature, on by default)
//! - **WebAuthn P-256** (enabled by the `webauthn` feature)
//!
//! [`Principal`]: dialog_capability::Principal
//! [`Authority`]: dialog_capability::Authority

pub mod key;

#[cfg(feature = "ed25519")]
pub mod ed25519;
#[cfg(feature = "ed25519")]
pub use ed25519::*;

#[cfg(feature = "webauthn")]
pub mod webauthn;
