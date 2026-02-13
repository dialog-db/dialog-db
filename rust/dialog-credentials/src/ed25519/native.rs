//! Native Ed25519 signing implementation using `ed25519_dalek`.
//!
//! This module provides Ed25519 signing for non-WASM platforms using
//! the `ed25519_dalek` crate.

/// Native Ed25519 signing key.
///
/// This is a type alias to `ed25519_dalek::SigningKey`.
pub type SigningKey = ed25519_dalek::SigningKey;

/// Native Ed25519 verifying key.
///
/// This is a type alias to `ed25519_dalek::VerifyingKey`.
pub type VerifyingKey = ed25519_dalek::VerifyingKey;
