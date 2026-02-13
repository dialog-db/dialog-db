//! Native P-256 ECDSA verification for WebAuthn signatures.
//!
//! Uses the `p256` crate for signature verification on non-WASM platforms,
//! and also provides the canonical implementation for WASM (since `p256`
//! is pure Rust and works everywhere).

/// Native P-256 verifying key.
pub type VerifyingKey = p256::ecdsa::VerifyingKey;
