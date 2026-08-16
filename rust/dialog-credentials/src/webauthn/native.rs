//! Native P-256 verifying-key alias for WebAuthn signatures.
//!
//! WebAuthn verification uses the pure-Rust `p256` crate on every platform
//! (native and WASM), so this is the canonical verifying-key type everywhere.

/// P-256 verifying key used to check the inner ECDSA signature of a WebAuthn
/// assertion.
pub type VerifyingKey = p256::ecdsa::VerifyingKey;
