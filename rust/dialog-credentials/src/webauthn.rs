//! WebAuthn P-256 key types, DID, and verifier implementation.
//!
//! Provides signature verification for WebAuthn/passkey credentials using
//! ECDSA P-256 (the dominant passkey algorithm). The verifier works on both
//! native and WASM platforms.
//!
//! The signature format follows the [varsig WebAuthn extension], encoding
//! `clientDataJSON` and `authenticatorData` alongside the inner ECDSA signature.
//!
//! [varsig WebAuthn extension]: https://github.com/ChainAgnostic/varsig/pull/11

mod error;
pub mod native;
mod resolver;
mod signer;
mod verifier;

pub use error::{WebAuthnDidFromStrError, WebAuthnResolveError, WebAuthnVerifyError};
pub use resolver::WebAuthnKeyResolver;
pub use signer::{RegistrationOptions, WebAuthnSigner, WebAuthnSignerError};
pub use verifier::WebAuthnVerifier;
