//! WebAuthn / passkey credential types.
//!
//! Signature verification for WebAuthn credentials using ECDSA P-256 (the
//! dominant passkey algorithm). The verifier works on both native and WASM
//! platforms; the browser signer is WASM-only.
//!
//! A WebAuthn credential is presented as a distinct `did:key` variant: the key
//! bytes are an ordinary 33-byte compressed P-256 point, but the multicodec
//! prefix is the private-use
//! [`WEBAUTHN_P256_MULTICODEC`](verifier::WEBAUTHN_P256_MULTICODEC) tag rather
//! than `p256-pub`. That prefix routes `did:key` resolution to a
//! [`WebAuthnVerifier`], so a passkey published as a `did:web` / `did:plc`
//! verification method (a `publicKeyMultibase` carrying that multicodec) is
//! resolved and verified through the ordinary multi-key path.
//!
//! The signature format follows the [varsig WebAuthn extension], encoding
//! `clientDataJSON` and `authenticatorData` alongside the inner DER-encoded
//! ECDSA signature.
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
pub use verifier::{WEBAUTHN_P256_MULTICODEC, WebAuthnVerifier};
