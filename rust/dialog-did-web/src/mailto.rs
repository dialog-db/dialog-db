//! `did:mailto` verification: a DKIM-signed email binds an email identity to a
//! `did:key`.
//!
//! A `did:mailto:{domain}:{local}` identity is bound to a `did:key` by a
//! one-time, DKIM-signed email whose subject reads `I am also known as
//! {did:key}`. This is a **powerline** delegation: the bound `did:key` may sign
//! anything on the email's behalf, and the `did:mailto` itself never signs a
//! per-UCAN payload. See [`verify_mailto_proof`] for the end-to-end check and
//! [`DkimKeyProvider`] for the DNS-over-HTTPS DKIM key resolution seam.
//!
//! The captured proof travels as a [`DkimSignature`](dialog_varsig::algorithm::dkim::DkimSignature)
//! varsig value; the actual DKIM parsing and verification lives in the
//! self-contained [`dialog_dkim`] crate.

mod did;
mod key_provider;
mod subject;
mod verifier;

#[cfg(all(test, feature = "test-fetch"))]
mod test;

pub use did::MailtoDid;
pub use key_provider::{DEFAULT_DKIM_KEY_TTL, DkimKeyProvider};
pub use subject::{extract_did_key, extract_from_address};
pub use verifier::{DidMailtoBinding, multi_verifier_from_bindings, verify_mailto_proof};
