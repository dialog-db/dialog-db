//! `did:mailto` verification: a DKIM-signed email binds an email identity to a
//! `did:key`.
//!
//! A `did:mailto:{domain}:{local}` identity is bound to a `did:key` by a
//! DKIM-signed email whose subject reads `I am also known as {did:key}`. This
//! is a **powerline** delegation: the bound `did:key` may sign anything on the
//! email's behalf, and the `did:mailto` itself never signs a per-UCAN payload.
//! See [`verify_mailto_proof`] for the end-to-end check and
//! [`DkimKeyProvider`] for the DNS-over-HTTPS DKIM key resolution seam.
//!
//! # The subject is the audience
//!
//! A DKIM signature is public and permanent: it rides in headers that every
//! relay, the recipient's provider, and anyone the mail is forwarded to can
//! read. So a captured proof *will* be replayed, and it carries no nonce,
//! expiry of its own, or audience tag to stop that.
//!
//! It does not need one. The binding's output is the `did:key` named in the
//! signed subject, and the subject is inside `h=`, so `b=` covers it. A
//! replayer cannot substitute their own key without breaking the signature, so
//! whoever presents the proof, to whichever relying party, it authorizes the
//! same key it always named. Replaying it only restates a true statement:
//! "alice@example.com also goes by this key." That is the audience binding,
//! and it is why no separate one is required.
//!
//! Two consequences worth keeping in view:
//!
//! - Moving the key out of the signed subject (to an unsigned header, say)
//!   would silently turn a public, permanent proof into a bearer token. The
//!   property is structural, and pinned by
//!   `a_replayed_proof_still_authorizes_only_the_subjects_key`.
//! - Because bindings only accumulate ([`multi_verifier_from_bindings`] unions
//!   them), a bound key cannot be un-bound by this layer. Revocation, if
//!   needed, belongs to whatever holds the set of proofs, not here.
//!
//! # The trust unit is the domain
//!
//! DKIM keys are published per domain, and `s=` is chosen by the signer, so
//! anyone controlling *any* `_domainkey` selector at `example.com` can sign for
//! *any* mailbox there. `did:mailto:example.com:alice` reads like a
//! mailbox-level identity, but the authority behind it is the domain's DNS.
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
