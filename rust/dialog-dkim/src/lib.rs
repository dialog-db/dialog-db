//! Self-contained [DKIM] (RFC 6376) verification: enough of the standard to
//! prove that a captured, DKIM-signed email really was signed by the domain that
//! sent it.
//!
//! This crate is deliberately narrow. It parses an email's headers, parses the
//! `DKIM-Signature` header into its tags, canonicalizes the signed headers
//! (both `relaxed` and `simple`, because a verifier does not control which the
//! signer used), reconstructs the exact byte string DKIM signs per [RFC 6376]
//! section 3.7, and verifies the `b=` signature with the domain's public key
//! using the inner algorithm (`rsa-sha256` or `ed25519-sha256`). It knows
//! nothing about DIDs, UCANs, or varsig; those live one layer up in
//! `dialog-did-web` / `dialog-credentials`.
//!
//! # What is and is not verified
//!
//! DKIM signs two things: a hash of the body (the `bh=` tag) and a signature
//! over the selected headers plus the `DKIM-Signature` header itself (the `b=`
//! tag). This crate verifies the **header signature `b=`** and treats the body
//! hash `bh=` as trusted input carried inside the (itself-signed)
//! `DKIM-Signature` header. That is intentional: our security claim is about the
//! signed `From:` and `Subject:` headers ("this domain sent an email from this
//! address whose subject binds this key"), not about the body, and the proof we
//! carry deliberately does **not** include the body (see
//! [`SignedEmail`](dkim::SignedEmail)). Because `bh=` lives inside the
//! `DKIM-Signature` header, it is covered by `b=`, so a valid `b=` proves the
//! signer committed to that body hash even though we never see the body. See the
//! module docs on [`verify`](dkim::verify) for the full argument.
//!
//! [DKIM]: https://en.wikipedia.org/wiki/DomainKeys_Identified_Mail
//! [RFC 6376]: https://www.rfc-editor.org/rfc/rfc6376

#![cfg_attr(docsrs, feature(doc_cfg))]

// The crate does nothing without the `dkim` feature (it only implements DKIM),
// so the whole module is gated on it. This keeps the feature OFF by default per
// the workspace convention while leaving an empty crate that still compiles.
#[cfg(feature = "dkim")]
pub mod dkim;

#[cfg(feature = "dkim")]
pub use dkim::*;
