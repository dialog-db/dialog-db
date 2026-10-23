//! DKIM (RFC 6376) parsing, canonicalization, and verification.

mod canonicalize;
mod error;
mod key;
mod message;
mod signature;
mod verify;

#[cfg(test)]
mod test;

pub use canonicalize::{BodyCanon, Canonicalization, HeaderCanon};
pub use error::DkimError;
pub use key::{DkimKeyType, DkimPublicKey};
pub use message::{Header, Message};
pub use signature::{DkimSignatureHeader, SignatureAlgorithm};
pub use verify::{SignedEmail, verify, verify_with_key, verify_with_key_at};
