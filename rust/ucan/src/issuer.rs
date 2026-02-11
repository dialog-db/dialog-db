//! UCAN issuer trait.

use varsig::{
    principal::Principal,
    signature::{Signature, Signer},
};

/// An entity that can issue UCANs: it can sign tokens and is
/// identified by a DID.
///
/// Blanket-implemented for any type that is both a [`Signer<S>`]
/// and a [`Principal`].
pub trait Issuer<S: Signature>: Signer<S> + Principal {}

impl<S: Signature, T> Issuer<S> for T where T: Signer<S> + Principal {}
