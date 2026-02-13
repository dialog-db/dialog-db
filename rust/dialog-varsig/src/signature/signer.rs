//! Signature creation trait.

use std::future::Future;

use super::Signature;
use dialog_common::ConditionalSend;

/// Produces a cryptographic signature over a payload.
pub trait Signer<S: Signature> {
    /// Sign `payload` and return the signature.
    fn sign(
        &self,
        payload: &[u8],
    ) -> impl Future<Output = Result<S, signature::Error>> + ConditionalSend;
}
