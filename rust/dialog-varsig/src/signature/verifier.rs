//! Signature verification trait.

use std::future::Future;

use super::Signature;
use dialog_common::ConditionalSend;

/// Verifies that a cryptographic signature is valid for a given payload.
///
/// Generic over `S: Signature` so a single type (e.g. a DID key)
/// can verify multiple signature algorithms.
pub trait Verifier<S: Signature> {
    /// Verify that `signature` is valid for `payload`.
    fn verify(
        &self,
        payload: &[u8],
        signature: &S,
    ) -> impl Future<Output = Result<(), signature::Error>> + ConditionalSend;
}
