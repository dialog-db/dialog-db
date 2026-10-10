//! An Ed25519 key held outside this process, which signs on request.
//!
//! The key material never enters this process: an [`ExternalSign`] gets the
//! bytes to sign and gives back the signature. A runtime component that signs
//! through an import of its host is one, so that no key is in its linear
//! memory. Only the public key is held here.
//!
//! Such a key cannot be exported, and cannot run a key agreement, so it cannot
//! derive a key or reveal a sealed secret.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_varsig::eddsa::Ed25519Signature;

/// A signature that an [`ExternalSign`] gives back, on its way: `Send` where
/// the target has threads.
pub trait Signing: Future<Output = Result<[u8; 64], signature::Error>> + ConditionalSend {}

impl<F> Signing for F where F: Future<Output = Result<[u8; 64], signature::Error>> + ConditionalSend {}

/// The future that [`ExternalSign::sign`] returns.
pub type ExternalSigning = Pin<Box<dyn Signing>>;

/// Signs bytes with an Ed25519 key that is held elsewhere.
pub trait ExternalSign: ConditionalSend + ConditionalSync {
    /// The Ed25519 signature of `payload`.
    fn sign(&self, payload: &[u8]) -> ExternalSigning;
}

/// An Ed25519 key held outside this process: its public key, and the
/// [`ExternalSign`] that signs with it.
#[derive(Clone)]
pub struct SigningKey {
    verifying_key: ed25519_dalek::VerifyingKey,
    signer: Arc<dyn ExternalSign>,
}

impl SigningKey {
    /// The key whose public key is `verifying_key`, which `signer` signs
    /// with.
    pub fn new(
        verifying_key: ed25519_dalek::VerifyingKey,
        signer: impl ExternalSign + 'static,
    ) -> Self {
        Self {
            verifying_key,
            signer: Arc::new(signer),
        }
    }

    /// The public key.
    #[must_use]
    pub const fn verifying_key(&self) -> ed25519_dalek::VerifyingKey {
        self.verifying_key
    }

    /// Signs `msg` through the signer, and checks the signature against the
    /// public key, so that a signer with another key fails here and not at a
    /// verifier.
    ///
    /// # Errors
    ///
    /// If the signer fails, or its signature does not verify.
    pub async fn sign_bytes(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        let bytes = self.signer.sign(msg).await?;
        let signature = ed25519_dalek::Signature::from_bytes(&bytes);
        self.verifying_key.verify_strict(msg, &signature)?;
        Ok(Ed25519Signature::from(signature))
    }
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigningKey")
            .field("verifying_key", &self.verifying_key)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Ed25519Signer;
    use crate::ed25519::{Ed25519KeyError, Ed25519SigningKey};
    use crate::secret::{Context, SecretError};
    use signature::Signer as _;

    /// A signer that holds its key in memory, as a host would on the other
    /// side of an import.
    struct Held(ed25519_dalek::SigningKey);

    impl ExternalSign for Held {
        fn sign(&self, payload: &[u8]) -> ExternalSigning {
            let signature = self.0.sign(payload).to_bytes();
            Box::pin(std::future::ready(Ok(signature)))
        }
    }

    fn held(seed: u8) -> Held {
        Held(ed25519_dalek::SigningKey::from_bytes(&[seed; 32]))
    }

    const CONTEXT: Context = Context::new("dialog/test/external/v1");

    /// A signer over a key held outside this process.
    fn external_signer() -> Ed25519Signer {
        let signer = held(1);
        let key = SigningKey::new(signer.0.verifying_key(), signer);
        Ed25519Signer::from(Ed25519SigningKey::from(key))
    }

    #[dialog_common::test]
    async fn it_signs_through_the_signer_of_the_key() {
        let signer = held(1);
        let verifying_key = signer.0.verifying_key();
        let key = SigningKey::new(verifying_key, signer);

        let signature = key.sign_bytes(b"a revision").await.unwrap();

        let signature = ed25519_dalek::Signature::from_bytes(&signature.to_bytes());
        verifying_key
            .verify_strict(b"a revision", &signature)
            .unwrap();
    }

    /// `signature::Error` is opaque, so the test can name no variant: it
    /// checks that the signature of another key does not come back.
    #[dialog_common::test]
    async fn it_refuses_a_signature_that_another_key_made() {
        let key = SigningKey::new(held(1).0.verifying_key(), held(2));

        let signed = key.sign_bytes(b"a revision").await;

        assert!(signed.is_err(), "{signed:?}");
    }

    #[dialog_common::test]
    async fn it_cannot_export_a_key_held_outside_this_process() {
        let signer = held(1);
        let key = Ed25519SigningKey::from(SigningKey::new(signer.0.verifying_key(), signer));

        let exported = key.export().await;

        assert!(
            matches!(exported, Err(Ed25519KeyError::External)),
            "{exported:?}"
        );
    }

    #[dialog_common::test]
    async fn it_has_no_agreement_key_for_a_key_held_outside_this_process() {
        let signer = held(1);
        let key = Ed25519SigningKey::from(SigningKey::new(signer.0.verifying_key(), signer));

        let agreement = key.agreement_key().await;

        assert!(
            matches!(agreement, Err(Ed25519KeyError::AgreementKeyUnavailable)),
            "{:?}",
            agreement.err()
        );
    }

    #[dialog_common::test]
    async fn it_cannot_derive_a_key_from_a_key_held_outside_this_process() {
        let signer = external_signer();

        let derived = signer.secret(CONTEXT).derive(b"peer").await;

        assert!(
            matches!(derived, Err(SecretError::AgreementKeyUnavailable)),
            "{:?}",
            derived.err()
        );
    }

    #[dialog_common::test]
    async fn it_cannot_reveal_a_secret_sealed_to_a_key_held_outside_this_process() {
        let signer = external_signer();
        let sealed = signer
            .ed25519_did()
            .secret(CONTEXT)
            .conceal(b"a vault key")
            .await
            .unwrap();

        let revealed = signer.secret(CONTEXT).reveal(&sealed).await;

        assert!(
            matches!(revealed, Err(SecretError::AgreementKeyUnavailable)),
            "{:?}",
            revealed.err()
        );
    }
}
