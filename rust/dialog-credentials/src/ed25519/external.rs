//! An Ed25519 key held outside this process, which signs on request.
//!
//! The key material never enters this process: an [`ExternalSign`] gets the
//! bytes to sign and gives back the signature. A runtime component that signs
//! through an import of its host is one, so that no key is in its linear
//! memory. Only the public key is held here.
//!
//! Such a key cannot be exported. It runs a key agreement only where its
//! holder does it ([`ExternalSecret`]): then it reveals a sealed secret and
//! derives a key, which is held there too. Otherwise it can do neither.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_varsig::eddsa::Ed25519Signature;

use crate::secret::{SealedSecret, SecretError};

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

/// What a key held outside this process does with its agreement key, which
/// stays where the key is: open a secret sealed to it, and derive a key from
/// it, which is held there too.
pub trait ExternalSecret: ConditionalSend + ConditionalSync {
    /// The plaintext of `sealed`, which was sealed to this key in the context
    /// labelled `context`.
    fn reveal(&self, context: &'static str, sealed: SealedSecret) -> ExternalRevealing;

    /// The key that this key derives in the context labelled `context` for
    /// `label`, held where this one is.
    fn derive(&self, context: &'static str, label: Vec<u8>) -> ExternalDeriving;
}

/// A plaintext that an [`ExternalSecret`] gives back, on its way.
pub trait Revealing: Future<Output = Result<Vec<u8>, SecretError>> + ConditionalSend {}

impl<F> Revealing for F where F: Future<Output = Result<Vec<u8>, SecretError>> + ConditionalSend {}

/// The future that [`ExternalSecret::reveal`] returns.
pub type ExternalRevealing = Pin<Box<dyn Revealing>>;

/// A derived key that an [`ExternalSecret`] gives back, on its way.
pub trait Deriving: Future<Output = Result<SigningKey, SecretError>> + ConditionalSend {}

impl<F> Deriving for F where F: Future<Output = Result<SigningKey, SecretError>> + ConditionalSend {}

/// The future that [`ExternalSecret::derive`] returns.
pub type ExternalDeriving = Pin<Box<dyn Deriving>>;

/// An Ed25519 key held outside this process: its public key, the
/// [`ExternalSign`] that signs with it, and the [`ExternalSecret`] that runs
/// its key agreement, if the key's holder does.
#[derive(Clone)]
pub struct SigningKey {
    verifying_key: ed25519_dalek::VerifyingKey,
    signer: Arc<dyn ExternalSign>,
    secret: Option<Arc<dyn ExternalSecret>>,
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
            secret: None,
        }
    }

    /// This key, whose key agreement `secret` runs where the key is held.
    #[must_use]
    pub fn with_secret(mut self, secret: impl ExternalSecret + 'static) -> Self {
        self.secret = Some(Arc::new(secret));
        self
    }

    /// What runs this key's agreement, if its holder does.
    pub(crate) fn secret(&self) -> Option<&Arc<dyn ExternalSecret>> {
        self.secret.as_ref()
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

    /// A holder that keeps the key in memory and runs its agreement, as a
    /// host would on the other side of an import.
    #[derive(Clone)]
    struct Holder(Ed25519Signer);

    impl ExternalSign for Holder {
        fn sign(&self, payload: &[u8]) -> ExternalSigning {
            let signer = self.0.clone();
            let payload = payload.to_vec();
            Box::pin(async move { Ok(signer.signing_key().sign_bytes(&payload).await?.to_bytes()) })
        }
    }

    impl ExternalSecret for Holder {
        fn reveal(&self, context: &'static str, sealed: SealedSecret) -> ExternalRevealing {
            let signer = self.0.clone();
            Box::pin(async move { signer.secret(Context::new(context)).reveal(&sealed).await })
        }

        fn derive(&self, context: &'static str, label: Vec<u8>) -> ExternalDeriving {
            let signer = self.0.clone();
            Box::pin(async move {
                let derived = signer.secret(Context::new(context)).derive(&label).await?;
                Ok(held_by(Holder(derived)))
            })
        }
    }

    /// The key `holder` keeps, as a key held outside this process.
    fn held_by(holder: Holder) -> SigningKey {
        let bytes = holder.0.signing_key().verifying_key().to_bytes();
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&bytes).unwrap();
        SigningKey::new(verifying_key, holder.clone()).with_secret(holder)
    }

    async fn held_with_secret() -> (Ed25519Signer, Ed25519Signer) {
        let inner = Ed25519Signer::import(&[9; 32]).await.unwrap();
        let key = held_by(Holder(inner.clone()));
        (inner, Ed25519Signer::from(Ed25519SigningKey::from(key)))
    }

    #[dialog_common::test]
    async fn it_reveals_a_secret_through_the_holder_of_the_key() {
        let (_, signer) = held_with_secret().await;
        let sealed = signer
            .ed25519_did()
            .secret(CONTEXT)
            .conceal(b"a vault key")
            .await
            .unwrap();

        let revealed = signer.secret(CONTEXT).reveal(&sealed).await.unwrap();

        assert_eq!(revealed, b"a vault key");
    }

    #[dialog_common::test]
    async fn it_derives_through_the_holder_the_key_the_holder_derives() {
        let (inner, signer) = held_with_secret().await;

        let derived = signer.secret(CONTEXT).derive(b"peer").await.unwrap();
        let directly = inner.secret(CONTEXT).derive(b"peer").await.unwrap();

        assert_eq!(derived.ed25519_did(), directly.ed25519_did());
    }

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
