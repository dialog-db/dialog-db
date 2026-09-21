//! Sealed secrets: encrypt to a `did:key` with no prior interaction.
//!
//! A holder of someone's DID can conceal a secret so that only that identity
//! can reveal it. Nothing has to be published or exchanged first -- the
//! recipient's X25519 agreement key is derived from the Ed25519 key its DID
//! already carries.
//!
//! ```no_run
//! # use dialog_credentials::{Ed25519Signer, secret::Context};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! const VAULT: Context = Context::new("dialog/vault/v1");
//!
//! let profile = Ed25519Signer::generate().await?;
//! let vault_key = [7u8; 32];
//!
//! // Anyone holding the profile's DID can seal to it.
//! let sealed = profile.ed25519_did().secret(VAULT).conceal(&vault_key).await?;
//!
//! // Only the profile can open it.
//! let revealed = profile.secret(VAULT).reveal(&sealed).await?;
//! assert_eq!(revealed, vault_key);
//! # Ok(())
//! # }
//! ```
//!
//! # Construction
//!
//! Each `conceal` generates a fresh ephemeral X25519 key pair, agrees with the
//! recipient's derived key, and derives an AES-256-GCM key with HKDF-SHA256
//! bound to the context label and both public keys. The ephemeral public key
//! travels with the ciphertext; the ephemeral secret is discarded. Sealing
//! twice therefore produces different bytes, and a later compromise of the
//! sender's own keys does not open past messages.

use crate::ed25519::{
    Ed25519Signer, Ed25519Verifier, Extractable, Sealed, X25519PublicKey, X25519SecretKey,
};
use crate::key::ExtractableKey;
use std::future::Future;

mod error;
mod message;
mod platform;

pub use error::SecretError;
pub use message::SealedSecret;

/// A domain-separation label scoping a sealed secret to one purpose.
///
/// Revealing requires the same context used to conceal, so a secret sealed for
/// one purpose cannot be opened as another. Labels are compile-time constants,
/// which keeps them from drifting apart at a call site.
///
/// Version the label whenever the meaning of what is sealed changes:
///
/// ```
/// # use dialog_credentials::secret::Context;
/// const VAULT: Context = Context::new("dialog/vault/v1");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Context(&'static str);

impl Context {
    /// Create a context from a static label.
    #[must_use]
    pub const fn new(label: &'static str) -> Self {
        Self(label)
    }

    /// Get the label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Seals secrets to another identity, in one [`Context`].
///
/// Obtained from [`Ed25519Verifier::secret`]. Holding only a public identity,
/// this can conceal but not reveal -- revealing needs [`Secret`], which only a
/// signer can produce.
#[derive(Debug, Clone, Copy)]
pub struct Seal<'a> {
    recipient: &'a Ed25519Verifier,
    context: Context,
}

impl Seal<'_> {
    /// Conceal `plain` so that only the recipient can reveal it.
    ///
    /// # Errors
    ///
    /// Returns an error if the recipient's DID yields no usable agreement key,
    /// or if a platform crypto operation fails.
    pub async fn conceal(&self, plain: &[u8]) -> Result<SealedSecret, SecretError> {
        let recipient = X25519PublicKey::from_ed25519(self.recipient).await?;
        platform::conceal(&recipient, self.recipient, self.context, plain).await
    }
}

/// Seals and opens secrets for one identity, in one [`Context`].
///
/// Obtained from [`Ed25519Signer::secret`]. Backed by a signing key, so it can
/// both conceal (to itself) and reveal.
#[derive(Debug, Clone, Copy)]
pub struct Secret<'a, E = crate::ed25519::Sealed> {
    signer: &'a Ed25519Signer<E>,
    context: Context,
}

impl<E> Secret<'_, E> {
    /// Reveal a secret concealed to this identity.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::Failed`] if the message was sealed to a
    /// different identity or context, or if it has been tampered with. The
    /// cases are deliberately indistinguishable.
    pub async fn reveal(&self, sealed: &SealedSecret) -> Result<Vec<u8>, SecretError> {
        let key: X25519SecretKey = self.signer.agreement_key().await?;
        platform::reveal(&key, self.signer.ed25519_did(), self.context, sealed).await
    }

    /// Derive a signer from this identity, deterministically.
    ///
    /// The same identity, context and `label` yield the same signer on every
    /// platform, so a derived DID is stable across sessions and across native
    /// and the browser.
    ///
    /// The derivation is a key agreement against this identity's own agreement
    /// key. Ed25519 signatures are not a pseudo-random function: RFC 8032
    /// specifies a deterministic nonce, but hedged variants that fold in fresh
    /// entropy are conforming and deployed (Apple's CryptoKit, and so WebKit's
    /// `Ed25519`). Agreement has no nonce to hedge.
    ///
    /// The result is [`Sealed`]: its material cannot be read back. A consumer
    /// that needs raw bytes derives the extractable form through
    /// [`SecretExtractableDerive`]:
    ///
    /// ```no_run
    /// # use dialog_credentials::{Ed25519Signer, secret::{Context, SecretExtractableDerive}};
    /// # async fn example(signer: &Ed25519Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// # const CTX: Context = Context::new("example/v1");
    /// let readable = SecretExtractableDerive::derive(&signer.secret(CTX), b"peer").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::AgreementKeyUnavailable`] when this identity
    /// carries no agreement key -- reachable only in the browser, for a key
    /// restored from an archive written before agreement keys were stored,
    /// whose seed is gone and cannot be re-derived. Otherwise returns an error
    /// if a platform crypto operation fails.
    pub async fn derive(&self, label: &[u8]) -> Result<Ed25519Signer<Sealed>, SecretError> {
        let seed = self.derive_bytes(label).await?;
        Ed25519Signer::import(&seed)
            .await
            .map_err(|error| SecretError::Crypto(error.to_string()))
    }

    /// The derived 32 bytes.
    ///
    /// Private: material leaves this module only inside a signer.
    async fn derive_bytes(&self, label: &[u8]) -> Result<[u8; 32], SecretError> {
        let key: X25519SecretKey = self.signer.agreement_key().await?;
        platform::derive(&key, self.context, label).await
    }

    /// Conceal `plain` to this same identity.
    ///
    /// Useful for sealing something only you can read back later.
    ///
    /// # Errors
    ///
    /// Returns an error if a platform crypto operation fails.
    pub async fn conceal(&self, plain: &[u8]) -> Result<SealedSecret, SecretError> {
        self.signer
            .ed25519_did()
            .secret(self.context)
            .conceal(plain)
            .await
    }
}

/// Derive a signer whose material can be read back.
///
/// The counterpart to [`Secret::derive`], deriving the same key under the same
/// label and differing only in extractability. `Secret`'s inherent `derive`
/// wins method resolution, so this one is reachable only through its
/// fully-qualified form:
///
/// ```no_run
/// # use dialog_credentials::{Ed25519Signer, secret::{Context, SecretExtractableDerive}};
/// # async fn example(signer: &Ed25519Signer) -> Result<(), Box<dyn std::error::Error>> {
/// # const CTX: Context = Context::new("example/v1");
/// let readable = SecretExtractableDerive::derive(&signer.secret(CTX), b"peer").await?;
/// # Ok(())
/// # }
/// ```
///
/// Importing the trait and writing that form is what makes a readable key a
/// deliberate act rather than the shape of a binding.
///
/// # Security Warning
///
/// A derived extractable key yields its seed to anyone holding it. Derive one
/// only for a consumer that is built from raw bytes and cannot take a signer,
/// such as `iroh::SecretKey`, whose QUIC stack needs the material in process.
pub trait SecretExtractableDerive {
    /// Derive an extractable signer, deterministically.
    ///
    /// # Errors
    ///
    /// As [`Secret::derive`].
    fn derive(
        &self,
        label: &[u8],
    ) -> impl Future<Output = Result<Ed25519Signer<Extractable>, SecretError>>;
}

impl<E> SecretExtractableDerive for Secret<'_, E> {
    async fn derive(&self, label: &[u8]) -> Result<Ed25519Signer<Extractable>, SecretError> {
        let seed = self.derive_bytes(label).await?;
        // `ExtractableKey::import` asks `WebCrypto` for a `CryptoKey` that will
        // give its seed back, where the default import asks for one that will
        // not.
        <Ed25519Signer<Extractable> as ExtractableKey>::import(&seed)
            .await
            .map_err(|error| SecretError::Crypto(error.to_string()))
    }
}

impl Ed25519Verifier {
    /// Seal secrets to this identity, scoped to `context`.
    #[must_use]
    pub const fn secret(&self, context: Context) -> Seal<'_> {
        Seal {
            recipient: self,
            context,
        }
    }
}

impl<E> Ed25519Signer<E> {
    /// Seal and open secrets for this identity, scoped to `context`.
    #[must_use]
    pub const fn secret(&self, context: Context) -> Secret<'_, E> {
        Secret {
            signer: self,
            context,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KeyExport;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    const VAULT: Context = Context::new("dialog/vault/v1");
    const OTHER: Context = Context::new("dialog/other/v1");

    async fn signer(seed: u8) -> Ed25519Signer {
        Ed25519Signer::import(&[seed; 32]).await.unwrap()
    }

    /// Both derivations give the same identity; only extractability differs.
    #[dialog_common::test]
    async fn extractable_derivation_is_the_same_key() {
        let profile = signer(1).await;

        let sealed = profile.secret(VAULT).derive(b"peer").await.unwrap();
        let readable = SecretExtractableDerive::derive(&profile.secret(VAULT), b"peer")
            .await
            .unwrap();

        assert_eq!(
            sealed.ed25519_did(),
            readable.ed25519_did(),
            "one derivation, two extractabilities"
        );
    }

    /// A key derived as extractable exports its seed.
    ///
    /// Only a real assertion in the browser: native keys are readable
    /// whatever the type says.
    #[dialog_common::test]
    async fn an_extractable_derivation_exports() {
        let profile = signer(1).await;
        let readable = SecretExtractableDerive::derive(&profile.secret(VAULT), b"peer")
            .await
            .unwrap();

        // Matched rather than destructured: on native `KeyExport` has only
        // the one variant, so a `let...else` here is irrefutable and the
        // compiler says so.
        match readable.export().await.unwrap() {
            KeyExport::Extractable(seed) => assert_eq!(seed.len(), 32),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            KeyExport::NonExtractable { .. } => {
                panic!("a key derived as extractable must export its seed")
            }
        }
    }

    #[dialog_common::test]
    async fn conceal_reveal_roundtrip() {
        let profile = signer(1).await;
        let vault_key = [7u8; 32];

        // The account holds only the profile's DID.
        let sealed = profile
            .ed25519_did()
            .secret(VAULT)
            .conceal(&vault_key)
            .await
            .unwrap();

        let revealed = profile.secret(VAULT).reveal(&sealed).await.unwrap();

        assert_eq!(revealed, vault_key);
    }

    #[dialog_common::test]
    async fn seals_to_a_did_with_no_prior_interaction() {
        // The whole point: the account parses a DID string and can seal to it.
        let profile = signer(2).await;
        let did: Ed25519Verifier = profile.ed25519_did().to_string().parse().unwrap();

        let sealed = did.secret(VAULT).conceal(b"vault key").await.unwrap();

        assert_eq!(
            profile.secret(VAULT).reveal(&sealed).await.unwrap(),
            b"vault key"
        );
    }

    #[dialog_common::test]
    async fn another_identity_cannot_reveal() {
        let profile = signer(3).await;
        let intruder = signer(4).await;

        let sealed = profile
            .ed25519_did()
            .secret(VAULT)
            .conceal(b"secret")
            .await
            .unwrap();

        assert!(
            matches!(
                intruder.secret(VAULT).reveal(&sealed).await,
                Err(SecretError::Failed)
            ),
            "a different identity must not reveal the secret"
        );
    }

    #[dialog_common::test]
    async fn a_different_context_cannot_reveal() {
        let profile = signer(5).await;

        let sealed = profile
            .ed25519_did()
            .secret(VAULT)
            .conceal(b"secret")
            .await
            .unwrap();

        assert!(
            matches!(
                profile.secret(OTHER).reveal(&sealed).await,
                Err(SecretError::Failed)
            ),
            "context is domain separation: the wrong label must not reveal"
        );
    }

    #[dialog_common::test]
    async fn derivation_is_deterministic() {
        let profile = signer(10).await;

        let first = profile
            .secret(VAULT)
            .derive_bytes(b"operator")
            .await
            .unwrap();
        let second = profile
            .secret(VAULT)
            .derive_bytes(b"operator")
            .await
            .unwrap();

        assert_eq!(
            first, second,
            "the same identity, context and label must derive the same secret"
        );
    }

    #[dialog_common::test]
    async fn derivation_separates_labels_contexts_and_identities() {
        let profile = signer(11).await;
        let other = signer(12).await;

        let base = profile
            .secret(VAULT)
            .derive_bytes(b"operator")
            .await
            .unwrap();

        assert_ne!(
            base,
            profile.secret(VAULT).derive_bytes(b"other").await.unwrap(),
            "a different label must derive an unrelated secret"
        );
        assert_ne!(
            base,
            profile
                .secret(OTHER)
                .derive_bytes(b"operator")
                .await
                .unwrap(),
            "a different context must derive an unrelated secret"
        );
        assert_ne!(
            base,
            other.secret(VAULT).derive_bytes(b"operator").await.unwrap(),
            "a different identity must derive an unrelated secret"
        );
    }

    /// A known-answer vector, and the structural guard on this derivation.
    ///
    /// Determinism alone is not enough: a signature-based derivation is
    /// deterministic too, on every platform whose Ed25519 does not hedge the
    /// nonce. This test pins the derivation to one value asserted by the same
    /// code on native AND wasm, so it fails the moment the derivation stops
    /// being a pure function of the key material -- including a relapse into
    /// signing, and including the two platform arms silently diverging, which
    /// is what the derivation this replaced actually did.
    ///
    /// Do not delete it as redundant with the determinism tests. If the
    /// derivation changes on purpose, bump the context label (which re-derives
    /// every operator and forks each profile's replica lineage) and record the
    /// new vector deliberately.
    #[dialog_common::test]
    async fn derivation_matches_a_known_vector() {
        const EXPECTED: [u8; 32] = [
            0x14, 0xaa, 0x7e, 0x0e, 0x1f, 0x45, 0x62, 0xb6, 0xf4, 0xdc, 0x84, 0xdc, 0x26, 0x0c,
            0x81, 0x07, 0xef, 0x76, 0xb3, 0x08, 0x52, 0x7e, 0xb4, 0x7b, 0x8a, 0x2f, 0xdc, 0x6b,
            0xe7, 0x7a, 0xf0, 0xa8,
        ];

        let profile = Ed25519Signer::import(&[42u8; 32]).await.unwrap();
        let derived = profile
            .secret(VAULT)
            .derive_bytes(b"operator")
            .await
            .unwrap();

        assert_eq!(derived, EXPECTED);
    }

    #[dialog_common::test]
    async fn tampering_is_detected() {
        let profile = signer(6).await;
        let sealed = profile
            .ed25519_did()
            .secret(VAULT)
            .conceal(b"secret")
            .await
            .unwrap();

        let mut bytes = sealed.to_bytes();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        let tampered = SealedSecret::from_bytes(&bytes).unwrap();

        assert!(matches!(
            profile.secret(VAULT).reveal(&tampered).await,
            Err(SecretError::Failed)
        ));
    }

    #[dialog_common::test]
    async fn tampering_with_the_ephemeral_key_is_detected() {
        let profile = signer(7).await;
        let sealed = profile
            .ed25519_did()
            .secret(VAULT)
            .conceal(b"secret")
            .await
            .unwrap();

        let mut bytes = sealed.to_bytes();
        bytes[0] ^= 0x01;

        // A corrupted ephemeral key either fails agreement or derives the
        // wrong key; either way the secret must not come back.
        let result = match SealedSecret::from_bytes(&bytes) {
            Ok(tampered) => profile.secret(VAULT).reveal(&tampered).await,
            Err(e) => Err(e),
        };
        assert!(result.is_err(), "a tampered ephemeral key must not reveal");
    }

    #[dialog_common::test]
    async fn sealing_twice_gives_different_bytes() {
        // A fresh ephemeral key per message means the ciphertext differs even
        // for identical plaintext, so equal vault entries are not linkable.
        let profile = signer(8).await;
        let seal = profile.ed25519_did().secret(VAULT);

        let first = seal.conceal(b"same secret").await.unwrap();
        let second = seal.conceal(b"same secret").await.unwrap();

        assert_ne!(first.to_bytes(), second.to_bytes());
        assert_ne!(
            first.ephemeral_public_key, second.ephemeral_public_key,
            "each seal should use a fresh ephemeral key"
        );

        // Both still open to the same plaintext.
        assert_eq!(
            profile.secret(VAULT).reveal(&first).await.unwrap(),
            profile.secret(VAULT).reveal(&second).await.unwrap()
        );
    }

    #[dialog_common::test]
    async fn seals_to_self() {
        let profile = signer(9).await;

        let sealed = profile
            .secret(VAULT)
            .conceal(b"note to self")
            .await
            .unwrap();

        assert_eq!(
            profile.secret(VAULT).reveal(&sealed).await.unwrap(),
            b"note to self"
        );
    }

    #[dialog_common::test]
    async fn wire_format_roundtrips() {
        let profile = signer(10).await;
        let sealed = profile
            .ed25519_did()
            .secret(VAULT)
            .conceal(&[3u8; 32])
            .await
            .unwrap();

        let bytes = sealed.to_bytes();
        assert_eq!(
            bytes.len(),
            32 + 12 + 32 + 16,
            "92 bytes for a 32-byte secret"
        );

        let decoded = SealedSecret::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, sealed);
        assert_eq!(
            profile.secret(VAULT).reveal(&decoded).await.unwrap(),
            [3u8; 32]
        );
    }

    #[dialog_common::test]
    async fn short_input_is_malformed() {
        assert!(matches!(
            SealedSecret::from_bytes(&[0u8; 16]),
            Err(SecretError::Malformed)
        ));
    }

    #[dialog_common::test]
    async fn empty_and_large_payloads_roundtrip() {
        let profile = signer(11).await;
        let seal = profile.ed25519_did().secret(VAULT);

        for payload in [vec![], vec![0xABu8; 4096]] {
            let sealed = seal.conceal(&payload).await.unwrap();
            assert_eq!(
                profile.secret(VAULT).reveal(&sealed).await.unwrap(),
                payload
            );
        }
    }
}

// Cross-session tests: the signer is archived, dropped, and restored before
// revealing. On the browser this is where the agreement key has to survive,
// since a non-extractable key cannot re-derive it from a seed.
/// Derivation under the archive shape WebKit forces.
///
/// WebKit cannot deserialize an X25519 `CryptoKey` -- `structuredClone`
/// throws `TypeError: Unable to deserialize data`, verified against a real
/// WebKit build -- so a profile there archives its agreement key AES-KW
/// wrapped instead (see [`AgreementArchive::Wrapped`](crate::key::AgreementArchive)).
/// Derivation reads that key, so it has to come back through the wrap and
/// derive the same value it did before.
///
/// CI runs wasm tests in Chromium, which clones X25519 keys happily and so
/// never takes this path on its own. `assume_x25519_keys_are_cloneable`
/// forces it, which is what makes the WebKit-only behaviour testable here.
#[cfg(all(test, target_arch = "wasm32", target_os = "unknown"))]
mod web_tests {
    use super::*;
    use crate::ed25519::web::assume_x25519_keys_are_cloneable;
    use crate::key::KeyExport;
    use js_sys::Reflect;
    use wasm_bindgen::JsValue;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    const VAULT: Context = Context::new("dialog/vault/v1");

    /// The vector from `tests::derivation_matches_a_known_vector`, which the
    /// wrapped path must reach too.
    const EXPECTED: [u8; 32] = [
        0x14, 0xaa, 0x7e, 0x0e, 0x1f, 0x45, 0x62, 0xb6, 0xf4, 0xdc, 0x84, 0xdc, 0x26, 0x0c, 0x81,
        0x07, 0xef, 0x76, 0xb3, 0x08, 0x52, 0x7e, 0xb4, 0x7b, 0x8a, 0x2f, 0xdc, 0x6b, 0xe7, 0x7a,
        0xf0, 0xa8,
    ];

    /// Makes this thread behave like WebKit, and probes again once dropped.
    struct Uncloneable;

    impl Uncloneable {
        fn assume() -> Self {
            assume_x25519_keys_are_cloneable(Some(false));
            Self
        }
    }

    impl Drop for Uncloneable {
        fn drop(&mut self) {
            assume_x25519_keys_are_cloneable(None);
        }
    }

    #[dialog_common::test]
    async fn derivation_survives_the_wrapped_archive() {
        let _webkit = Uncloneable::assume();

        let signer = Ed25519Signer::import(&[42u8; 32]).await.unwrap();
        let before = signer
            .secret(VAULT)
            .derive_bytes(b"operator")
            .await
            .unwrap();
        assert_eq!(
            before, EXPECTED,
            "the wrapped archive must not change the derived value"
        );

        // Through the shape storage sees, as IndexedDB would hand it back.
        let export = signer.export().await.unwrap();
        assert!(
            matches!(
                &export,
                KeyExport::NonExtractable {
                    agreement: Some(crate::key::AgreementArchive::Wrapped(_)),
                    ..
                }
            ),
            "this test is only meaningful if the wrapped path was actually taken"
        );

        let archived: JsValue = export.into();
        let restored = Ed25519Signer::import(KeyExport::try_from(archived).unwrap())
            .await
            .unwrap();

        assert_eq!(
            restored
                .secret(VAULT)
                .derive_bytes(b"operator")
                .await
                .unwrap(),
            EXPECTED,
            "a profile restored from a wrapped archive must derive the same operator"
        );
    }

    /// A profile archived before agreement keys existed has no seed to
    /// re-derive one from, so derivation fails cleanly instead of deriving
    /// something wrong. The signature-based path this replaced would have
    /// derived an operator here -- a different one on every call, in Safari.
    #[dialog_common::test]
    async fn derivation_without_an_agreement_key_fails_cleanly() {
        let signer = Ed25519Signer::import(&[42u8; 32]).await.unwrap();
        let archived: JsValue = signer.export().await.unwrap().into();

        // An export written before the agreement component existed.
        Reflect::delete_property(&archived.clone().into(), &"agreementKey".into()).unwrap();
        let restored = Ed25519Signer::import(KeyExport::try_from(archived).unwrap())
            .await
            .unwrap();

        assert!(
            matches!(
                restored.secret(VAULT).derive_bytes(b"operator").await,
                Err(SecretError::AgreementKeyUnavailable)
            ),
            "a profile with no agreement key must fail, not derive"
        );
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use crate::key::KeyExport;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    const VAULT: Context = Context::new("dialog/vault/v1");

    #[dialog_common::test]
    async fn secret_survives_a_storage_roundtrip() {
        // Session 1: a profile is generated and archived.
        let archived: KeyExport = {
            let profile = Ed25519Signer::generate().await.unwrap();
            let did = profile.ed25519_did().to_string();
            let export = profile.export().await.unwrap();
            // The account seals to the DID it was given; it never sees the key.
            let sealed = did
                .parse::<Ed25519Verifier>()
                .unwrap()
                .secret(VAULT)
                .conceal(&[42u8; 32])
                .await
                .unwrap();
            // Stash the sealed secret for the next session.
            SEALED.with(|s| *s.borrow_mut() = Some(sealed));
            export
        };

        // Session 2: the profile is restored from storage and opens it.
        let restored = Ed25519Signer::import(archived).await.unwrap();
        let sealed = SEALED.with(|s| s.borrow_mut().take()).unwrap();

        assert_eq!(
            restored.secret(VAULT).reveal(&sealed).await.unwrap(),
            [42u8; 32],
            "a restored profile should open a secret sealed to its DID"
        );
    }

    #[dialog_common::test]
    async fn secret_sealed_after_restore_still_opens() {
        // Seal *after* the restore too: the restored key must work as a
        // recipient in both directions, not just for previously sealed data.
        let export = Ed25519Signer::generate()
            .await
            .unwrap()
            .export()
            .await
            .unwrap();
        let restored = Ed25519Signer::import(export).await.unwrap();

        let sealed = restored
            .ed25519_did()
            .secret(VAULT)
            .conceal(b"after restore")
            .await
            .unwrap();

        assert_eq!(
            restored.secret(VAULT).reveal(&sealed).await.unwrap(),
            b"after restore"
        );
    }

    #[dialog_common::test]
    async fn secret_survives_two_storage_roundtrips() {
        // Archives get rewritten; make sure the agreement key is not lost on a
        // second pass through storage.
        let first = Ed25519Signer::generate().await.unwrap();
        let sealed = first
            .ed25519_did()
            .secret(VAULT)
            .conceal(b"twice stored")
            .await
            .unwrap();

        let once = Ed25519Signer::import(first.export().await.unwrap())
            .await
            .unwrap();
        let twice = Ed25519Signer::import(once.export().await.unwrap())
            .await
            .unwrap();

        assert_eq!(
            twice.ed25519_did().to_string(),
            first.ed25519_did().to_string()
        );
        assert_eq!(
            twice.secret(VAULT).reveal(&sealed).await.unwrap(),
            b"twice stored"
        );
    }

    #[dialog_common::test]
    async fn seed_imported_signer_survives_a_storage_roundtrip() {
        // The other construction path: a signer imported from a seed rather
        // than generated.
        let signer = Ed25519Signer::import(&[13u8; 32]).await.unwrap();
        let sealed = signer
            .ed25519_did()
            .secret(VAULT)
            .conceal(b"from seed")
            .await
            .unwrap();

        let restored = Ed25519Signer::import(signer.export().await.unwrap())
            .await
            .unwrap();

        assert_eq!(
            restored.secret(VAULT).reveal(&sealed).await.unwrap(),
            b"from seed"
        );
    }

    thread_local! {
        /// Carries a sealed secret between the two "sessions" of a test.
        static SEALED: std::cell::RefCell<Option<SealedSecret>> =
            const { std::cell::RefCell::new(None) };
    }
}

// Interop: native uses RustCrypto, the browser uses WebCrypto. They cannot run
// in one process, so both sides are pinned against the same fixed vectors --
// each platform opens a secret the other produced, and both agree on the
// derived key for a fixed input.
#[cfg(test)]
mod interop_tests {
    use super::*;
    use crate::ed25519::{X25519PublicKey, X25519SecretKey};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    const VAULT: Context = Context::new("dialog/vault/v1");

    /// The recipient seed used by every fixture below.
    const RECIPIENT_SEED: [u8; 32] = [200u8; 32];

    #[dialog_common::test]
    async fn key_agreement_matches_across_platforms() {
        // Both platforms must derive the same shared secret for fixed inputs;
        // everything above this depends on it.
        let recipient = Ed25519Signer::import(&RECIPIENT_SEED).await.unwrap();
        let recipient_public = X25519PublicKey::from_ed25519(recipient.ed25519_did())
            .await
            .unwrap();

        // Pinned: the recipient's agreement key derived from its Ed25519 DID.
        assert_eq!(
            recipient_public.to_bytes(),
            [
                0x26, 0xf5, 0x23, 0x71, 0x00, 0x4f, 0x64, 0xa0, 0x59, 0xec, 0x36, 0xd5, 0x60, 0xab,
                0x0b, 0x21, 0x0c, 0xc1, 0x13, 0x38, 0xe5, 0x85, 0x63, 0x81, 0xea, 0xae, 0x80, 0x8f,
                0x1d, 0x6c, 0x04, 0x0c,
            ],
            "recipient agreement key derived from the DID must be stable"
        );
    }

    #[dialog_common::test]
    async fn opens_a_secret_sealed_on_the_other_platform() {
        // A sealed secret generated once and checked into the test. Whichever
        // platform produced it, the other must open it -- this is the real
        // cross-platform proof.
        let recipient = Ed25519Signer::import(&RECIPIENT_SEED).await.unwrap();

        let sealed = SealedSecret::from_bytes(&FIXTURE).unwrap();

        assert_eq!(
            recipient.secret(VAULT).reveal(&sealed).await.unwrap(),
            PLAINTEXT,
            "a secret sealed on the other platform must open here"
        );
    }

    /// Plaintext held by [`FIXTURE`].
    const PLAINTEXT: &[u8] = b"cross-platform vault key";

    /// A sealed secret produced for `RECIPIENT_SEED` in context
    /// `dialog/vault/v1`. Regenerate with `print_fixture` if the format changes.
    const FIXTURE: [u8; 84] = [
        0x1f, 0x09, 0xe2, 0xbc, 0x6e, 0xb3, 0xf9, 0xbc, 0xac, 0x9a, 0xc7, 0x34, 0xc5, 0x9b, 0x24,
        0xed, 0xe0, 0x03, 0xc2, 0xdc, 0xcd, 0x29, 0xc6, 0x91, 0x06, 0x5c, 0x36, 0x86, 0xd9, 0x60,
        0x1e, 0x3f, 0x75, 0xad, 0xcf, 0x9b, 0x19, 0x7a, 0xe9, 0x3e, 0xc9, 0x49, 0xbb, 0x80, 0x13,
        0xc2, 0x1a, 0x8b, 0x63, 0x65, 0x05, 0x38, 0xf9, 0xeb, 0x87, 0x73, 0x80, 0x21, 0xae, 0xe9,
        0xec, 0xc6, 0x55, 0x45, 0x0c, 0x52, 0x50, 0x3c, 0xab, 0xc1, 0x6e, 0x23, 0xd6, 0x35, 0xa7,
        0xf3, 0xc5, 0xab, 0xfb, 0x43, 0x80, 0x96, 0xf9, 0x73,
    ];

    /// Prints a fixture for the constants above.
    ///
    /// Ignored by default; run explicitly to regenerate after a format change.
    #[dialog_common::test]
    #[ignore = "regenerates the interop fixture; run explicitly"]
    async fn print_fixture() {
        let recipient = Ed25519Signer::import(&RECIPIENT_SEED).await.unwrap();
        let sealed = recipient
            .ed25519_did()
            .secret(VAULT)
            .conceal(PLAINTEXT)
            .await
            .unwrap();

        let bytes = sealed.to_bytes();
        let rendered = bytes
            .iter()
            .map(|b| format!("0x{b:02x}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("recipient agreement key: {:?}", {
            let k: X25519SecretKey = recipient.agreement_key().await.unwrap();
            k.public_key().to_bytes()
        });
        println!("FIXTURE ({} bytes): [{}]", bytes.len(), rendered);
    }
}
