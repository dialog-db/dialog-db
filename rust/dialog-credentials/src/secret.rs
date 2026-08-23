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

use crate::ed25519::{Ed25519Signer, Ed25519Verifier, X25519PublicKey, X25519SecretKey};

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
pub struct Secret<'a> {
    signer: &'a Ed25519Signer,
    context: Context,
}

impl Secret<'_> {
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

impl Ed25519Signer {
    /// Seal and open secrets for this identity, scoped to `context`.
    #[must_use]
    pub const fn secret(&self, context: Context) -> Secret<'_> {
        Secret {
            signer: self,
            context,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    const VAULT: Context = Context::new("dialog/vault/v1");
    const OTHER: Context = Context::new("dialog/other/v1");

    async fn signer(seed: u8) -> Ed25519Signer {
        Ed25519Signer::import(&[seed; 32]).await.unwrap()
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
