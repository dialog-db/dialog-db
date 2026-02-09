//! Ed25519 signer implementation.

use super::{Ed25519SigningKey, error::Ed25519SignerError, verifier::Ed25519Verifier};
use crate::key::KeyExport;
use serde::Serialize;
use varsig::{Did, Principal, Signer, eddsa::Ed25519Signature};

// Re-import WebCrypto types on WASM
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use super::web;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::key::{ExtractableKey, WebCryptoError};

/// An `Ed25519` `did:key` signer.
///
/// This is the unified signer that works on both native and WASM platforms.
/// On native platforms, it wraps an `ed25519_dalek::SigningKey`.
/// On WASM, it can also wrap a `WebCrypto` `CryptoKey` for non-extractable key support.
#[derive(Debug, Clone)]
pub struct Ed25519Signer {
    did: Ed25519Verifier,
    signer: Ed25519SigningKey,
}

impl From<Ed25519SigningKey> for Ed25519Signer {
    fn from(signer: Ed25519SigningKey) -> Self {
        let did = Ed25519Verifier::from(signer.verifying_key());
        Self { did, signer }
    }
}

impl Ed25519Signer {
    /// Generate a new Ed25519 keypair.
    ///
    /// On WASM, uses the `WebCrypto` API (non-extractable key by default).
    /// On native, uses `ed25519_dalek` with random bytes from `getrandom`.
    ///
    /// # Errors
    ///
    /// On WASM, returns an error if key generation fails or the browser
    /// doesn't support Ed25519. On native, returns an error if the RNG fails.
    pub async fn generate() -> Result<Self, Ed25519SignerError> {
        Ok(Ed25519SigningKey::generate().await?.into())
    }

    /// Import a keypair from a [`KeyExport`].
    ///
    /// Accepts anything that converts `Into<KeyExport>`, including `&[u8; 32]`.
    ///
    /// # Errors
    ///
    /// Returns an error if the seed has the wrong length or the `WebCrypto` import fails.
    pub async fn import(key: impl Into<KeyExport>) -> Result<Self, Ed25519SignerError> {
        let signing_key = Ed25519SigningKey::import(key).await?;
        Ok(signing_key.into())
    }

    /// Export the key material.
    ///
    /// # Errors
    ///
    /// Returns an error if the `WebCrypto` export operation fails.
    pub async fn export(&self) -> Result<KeyExport, Ed25519SignerError> {
        Ok(self.signer.export().await?)
    }

    /// Get the associated Ed25519 DID (verifier).
    #[must_use]
    pub const fn ed25519_did(&self) -> &Ed25519Verifier {
        &self.did
    }

    /// Get the inner signing key.
    #[must_use]
    pub const fn signing_key(&self) -> &Ed25519SigningKey {
        &self.signer
    }
}

impl From<ed25519_dalek::SigningKey> for Ed25519Signer {
    fn from(key: ed25519_dalek::SigningKey) -> Self {
        Ed25519SigningKey::from(key).into()
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::SigningKey> for Ed25519Signer {
    fn from(key: web::SigningKey) -> Self {
        Ed25519SigningKey::from(key).into()
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl ExtractableKey for Ed25519Signer {
    async fn generate() -> Result<Self, WebCryptoError> {
        let key = <web::SigningKey as ExtractableKey>::generate().await?;
        Ok(Ed25519SigningKey::from(key).into())
    }

    async fn import(key: impl Into<KeyExport>) -> Result<Self, WebCryptoError> {
        let key = <web::SigningKey as ExtractableKey>::import(key).await?;
        Ok(Ed25519SigningKey::from(key).into())
    }

    async fn export(&self) -> Result<KeyExport, WebCryptoError> {
        match &self.signer {
            Ed25519SigningKey::WebCrypto(key) => {
                <web::SigningKey as ExtractableKey>::export(key).await
            }
            Ed25519SigningKey::Native(key) => Ok(KeyExport::Extractable(key.to_bytes().to_vec())),
        }
    }
}

impl std::fmt::Display for Ed25519Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did)
    }
}

// Signer impl for Ed25519Signer
impl Signer<Ed25519Signature> for Ed25519Signer {
    async fn sign(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        self.signer.sign_bytes(msg).await
    }
}

// Principal impl for Ed25519Signer
impl Principal for Ed25519Signer {
    fn did(&self) -> Did {
        self.did.did()
    }
}

// Authority impl — combines Principal + Signer into a single trait
use dialog_capability::Authority;

impl Authority for Ed25519Signer {
    type Signature = Ed25519Signature;
}

impl Serialize for Ed25519Signer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.did.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a deterministic test signer from a seed.
    async fn test_signer(seed: u8) -> Ed25519Signer {
        Ed25519Signer::import(&[seed; 32]).await.unwrap()
    }

    #[dialog_common::test]
    async fn ed25519_did_round_trip() {
        let signer = test_signer(0).await;
        let did_string = signer.ed25519_did().to_string();
        let parsed: Ed25519Verifier = did_string.parse().unwrap();
        assert_eq!(parsed, signer.ed25519_did().clone());
    }

    #[dialog_common::test]
    async fn ed25519_varsig_signer_produces_valid_signature() {
        let signer = test_signer(42).await;
        let msg = b"test message for async signing";

        let signature = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer, msg)
            .await
            .unwrap();

        let did = signer.ed25519_did();
        <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(did, msg, &signature)
            .await
            .unwrap();
    }

    #[dialog_common::test]
    async fn ed25519_varsig_signer_different_messages_different_signatures() {
        let signer = test_signer(7).await;
        let msg1 = b"first message";
        let msg2 = b"second message";

        let sig1 = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer, msg1)
            .await
            .unwrap();
        let sig2 = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer, msg2)
            .await
            .unwrap();

        assert_ne!(
            sig1, sig2,
            "Different messages should produce different signatures"
        );

        let did = signer.ed25519_did();
        <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(did, msg1, &sig1)
            .await
            .unwrap();
        <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(did, msg2, &sig2)
            .await
            .unwrap();
    }

    #[dialog_common::test]
    async fn ed25519_varsig_signer_wrong_message_fails_verification() {
        let signer = test_signer(99).await;
        let msg = b"original message";
        let wrong_msg = b"tampered message";

        let signature = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer, msg)
            .await
            .unwrap();

        let did = signer.ed25519_did();
        assert!(
            <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
                did, wrong_msg, &signature
            )
            .await
            .is_err(),
            "Verification should fail for wrong message"
        );
    }

    #[dialog_common::test]
    async fn different_signers_produce_different_signatures() {
        let signer1 = test_signer(1).await;
        let signer2 = test_signer(2).await;
        let msg = b"same message";

        let sig1 = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer1, msg)
            .await
            .unwrap();
        let sig2 = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer2, msg)
            .await
            .unwrap();

        assert_ne!(sig1, sig2);

        assert!(
            <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
                signer1.ed25519_did(),
                msg,
                &sig1
            )
            .await
            .is_ok()
        );
        assert!(
            <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
                signer2.ed25519_did(),
                msg,
                &sig2
            )
            .await
            .is_ok()
        );

        // Cross-verification should fail
        assert!(
            <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
                signer1.ed25519_did(),
                msg,
                &sig2
            )
            .await
            .is_err()
        );
        assert!(
            <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
                signer2.ed25519_did(),
                msg,
                &sig1
            )
            .await
            .is_err()
        );
    }

    #[dialog_common::test]
    async fn export_import_roundtrip_preserves_did() {
        let signer = test_signer(77).await;
        let original_did = signer.ed25519_did().to_string();

        let exported = signer.export().await.unwrap();
        let restored = Ed25519Signer::import(exported).await.unwrap();

        assert_eq!(
            restored.ed25519_did().to_string(),
            original_did,
            "Restored signer should have the same DID"
        );
    }

    #[dialog_common::test]
    async fn export_import_roundtrip_produces_valid_signatures() {
        let signer = test_signer(88).await;
        let msg = b"roundtrip signing test";

        let exported = signer.export().await.unwrap();
        let restored = Ed25519Signer::import(exported).await.unwrap();

        let signature = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&restored, msg)
            .await
            .unwrap();
        <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
            signer.ed25519_did(),
            msg,
            &signature,
        )
        .await
        .expect("Original verifier should accept signature from restored signer");
    }

    #[dialog_common::test]
    async fn export_import_roundtrip_seed_bytes_match() {
        let seed = [55u8; 32];
        let signer = Ed25519Signer::import(&seed).await.unwrap();

        let exported = signer.export().await.unwrap();
        match exported {
            KeyExport::Extractable(ref bytes) => {
                assert_eq!(
                    bytes.as_slice(),
                    &seed,
                    "Exported seed should match original"
                );
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            KeyExport::NonExtractable { .. } => {
                // On the web the default import creates non-extractable keys,
                // so we just verify the DID roundtrips instead.
                let restored = Ed25519Signer::import(exported).await.unwrap();
                assert_eq!(
                    restored.ed25519_did().to_string(),
                    signer.ed25519_did().to_string()
                );
            }
        }
    }

    #[dialog_common::test]
    async fn double_export_import_roundtrip() {
        let signer = test_signer(66).await;

        let exported1 = signer.export().await.unwrap();
        let restored1 = Ed25519Signer::import(exported1).await.unwrap();

        let exported2 = restored1.export().await.unwrap();
        let restored2 = Ed25519Signer::import(exported2).await.unwrap();

        assert_eq!(
            restored2.ed25519_did().to_string(),
            signer.ed25519_did().to_string(),
            "Double roundtrip should preserve DID"
        );

        let msg = b"double roundtrip";
        let sig = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&restored2, msg)
            .await
            .unwrap();
        <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
            signer.ed25519_did(),
            msg,
            &sig,
        )
        .await
        .expect("Original verifier should accept double-roundtripped signature");
    }
}

// WebCrypto-only tests (extractable keys, non-extractable public key extraction)
#[cfg(all(test, target_arch = "wasm32", target_os = "unknown"))]
mod web_tests {
    use super::*;

    #[dialog_common::test]
    async fn generate_signer_succeeds() {
        let signer = Ed25519Signer::generate().await;
        assert!(
            signer.is_ok(),
            "Failed to generate signer: {:?}",
            signer.err()
        );
    }

    #[dialog_common::test]
    async fn generated_signer_has_valid_did() {
        let signer = Ed25519Signer::generate().await.unwrap();
        let did_string = signer.ed25519_did().to_string();

        assert!(
            did_string.starts_with("did:key:z"),
            "DID should start with 'did:key:z', got: {}",
            did_string
        );

        let parsed: Result<Ed25519Verifier, _> = did_string.parse();
        assert!(parsed.is_ok(), "DID should be parseable");
        assert_eq!(parsed.unwrap(), signer.ed25519_did().clone());
    }

    #[dialog_common::test]
    async fn generate_extractable_key() {
        let signer = <Ed25519Signer as ExtractableKey>::generate().await;
        assert!(signer.is_ok(), "Should be able to generate extractable key");

        let signer = signer.unwrap();
        let msg = b"test";
        let sig = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer, msg).await;
        assert!(sig.is_ok());
    }

    #[dialog_common::test]
    async fn import_extractable_key() {
        let seed = [42u8; 32];

        let signer = <Ed25519Signer as ExtractableKey>::import(&seed).await;
        assert!(
            signer.is_ok(),
            "Should be able to import extractable key: {:?}",
            signer.err()
        );

        let signer = signer.unwrap();
        let msg = b"test";
        let sig = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer, msg).await;
        assert!(sig.is_ok());
    }

    #[dialog_common::test]
    async fn non_extractable_key_can_extract_public_key() {
        let signer = Ed25519Signer::generate().await.unwrap();
        let did = signer.ed25519_did();
        let did_string = did.to_string();

        assert!(
            did_string.starts_with("did:key:z"),
            "DID should be valid: {}",
            did_string
        );

        let msg = b"test message for non-extractable key";
        let signature = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&signer, msg)
            .await
            .unwrap();

        let result =
            <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(did, msg, &signature)
                .await;
        assert!(
            result.is_ok(),
            "Public key from non-extractable key should verify signatures: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn extractable_export_import_roundtrip_preserves_seed() {
        let seed = [42u8; 32];
        let signer = <Ed25519Signer as ExtractableKey>::import(&seed)
            .await
            .unwrap();

        let exported = <Ed25519Signer as ExtractableKey>::export(&signer)
            .await
            .unwrap();
        match &exported {
            KeyExport::Extractable(bytes) => {
                assert_eq!(
                    bytes.as_slice(),
                    &seed,
                    "Extractable export should return the original seed"
                );
            }
            _ => panic!("Extractable key should export as Extractable"),
        }

        let restored = Ed25519Signer::import(exported).await.unwrap();
        assert_eq!(
            restored.ed25519_did().to_string(),
            signer.ed25519_did().to_string()
        );
    }

    #[dialog_common::test]
    async fn extractable_export_import_roundtrip_signs_correctly() {
        let seed = [99u8; 32];
        let signer = <Ed25519Signer as ExtractableKey>::import(&seed)
            .await
            .unwrap();
        let msg = b"extractable roundtrip signing test";

        let exported = <Ed25519Signer as ExtractableKey>::export(&signer)
            .await
            .unwrap();
        let restored = Ed25519Signer::import(exported).await.unwrap();

        let sig = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&restored, msg)
            .await
            .unwrap();
        <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
            signer.ed25519_did(),
            msg,
            &sig,
        )
        .await
        .expect("Original verifier should accept signature from restored signer");
    }

    #[dialog_common::test]
    async fn non_extractable_export_import_roundtrip() {
        let signer = Ed25519Signer::import(&[33u8; 32]).await.unwrap();
        let original_did = signer.ed25519_did().to_string();
        let msg = b"non-extractable roundtrip test";

        let exported = signer.export().await.unwrap();
        match &exported {
            KeyExport::NonExtractable { .. } => { /* expected */ }
            KeyExport::Extractable(_) => {
                panic!("Default import should create non-extractable key on WASM")
            }
        }

        let restored = Ed25519Signer::import(exported).await.unwrap();
        assert_eq!(
            restored.ed25519_did().to_string(),
            original_did,
            "Non-extractable roundtrip should preserve DID"
        );

        let sig = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&restored, msg)
            .await
            .unwrap();
        <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
            signer.ed25519_did(),
            msg,
            &sig,
        )
        .await
        .expect("Original verifier should accept non-extractable roundtrip signature");
    }

    #[dialog_common::test]
    async fn imported_non_extractable_key_matches_native_public_key() {
        let seed = [42u8; 32];

        let web_signer = Ed25519Signer::import(&seed).await.unwrap();

        let native_signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
        let native_did: Ed25519Verifier = native_signing_key.verifying_key().into();

        let web_did = web_signer.ed25519_did();

        assert_eq!(
            web_did, &native_did,
            "DID from WebCrypto import should match native derivation"
        );

        let msg = b"cross-platform verification test";
        let signature = <Ed25519Signer as Signer<Ed25519Signature>>::sign(&web_signer, msg)
            .await
            .unwrap();

        assert!(
            <Ed25519Verifier as varsig::Verifier<Ed25519Signature>>::verify(
                &native_did,
                msg,
                &signature
            )
            .await
            .is_ok(),
            "Native verifier should verify WebCrypto signature"
        );
    }
}
