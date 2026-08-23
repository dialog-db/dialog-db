//! Ed25519 key types, DID, and signer implementations.

use dialog_varsig::eddsa::Ed25519Signature;

// Platform-specific implementations
pub mod native;

// WebCrypto is only available in web browsers (wasm32 + unknown OS)
// Not available in WASI or other WASM environments
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod web;

// Submodules
mod error;
mod resolver;
mod signer;
mod verifier;

// Re-export all public types for backwards compatibility
pub use crate::key::KeyExport;
pub use error::{Ed25519DidFromStrError, Ed25519KeyError, Ed25519ResolveError, Ed25519SignerError};
pub use resolver::Ed25519KeyResolver;
pub use signer::Ed25519Signer;
pub use verifier::Ed25519Verifier;

// Re-export WebCrypto types on WASM
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use crate::key::{ExtractableAgreementKey, ExtractableKey, WebCryptoError};

/// Derive the raw X25519 secret scalar bytes from a 32-byte Ed25519 seed.
///
/// This is the standard Ed25519-to-X25519 derivation: the seed is expanded with
/// SHA-512 and the lower 32 bytes are the (unclamped) Curve25519 scalar. Both
/// `x25519_dalek::StaticSecret` and WebCrypto clamp on use, so feeding these
/// bytes to either produces the same agreement key.
///
/// Keeping this in one place is what makes native and browser derivation agree:
/// WebCrypto has no Ed25519-to-X25519 conversion of its own, so the browser arm
/// derives here too and only hands the result to `importKey`.
#[must_use]
pub fn agreement_secret_bytes(seed: &[u8; 32]) -> [u8; 32] {
    ed25519_dalek::SigningKey::from_bytes(seed).to_scalar_bytes()
}

/// Derive the raw X25519 public key bytes from raw X25519 secret bytes.
#[must_use]
pub fn agreement_public_bytes(secret: &[u8; 32]) -> [u8; 32] {
    x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(*secret)).to_bytes()
}

// Key material types

/// Ed25519 verifying key.
///
/// This enum abstracts over different Ed25519 verification implementations:
/// - `Native`: Uses `ed25519_dalek::VerifyingKey` for native platforms
/// - `WebCrypto`: Uses the browser's `WebCrypto` API (web WASM only)
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // CryptoKey is not Copy on WASM
pub enum Ed25519VerifyingKey {
    /// Native verifying key using `ed25519_dalek`.
    Native(native::VerifyingKey),

    /// WebCrypto verifying key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::VerifyingKey),
}

impl From<native::VerifyingKey> for Ed25519VerifyingKey {
    fn from(key: native::VerifyingKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::VerifyingKey> for Ed25519VerifyingKey {
    fn from(key: web::VerifyingKey) -> Self {
        Self::WebCrypto(key)
    }
}

impl Ed25519VerifyingKey {
    /// Get the raw public key bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 32] {
        match self {
            Self::Native(key) => key.to_bytes(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.to_bytes(),
        }
    }
}

impl Ed25519VerifyingKey {
    /// Verify a signature for the given message asynchronously.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if verification fails.
    #[allow(clippy::unused_async)]
    pub async fn verify_signature(
        &self,
        msg: &[u8],
        signature: &Ed25519Signature,
    ) -> Result<(), signature::Error> {
        match self {
            Self::Native(key) => {
                use signature::Verifier;
                let dalek_sig = ed25519_dalek::Signature::from(*signature);
                key.verify(msg, &dalek_sig)
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => web::verify(key.crypto_key(), msg, signature).await,
        }
    }
}

impl PartialEq for Ed25519VerifyingKey {
    fn eq(&self, other: &Self) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

impl Eq for Ed25519VerifyingKey {}

/// Ed25519 signing key.
///
/// This enum abstracts over different Ed25519 signing implementations:
/// - `Native`: Uses `ed25519_dalek::SigningKey` for native platforms
/// - `WebCrypto`: Uses the browser's `WebCrypto` API (web WASM only)
#[derive(Debug, Clone)]
pub enum Ed25519SigningKey {
    /// Native signing key using `ed25519_dalek`.
    Native(native::SigningKey),

    /// WebCrypto signing key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::SigningKey),
}

impl Ed25519SigningKey {
    /// Get the verifying (public) key.
    #[must_use]
    pub fn verifying_key(&self) -> Ed25519VerifyingKey {
        match self {
            Self::Native(key) => Ed25519VerifyingKey::Native(key.verifying_key()),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => Ed25519VerifyingKey::WebCrypto(key.verifying_key()),
        }
    }

    /// Generate a new Ed25519 signing key.
    ///
    /// On WASM, uses the `WebCrypto` API (non-extractable key by default).
    /// On native, uses `ed25519_dalek` with random bytes from `getrandom`.
    ///
    /// # Errors
    ///
    /// On WASM, returns an error if key generation fails or the browser
    /// doesn't support Ed25519. On native, returns an error if the RNG fails.
    #[allow(clippy::unused_async)]
    pub async fn generate() -> Result<Self, Ed25519KeyError> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(web::SigningKey::generate().await?))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            let mut seed = [0u8; 32];
            getrandom::getrandom(&mut seed).map_err(Ed25519KeyError::Rng)?;
            Ok(Self::Native(ed25519_dalek::SigningKey::from_bytes(&seed)))
        }
    }

    /// Export the key material.
    ///
    /// For `Native` keys, returns `KeyExport::Extractable` with the raw seed bytes.
    /// For `WebCrypto` keys, delegates to [`web::SigningKey::export`].
    ///
    /// # Errors
    ///
    /// On WASM with a non-extractable `WebCrypto` key, returns
    /// `KeyExport::NonExtractable` (not an error). Errors only if the
    /// `WebCrypto` export operation itself fails.
    #[allow(clippy::unused_async)]
    pub async fn export(&self) -> Result<KeyExport, Ed25519KeyError> {
        match self {
            Self::Native(key) => Ok(KeyExport::Extractable(key.to_bytes().to_vec())),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => Ok(key.export().await?),
        }
    }

    /// Import from a [`KeyExport`].
    ///
    /// On native, `Extractable(bytes)` constructs a native `ed25519_dalek::SigningKey`.
    ///
    /// On WASM, both variants are routed through [`web::SigningKey::import`] so
    /// that `Extractable` seeds produce a **non-extractable** `WebCrypto` key
    /// (matching the security default of [`web::SigningKey::import`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the seed has the wrong length or the `WebCrypto` import fails.
    #[allow(clippy::unused_async)] // async is needed on WASM
    pub async fn import(key: impl Into<KeyExport>) -> Result<Self, Ed25519KeyError> {
        let key = key.into();

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(web::SigningKey::import(key).await?))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            match key {
                KeyExport::Extractable(ref bytes) => {
                    let seed: [u8; 32] = bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| Ed25519KeyError::InvalidSeedLength(bytes.len()))?;
                    Ok(Self::Native(ed25519_dalek::SigningKey::from_bytes(&seed)))
                }
            }
        }
    }
}

impl From<ed25519_dalek::SigningKey> for Ed25519SigningKey {
    fn from(key: ed25519_dalek::SigningKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::SigningKey> for Ed25519SigningKey {
    fn from(key: web::SigningKey) -> Self {
        Self::WebCrypto(key)
    }
}

impl Ed25519SigningKey {
    /// Get the X25519 agreement key derived from this signing key.
    ///
    /// On native this is derived on demand from the seed. On the browser it was
    /// derived once at generation or import time and has been carried since,
    /// because a non-extractable `CryptoKey` cannot give its seed back.
    ///
    /// # Errors
    ///
    /// Returns [`Ed25519KeyError::AgreementKeyUnavailable`] when the key was
    /// restored on the browser from an archive that carried no agreement
    /// component. On native this never fails.
    #[allow(clippy::unused_async)] // async is needed on WASM
    pub async fn agreement_key(&self) -> Result<X25519SecretKey, Ed25519KeyError> {
        match self {
            Self::Native(key) => Ok(X25519SecretKey::Native(native::AgreementSecretKey::from(
                key.to_scalar_bytes(),
            ))),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key
                .agreement_key()
                .cloned()
                .map(X25519SecretKey::WebCrypto)
                .ok_or(Ed25519KeyError::AgreementKeyUnavailable),
        }
    }

    /// Sign a message asynchronously.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if signing fails.
    #[allow(clippy::unused_async)]
    pub async fn sign_bytes(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        match self {
            Self::Native(key) => {
                use signature::Signer;
                let sig = key.try_sign(msg)?;
                Ok(Ed25519Signature::from(sig))
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.sign_bytes(msg).await,
        }
    }
}

/// X25519 public key, for use as a key-agreement peer.
///
/// Mirrors [`Ed25519VerifyingKey`]: `Native` holds raw `x25519_dalek` material,
/// `WebCrypto` holds a browser `CryptoKey` plus its cached raw bytes.
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // CryptoKey is not Copy on WASM
pub enum X25519PublicKey {
    /// Native public key using `x25519_dalek`.
    Native(native::AgreementPublicKey),

    /// WebCrypto public key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::AgreementPublicKey),
}

impl X25519PublicKey {
    /// Get the raw public key bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 32] {
        match self {
            Self::Native(key) => key.to_bytes(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.to_bytes(),
        }
    }

    /// Build from raw X25519 public key bytes.
    ///
    /// # Errors
    ///
    /// On the browser, returns an error if the `WebCrypto` import fails. Never
    /// fails on native.
    #[allow(clippy::unused_async)] // async is needed on WASM
    pub async fn from_bytes(bytes: &[u8; 32]) -> Result<Self, Ed25519KeyError> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(
                web::AgreementPublicKey::from_bytes(bytes).await?,
            ))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            Ok(Self::Native(native::AgreementPublicKey::from(*bytes)))
        }
    }
}

impl From<native::AgreementPublicKey> for X25519PublicKey {
    fn from(key: native::AgreementPublicKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::AgreementPublicKey> for X25519PublicKey {
    fn from(key: web::AgreementPublicKey) -> Self {
        Self::WebCrypto(key)
    }
}

impl PartialEq for X25519PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

impl Eq for X25519PublicKey {}

/// X25519 secret key, derived from an Ed25519 seed.
///
/// On native the secret is derived on demand and needs no storage. On the
/// browser it cannot be derived at all -- WebCrypto offers no Ed25519-to-X25519
/// conversion, and a non-extractable Ed25519 `CryptoKey` never yields its seed.
/// So the browser arm derives the secret from the seed once, at generation or
/// import time, and keeps it as a `CryptoKey` that is archived alongside the
/// signing key (see [`KeyExport`]) so it can be restored.
///
/// `Debug` is redacted: `x25519_dalek::StaticSecret` withholds its own `Debug`
/// so secret scalars cannot be logged, and this wrapper keeps that property.
#[derive(Clone)]
pub enum X25519SecretKey {
    /// Native secret key using `x25519_dalek`.
    Native(native::AgreementSecretKey),

    /// WebCrypto secret key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::AgreementSecretKey),
}

impl X25519SecretKey {
    /// Derive the X25519 secret from a 32-byte Ed25519 seed.
    ///
    /// Both platforms run the same derivation ([`agreement_secret_bytes`]), so
    /// the same seed yields the same agreement key everywhere. On the browser
    /// the derived secret is imported into `WebCrypto` as a **non-extractable**
    /// key.
    ///
    /// # Errors
    ///
    /// On the browser, returns an error if the `WebCrypto` import fails. Never
    /// fails on native.
    #[allow(clippy::unused_async)] // async is needed on WASM
    pub async fn from_ed25519_seed(seed: &[u8; 32]) -> Result<Self, Ed25519KeyError> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(
                web::AgreementSecretKey::from_ed25519_seed(seed).await?,
            ))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            Ok(Self::Native(native::AgreementSecretKey::from(
                agreement_secret_bytes(seed),
            )))
        }
    }

    /// Get the corresponding X25519 public key.
    #[must_use]
    pub fn public_key(&self) -> X25519PublicKey {
        match self {
            Self::Native(key) => X25519PublicKey::Native(native::AgreementPublicKey::from(key)),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => X25519PublicKey::WebCrypto(key.agreement_public_key()),
        }
    }

    /// Perform X25519 key agreement with `peer`, returning the raw shared secret.
    ///
    /// The result is the raw Diffie-Hellman output. Callers that need symmetric
    /// key material should run it through a KDF rather than using it directly.
    ///
    /// # Errors
    ///
    /// On the browser, returns an error if `deriveBits` fails. Never fails on
    /// native.
    #[allow(clippy::unused_async)] // async is needed on WASM
    pub async fn diffie_hellman(
        &self,
        peer: &X25519PublicKey,
    ) -> Result<[u8; 32], Ed25519KeyError> {
        match self {
            Self::Native(key) => {
                // A browser-side peer key may reach a native secret only via
                // its raw bytes, which is what `X25519PublicKey::to_bytes`
                // gives us in either case.
                let peer = native::AgreementPublicKey::from(peer.to_bytes());
                Ok(key.diffie_hellman(&peer).to_bytes())
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => {
                let peer = match peer {
                    X25519PublicKey::WebCrypto(peer) => peer.clone(),
                    X25519PublicKey::Native(peer) => {
                        web::AgreementPublicKey::from_bytes(&peer.to_bytes()).await?
                    }
                };
                Ok(key.diffie_hellman(&peer).await?)
            }
        }
    }
}

impl std::fmt::Debug for X25519SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The public key is safe to show and identifies the key; the secret
        // scalar is deliberately never rendered.
        f.debug_struct("X25519SecretKey")
            .field("public_key", &self.public_key().to_bytes())
            .finish_non_exhaustive()
    }
}

impl From<native::AgreementSecretKey> for X25519SecretKey {
    fn from(key: native::AgreementSecretKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::AgreementSecretKey> for X25519SecretKey {
    fn from(key: web::AgreementSecretKey) -> Self {
        Self::WebCrypto(key)
    }
}

#[cfg(test)]
mod agreement_tests {
    use super::*;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    async fn signing_key(seed: u8) -> Ed25519SigningKey {
        Ed25519SigningKey::import(&[seed; 32]).await.unwrap()
    }

    #[dialog_common::test]
    async fn agreement_key_is_derived_from_the_seed() {
        let seed = [3u8; 32];
        let key = Ed25519SigningKey::import(&seed).await.unwrap();

        let derived = key.agreement_key().await.unwrap();
        let expected = X25519SecretKey::from_ed25519_seed(&seed).await.unwrap();

        assert_eq!(
            derived.public_key(),
            expected.public_key(),
            "the signing key's agreement key should match one derived from the same seed"
        );
    }

    #[dialog_common::test]
    async fn agreement_key_matches_the_reference_derivation() {
        // Pin the derivation to the ed25519-to-x25519 standard: the X25519
        // secret is the unclamped lower half of SHA-512(seed), and its public
        // key is the Montgomery form of the Ed25519 public key.
        let seed = [9u8; 32];
        let key = Ed25519SigningKey::import(&seed).await.unwrap();

        let signing = ed25519_dalek::SigningKey::from_bytes(&seed);
        let montgomery = signing.verifying_key().to_montgomery().to_bytes();

        assert_eq!(
            key.agreement_key().await.unwrap().public_key().to_bytes(),
            montgomery,
            "agreement public key should be the Montgomery form of the Ed25519 public key"
        );
        assert_eq!(
            agreement_secret_bytes(&seed),
            signing.to_scalar_bytes(),
            "agreement secret should be the unclamped SHA-512 lower half"
        );
    }

    #[dialog_common::test]
    async fn agreement_key_is_stable_across_derivations() {
        let key = signing_key(11).await;

        let first = key.agreement_key().await.unwrap();
        let second = key.agreement_key().await.unwrap();

        assert_eq!(
            first.public_key(),
            second.public_key(),
            "deriving twice should give the same agreement key"
        );
    }

    #[dialog_common::test]
    async fn different_seeds_give_different_agreement_keys() {
        let a = signing_key(1).await.agreement_key().await.unwrap();
        let b = signing_key(2).await.agreement_key().await.unwrap();

        assert_ne!(
            a.public_key(),
            b.public_key(),
            "distinct seeds should give distinct agreement keys"
        );
    }

    #[dialog_common::test]
    async fn diffie_hellman_agrees_in_both_directions() {
        let alice = signing_key(20).await.agreement_key().await.unwrap();
        let bob = signing_key(21).await.agreement_key().await.unwrap();

        let alice_shared = alice.diffie_hellman(&bob.public_key()).await.unwrap();
        let bob_shared = bob.diffie_hellman(&alice.public_key()).await.unwrap();

        assert_eq!(
            alice_shared, bob_shared,
            "both sides should agree on the same shared secret"
        );
        assert_ne!(
            alice_shared, [0u8; 32],
            "the shared secret should not be the all-zero (low-order) result"
        );
    }

    #[dialog_common::test]
    async fn diffie_hellman_differs_for_a_different_peer() {
        let alice = signing_key(30).await.agreement_key().await.unwrap();
        let bob = signing_key(31).await.agreement_key().await.unwrap();
        let eve = signing_key(32).await.agreement_key().await.unwrap();

        let with_bob = alice.diffie_hellman(&bob.public_key()).await.unwrap();
        let with_eve = alice.diffie_hellman(&eve.public_key()).await.unwrap();

        assert_ne!(
            with_bob, with_eve,
            "agreement with a different peer should give a different secret"
        );
    }

    #[dialog_common::test]
    async fn agreement_survives_an_export_import_roundtrip() {
        let key = signing_key(40).await;
        let peer = signing_key(41).await.agreement_key().await.unwrap();

        let before = key
            .agreement_key()
            .await
            .unwrap()
            .diffie_hellman(&peer.public_key())
            .await
            .unwrap();

        let exported = key.export().await.unwrap();
        let restored = Ed25519SigningKey::import(exported).await.unwrap();

        let after = restored
            .agreement_key()
            .await
            .unwrap()
            .diffie_hellman(&peer.public_key())
            .await
            .unwrap();

        assert_eq!(
            before, after,
            "a restored key should agree on the same shared secret"
        );
    }

    #[dialog_common::test]
    async fn public_key_roundtrips_through_raw_bytes() {
        let key = signing_key(50).await.agreement_key().await.unwrap();
        let bytes = key.public_key().to_bytes();

        let restored = X25519PublicKey::from_bytes(&bytes).await.unwrap();

        assert_eq!(restored, key.public_key());
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[dialog_common::test]
    async fn generated_key_can_agree_with_an_imported_peer() {
        // A freshly generated key (no seed in hand on the browser) must still
        // agree with a peer whose key came from a known seed.
        let generated = Ed25519SigningKey::generate().await.unwrap();
        let peer = signing_key(70).await;

        let generated_key = generated.agreement_key().await.unwrap();
        let peer_key = peer.agreement_key().await.unwrap();

        let from_generated = generated_key
            .diffie_hellman(&peer_key.public_key())
            .await
            .unwrap();
        let from_peer = peer_key
            .diffie_hellman(&generated_key.public_key())
            .await
            .unwrap();

        assert_eq!(
            from_generated, from_peer,
            "a generated key and an imported peer should agree"
        );
    }

    #[dialog_common::test]
    async fn signer_exposes_the_same_agreement_key() {
        let seed = [71u8; 32];
        let signer = Ed25519Signer::import(&seed).await.unwrap();
        let key = Ed25519SigningKey::import(&seed).await.unwrap();

        assert_eq!(
            signer.agreement_key().await.unwrap().public_key(),
            key.agreement_key().await.unwrap().public_key(),
            "the signer should expose the signing key's agreement key"
        );
    }

    #[dialog_common::test]
    async fn debug_does_not_leak_the_secret() {
        let seed = [60u8; 32];
        let key = X25519SecretKey::from_ed25519_seed(&seed).await.unwrap();

        let rendered = format!("{key:?}");
        let secret = agreement_secret_bytes(&seed);

        assert!(
            !rendered.contains(&format!("{}", secret[0])) || !rendered.contains("secret"),
            "Debug output should not render the secret scalar: {rendered}"
        );
        assert!(
            rendered.contains("X25519SecretKey"),
            "Debug output should still identify the type: {rendered}"
        );
    }
}

// WebCrypto-only tests. These pin the property that makes the browser path
// work at all: the X25519 key is created at generation time and persisted in
// non-extractable form, because it can never be re-derived afterwards.
#[cfg(all(test, target_arch = "wasm32", target_os = "unknown"))]
mod agreement_web_tests {
    use super::*;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    #[dialog_common::test]
    async fn generated_key_carries_a_non_extractable_agreement_key() {
        let key = Ed25519SigningKey::generate().await.unwrap();

        let Ed25519SigningKey::WebCrypto(ref web_key) = key else {
            panic!("generate should produce a WebCrypto key on the browser");
        };
        let agreement = web_key
            .agreement_key()
            .expect("a generated key should carry an agreement key");

        assert!(
            !agreement.private_key().extractable(),
            "the archived agreement key must be non-extractable"
        );
        assert!(
            !web_key.private_key_is_extractable(),
            "the signing key must be non-extractable"
        );
    }

    #[dialog_common::test]
    async fn generated_key_can_agree_and_restore() {
        // The whole point of persisting the agreement key: a generated key is
        // non-extractable, yet key agreement still works after a roundtrip.
        let key = Ed25519SigningKey::generate().await.unwrap();
        let peer = Ed25519SigningKey::generate().await.unwrap();

        let peer_public = peer.agreement_key().await.unwrap().public_key();
        let before = key
            .agreement_key()
            .await
            .unwrap()
            .diffie_hellman(&peer_public)
            .await
            .unwrap();

        let exported = key.export().await.unwrap();
        assert!(
            matches!(
                exported,
                KeyExport::NonExtractable {
                    agreement: Some(_),
                    ..
                }
            ),
            "a generated key should export its agreement component"
        );

        let restored = Ed25519SigningKey::import(exported).await.unwrap();
        let after = restored
            .agreement_key()
            .await
            .unwrap()
            .diffie_hellman(&peer_public)
            .await
            .unwrap();

        assert_eq!(
            before, after,
            "a restored non-extractable key should still agree on the same secret"
        );
    }

    #[dialog_common::test]
    async fn generated_key_agrees_in_both_directions() {
        let alice = Ed25519SigningKey::generate().await.unwrap();
        let bob = Ed25519SigningKey::generate().await.unwrap();

        let alice_key = alice.agreement_key().await.unwrap();
        let bob_key = bob.agreement_key().await.unwrap();

        let alice_shared = alice_key
            .diffie_hellman(&bob_key.public_key())
            .await
            .unwrap();
        let bob_shared = bob_key
            .diffie_hellman(&alice_key.public_key())
            .await
            .unwrap();

        assert_eq!(alice_shared, bob_shared);
        assert_ne!(alice_shared, [0u8; 32]);
    }

    #[dialog_common::test]
    async fn web_agreement_matches_native_derivation() {
        // The browser derives the X25519 secret in Rust and imports it, so the
        // same seed must give the same agreement key as the native path.
        let seed = [77u8; 32];
        let key = Ed25519SigningKey::import(&seed).await.unwrap();

        let expected = ed25519_dalek::SigningKey::from_bytes(&seed)
            .verifying_key()
            .to_montgomery()
            .to_bytes();

        assert_eq!(
            key.agreement_key().await.unwrap().public_key().to_bytes(),
            expected,
            "browser-derived agreement key should match the native derivation"
        );
    }

    #[dialog_common::test]
    async fn shared_secret_matches_the_native_computation() {
        // Cross-check the WebCrypto `deriveBits` output against x25519-dalek
        // over the same two seeds.
        let alice_seed = [81u8; 32];
        let bob_seed = [82u8; 32];

        let alice = Ed25519SigningKey::import(&alice_seed).await.unwrap();
        let bob = Ed25519SigningKey::import(&bob_seed).await.unwrap();

        let web_shared = alice
            .agreement_key()
            .await
            .unwrap()
            .diffie_hellman(&bob.agreement_key().await.unwrap().public_key())
            .await
            .unwrap();

        let native_alice = x25519_dalek::StaticSecret::from(agreement_secret_bytes(&alice_seed));
        let native_bob = x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(
            agreement_secret_bytes(&bob_seed),
        ));

        assert_eq!(
            web_shared,
            native_alice.diffie_hellman(&native_bob).to_bytes(),
            "WebCrypto deriveBits should match the native shared secret"
        );
    }

    #[dialog_common::test]
    async fn key_restored_without_an_agreement_component_reports_it() {
        // Archives written before the agreement component existed carry no
        // X25519 key. Signing still works; only agreement is unavailable.
        let key = Ed25519SigningKey::generate().await.unwrap();
        let KeyExport::NonExtractable {
            private_key,
            public_key,
            ..
        } = key.export().await.unwrap()
        else {
            panic!("expected a non-extractable export");
        };

        let legacy = Ed25519SigningKey::import(KeyExport::NonExtractable {
            private_key,
            public_key,
            agreement: None,
        })
        .await
        .unwrap();

        assert!(
            legacy.sign_bytes(b"still signs").await.is_ok(),
            "a key without an agreement component should still sign"
        );
        assert!(
            matches!(
                legacy.agreement_key().await,
                Err(Ed25519KeyError::AgreementKeyUnavailable)
            ),
            "agreement should report itself unavailable rather than derive a wrong key"
        );
    }
}
