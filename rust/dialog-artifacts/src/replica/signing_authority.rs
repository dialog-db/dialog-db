pub use super::Replica;
use super::principal::Principal;
use super::{Formatter, PlatformBackend, ReplicaError, SECRET_KEY_LENGTH, SignerMut, SigningKey};
use async_trait::async_trait;
pub use dialog_capability::Did;
use dialog_capability::{Authority, Principal as PrincipalTrait, SignError};
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
pub use ucan::WebCryptoEd25519Signer;

/// Re-export CryptoKey for storage purposes.
#[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
pub use web_sys::CryptoKey;

/// Trait for dynamic signers (escape hatch for custom implementations).
///
/// This trait is used by the `SigningAuthority::Dynamic` variant to allow custom
/// signing implementations at the cost of dynamic dispatch.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Signer: Send + Sync {
    /// Get the principal (public key) for this signer.
    fn principal(&self) -> &Principal;

    /// Sign a payload.
    async fn sign(&mut self, payload: &[u8]) -> Result<Vec<u8>, SignError>;

    /// Try to export the raw Ed25519 secret key bytes.
    ///
    /// Returns `Some([u8; 32])` if this signer supports key export,
    /// `None` otherwise (e.g., for non-extractable WebCrypto keys).
    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        None
    }
}

/// A signing authority that can operate replicas.
///
/// SigningAuthority provides signing capabilities with platform-specific implementations:
///
/// - **Native**: Uses `ed25519_dalek` directly (all platforms)
/// - **WebCrypto** (WASM + `webcrypto` feature): Uses WebCrypto API with non-extractable keys
/// - **Dynamic**: Escape hatch for custom signers (uses dynamic dispatch)
///
/// # Example
///
/// ```rust,no_run
/// use dialog_artifacts::replica::SigningAuthority;
///
/// async fn example() {
///     // Generate a new signing authority
///     let authority = SigningAuthority::generate().await.expect("should generate");
///
///     // Get the DID
///     println!("DID: {}", authority.did());
/// }
/// ```
#[derive(Clone)]
pub enum SigningAuthority {
    /// Native Ed25519 signing using ed25519_dalek (all platforms).
    Native {
        /// The principal (public key).
        principal: Principal,
        /// The signing key (boxed to reduce enum size).
        key: Box<SigningKey>,
    },

    /// WebCrypto Ed25519 with non-extractable keys (WASM + webcrypto feature).
    ///
    /// Wraps the `ucan::WebCryptoEd25519Signer` for UCAN delegation signing.
    /// Note: We generate non-extractable keys by default for security, but
    /// WebCrypto does support extractable keys if created with that option.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
    WebCrypto {
        /// The principal (public key).
        principal: Principal,
        /// The ucan WebCrypto signer (owns the non-extractable CryptoKey).
        signer: WebCryptoEd25519Signer,
    },

    /// Dynamic signer for custom implementations (escape hatch).
    ///
    /// Uses `Arc<RwLock<dyn Signer>>` to allow cloning and interior mutability.
    /// The principal is cached at construction time for efficient access.
    Dynamic {
        /// The principal (public key), cached at construction.
        principal: Principal,
        /// The underlying signer.
        signer: Arc<RwLock<dyn Signer>>,
    },
}

impl std::fmt::Debug for SigningAuthority {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did())
    }
}

impl PartialEq for SigningAuthority {
    fn eq(&self, other: &Self) -> bool {
        self.did() == other.did()
    }
}

impl Eq for SigningAuthority {}

// SAFETY: In WASM environments, we're single-threaded so Send+Sync are safe.
// The WebCrypto CryptoKey is a JS handle that doesn't implement Send+Sync,
// but since WASM is single-threaded, this is not a practical concern.
#[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
unsafe impl Send for SigningAuthority {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
unsafe impl Sync for SigningAuthority {}

impl SigningAuthority {
    /// Creates a new signing authority from a passphrase by hashing it to derive a signing key.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let bytes = passphrase.as_bytes();
        Self::from_secret(blake3::hash(bytes).as_bytes())
    }

    /// Creates a new signing authority from a secret key.
    pub fn from_secret(secret: &[u8; SECRET_KEY_LENGTH]) -> Self {
        Self::from_signing_key(SigningKey::from_bytes(secret))
    }

    /// Creates a new signing authority from a signing key.
    ///
    // TODO: Consider supporting import of signing keys into WebCrypto for better
    // security on WASM. This is awkward as each key type has a different format.
    // See: https://github.com/storacha/ucanto/blob/main/packages/principal/src/rsa.js#L129-L147
    pub fn from_signing_key(key: SigningKey) -> Self {
        let principal = Principal::new(key.verifying_key().to_bytes());

        Self::Native {
            principal,
            key: Box::new(key),
        }
    }

    /// Generates a new signing authority with a random signing key.
    ///
    /// On WASM with the `webcrypto` feature enabled, this will:
    /// 1. Try to generate a WebCrypto Ed25519 key pair with non-extractable keys
    /// 2. If WebCrypto fails, fall back to the `Native` variant with extractable keys
    ///
    /// On non-WASM platforms, this uses the native Ed25519 implementation.
    /// On WASM platforms without the `webcrypto` feature, this uses the native implementation.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
    pub async fn generate() -> Result<Self, ReplicaError> {
        match WebCryptoEd25519Signer::generate().await {
            Ok(signer) => Ok(SigningAuthority::from(signer)),
            Err(_) => {
                // WebCrypto Ed25519 not available, fall back to native
                Ok(Self::from_signing_key(SigningKey::generate(
                    &mut rand::thread_rng(),
                )))
            }
        }
    }

    /// Generates a new signing authority with a random signing key.
    ///
    /// On non-WASM platforms, this uses the native Ed25519 implementation.
    /// On WASM platforms without the `webcrypto` feature, this uses the native implementation.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto")))]
    pub async fn generate() -> Result<Self, ReplicaError> {
        Ok(Self::from_signing_key(SigningKey::generate(
            &mut rand::thread_rng(),
        )))
    }

    /// Creates a signing authority from a custom signer (escape hatch).
    ///
    /// This allows using custom signing implementations at the cost of
    /// dynamic dispatch. The principal is cached at construction time.
    pub fn from_signer(signer: impl Signer + 'static) -> Self {
        let principal = signer.principal().clone();
        Self::Dynamic {
            principal,
            signer: Arc::new(RwLock::new(signer)),
        }
    }

    /// Returns the WebCrypto signer if this is a WebCrypto signing authority.
    ///
    /// This is useful for accessing the underlying `CryptoKey` for storage.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a WebCrypto signing authority.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
    pub fn webcrypto_signer(&self) -> Result<&WebCryptoEd25519Signer, ReplicaError> {
        match self {
            Self::WebCrypto { signer, .. } => Ok(signer),
            _ => Err(ReplicaError::InvalidState {
                message: "Not a WebCrypto signing authority".to_string(),
            }),
        }
    }

    /// Returns the DID (Decentralized Identifier) for this signing authority.
    pub fn did(&self) -> &Did {
        self.principal().did()
    }

    /// Returns the principal (public key) for this signing authority.
    pub fn principal(&self) -> &Principal {
        match self {
            Self::Native { principal, .. } => principal,

            #[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
            Self::WebCrypto { principal, .. } => principal,

            Self::Dynamic { principal, .. } => principal,
        }
    }

    /// Returns the raw secret key bytes, if available.
    ///
    /// Returns `None` for `WebCrypto` (non-extractable by default) and `Dynamic` variants
    /// that don't support key extraction.
    pub fn secret_key_bytes(&self) -> Option<[u8; SECRET_KEY_LENGTH]> {
        match self {
            Self::Native { key, .. } => Some(key.to_bytes()),

            #[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
            Self::WebCrypto { .. } => None, // Non-extractable by default

            Self::Dynamic { signer, .. } => {
                if let Ok(guard) = signer.try_read() {
                    guard.secret_key_bytes()
                } else {
                    None
                }
            }
        }
    }

    /// Returns true if this signing authority uses non-extractable keys.
    ///
    /// This is useful for determining whether the signing authority can be serialized
    /// or if it requires special handling (e.g., storing CryptoKey in IndexedDB).
    ///
    /// Note: For WebCrypto, we generate non-extractable keys by default for security,
    /// but WebCrypto does support extractable keys if created with that option.
    pub fn is_non_extractable(&self) -> bool {
        match self {
            Self::Native { .. } => false,

            #[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
            Self::WebCrypto { .. } => true, // We generate non-extractable by default

            Self::Dynamic { signer, .. } => {
                if let Ok(guard) = signer.try_read() {
                    guard.secret_key_bytes().is_none()
                } else {
                    true // Assume non-extractable if we can't check
                }
            }
        }
    }

    /// Opens a replica with this signing authority acting as an issuer.
    ///
    /// If a replica with the given `subject` is already persisted in the given
    /// `backend`, loads it; otherwise creates one and persists it.
    pub fn open<Backend: PlatformBackend + 'static>(
        &self,
        subject: impl Into<Did>,
        backend: Backend,
    ) -> Result<Replica<Backend>, ReplicaError> {
        Replica::open(self.clone(), subject.into(), backend)
    }
}

impl SigningAuthority {
    /// Creates a SigningAuthority from any Authority with extractable keys.
    ///
    /// This encapsulates the pattern of extracting secret key bytes and creating
    /// a SigningAuthority from them. Useful for remote operations that require
    /// a concrete SigningAuthority.
    ///
    /// # Errors
    ///
    /// Returns an error if the authority does not have extractable key material.
    pub fn try_from_authority<T: Authority>(authority: &T) -> Result<Self, ReplicaError> {
        match authority.secret_key_bytes() {
            Some(bytes) => Ok(SigningAuthority::from_secret(&bytes)),
            None => Err(ReplicaError::StorageError(
                "Remote operations require an authority with extractable key material".to_string(),
            )),
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
impl From<WebCryptoEd25519Signer> for SigningAuthority {
    fn from(signer: WebCryptoEd25519Signer) -> Self {
        // Get public key bytes from the Ed25519Did
        let public_key_bytes: [u8; 32] = *signer.did().0.as_bytes();
        let principal = Principal::new(public_key_bytes);
        Self::WebCrypto { principal, signer }
    }
}

impl PrincipalTrait for SigningAuthority {
    fn did(&self) -> &Did {
        SigningAuthority::did(self)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Authority for SigningAuthority {
    async fn sign(&mut self, payload: &[u8]) -> Result<Vec<u8>, SignError> {
        match self {
            Self::Native { key, .. } => Ok(key.sign(payload).to_bytes().to_vec()),

            #[cfg(all(target_arch = "wasm32", target_os = "unknown", feature = "webcrypto"))]
            Self::WebCrypto { signer, .. } => {
                use ucan::AsyncDidSigner;
                signer
                    .sign(payload)
                    .await
                    .map(|sig| sig.to_bytes().to_vec())
                    .map_err(|e| SignError::SigningFailed(e.to_string()))
            }

            Self::Dynamic { signer, .. } => {
                let mut guard = signer.write().await;
                guard.sign(payload).await
            }
        }
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        SigningAuthority::secret_key_bytes(self)
    }
}
