pub use super::Replica;
use super::principal::Principal;
use super::{Formatter, PlatformBackend, ReplicaError, SECRET_KEY_LENGTH, SigningKey};
use async_trait::async_trait;
pub use dialog_capability::Did;
use dialog_capability::{Authority, Principal as PrincipalTrait, SignError};
use ucan::Ed25519Signer;

/// A signing authority that can operate replicas.
///
/// `SigningAuthority` wraps an [`Ed25519Signer`] from the `ucan` crate, which
/// provides a unified signing API across native (`ed25519_dalek`) and WASM
/// (`WebCrypto`) platforms. A cached [`Principal`] is kept alongside for
/// efficient DID lookups.
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
pub struct SigningAuthority {
    /// The principal (public key with cached DID).
    principal: Principal,
    /// The underlying UCAN signer (handles both native and WebCrypto).
    signer: Ed25519Signer,
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

    /// Creates a new signing authority from an `ed25519_dalek` signing key.
    pub fn from_signing_key(key: SigningKey) -> Self {
        let principal = Principal::new(key.verifying_key().to_bytes());
        let signer = Ed25519Signer::from(key);
        Self { principal, signer }
    }

    /// Creates a new signing authority from a [`Ed25519Signer`].
    pub fn from_ucan_signer(signer: Ed25519Signer) -> Self {
        let public_key_bytes: [u8; 32] = signer.did().0.to_bytes();
        let principal = Principal::new(public_key_bytes);
        Self { principal, signer }
    }

    /// Generates a new signing authority with a random signing key.
    ///
    /// On WASM, the underlying [`Ed25519Signer`] uses the `WebCrypto` API
    /// with non-extractable keys. On native platforms, it uses `ed25519_dalek`.
    pub async fn generate() -> Result<Self, ReplicaError> {
        let signer = Ed25519Signer::generate()
            .await
            .map_err(|e| ReplicaError::StorageError(format!("Key generation failed: {e}")))?;
        Ok(Self::from_ucan_signer(signer))
    }

    /// Returns the DID (Decentralized Identifier) for this signing authority.
    pub fn did(&self) -> &Did {
        self.principal().did()
    }

    /// Returns the principal (public key) for this signing authority.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the underlying UCAN [`Ed25519Signer`].
    ///
    /// This is useful for building UCAN delegations and invocations directly.
    pub fn ucan_signer(&self) -> &Ed25519Signer {
        &self.signer
    }

    /// Returns the raw secret key bytes, if available.
    ///
    /// Returns `None` for WebCrypto keys (non-extractable by default on WASM).
    pub fn secret_key_bytes(&self) -> Option<[u8; SECRET_KEY_LENGTH]> {
        use varsig::signature::eddsa::Ed25519SigningKey;
        match self.signer.signer() {
            Ed25519SigningKey::Native(key) => Some(key.to_bytes()),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Ed25519SigningKey::WebCrypto(_) => None,
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

impl From<Ed25519Signer> for SigningAuthority {
    fn from(signer: Ed25519Signer) -> Self {
        Self::from_ucan_signer(signer)
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
        use async_signature::AsyncSigner;
        self.signer
            .signer()
            .sign_async(payload)
            .await
            .map(|sig| sig.to_bytes().to_vec())
            .map_err(|e| SignError::SigningFailed(e.to_string()))
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        SigningAuthority::secret_key_bytes(self)
    }
}
