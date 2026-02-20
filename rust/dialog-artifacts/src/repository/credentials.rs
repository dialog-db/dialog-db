use dialog_capability::{Authority, Did, Principal};
use dialog_credentials::{Ed25519Signer, Ed25519Verifier};
use dialog_varsig::Signer as VarsigSigner;
use dialog_varsig::eddsa::Ed25519Signature;

use super::{PlatformBackend, Repository, RepositoryError};

/// Credentials for operating on repositories.
///
/// Wraps `Ed25519Signer` from dialog-credentials, adding repository-specific
/// convenience constructors.
#[derive(Clone, Debug)]
pub struct Credentials(Ed25519Signer);

impl Credentials {
    /// Creates credentials from a passphrase by hashing it to derive a signing key.
    pub async fn from_passphrase(passphrase: &str) -> Result<Self, RepositoryError> {
        let bytes = blake3::hash(passphrase.as_bytes());
        Ed25519Signer::import(bytes.as_bytes())
            .await
            .map(Self)
            .map_err(|e| RepositoryError::StorageError(format!("{:?}", e)))
    }

    /// Returns the DID (Decentralized Identifier) for these credentials.
    pub fn did(&self) -> Did {
        Principal::did(&self.0)
    }

    /// Returns the verifier (public key identity) for these credentials.
    pub fn verifier(&self) -> &Ed25519Verifier {
        self.0.ed25519_did()
    }

    /// Opens a repository with these credentials acting as an issuer.
    ///
    /// If a repository with the given `subject` is already persisted in the given
    /// `backend`, loads it; otherwise creates one and persists it.
    pub fn open<Backend: PlatformBackend + 'static>(
        &self,
        subject: impl Into<Did>,
        backend: Backend,
    ) -> Result<Repository<Backend>, RepositoryError> {
        Repository::open(self.clone(), subject.into(), backend)
    }
}

impl PartialEq for Credentials {
    fn eq(&self, other: &Self) -> bool {
        self.did() == other.did()
    }
}

impl Eq for Credentials {}

impl From<Ed25519Signer> for Credentials {
    fn from(signer: Ed25519Signer) -> Self {
        Self(signer)
    }
}

impl From<Credentials> for Ed25519Signer {
    fn from(creds: Credentials) -> Self {
        creds.0
    }
}

impl Principal for Credentials {
    fn did(&self) -> Did {
        Principal::did(&self.0)
    }
}

impl VarsigSigner<Ed25519Signature> for Credentials {
    async fn sign(&self, payload: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        self.0.sign(payload).await
    }
}

impl Authority for Credentials {
    type Signature = Ed25519Signature;
}
