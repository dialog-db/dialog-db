use std::hash::{Hash, Hasher};

use dialog_capability::{Capability, Did, Issuer, Policy, Principal, Provider, credential};
use dialog_credentials::{Ed25519Signer, Ed25519Verifier};
use dialog_varsig::Signer as VarsigSigner;
use dialog_varsig::eddsa::Ed25519Signature;

use super::RepositoryError;

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
}

impl PartialEq for Credentials {
    fn eq(&self, other: &Self) -> bool {
        self.did() == other.did()
    }
}

impl Eq for Credentials {}

impl Hash for Credentials {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.did().hash(state);
    }
}

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

impl Issuer for Credentials {
    type Signature = Ed25519Signature;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Identify> for Credentials {
    async fn execute(
        &self,
        _input: Capability<credential::Identify>,
    ) -> Result<Did, credential::CredentialError> {
        Ok(Principal::did(self))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Sign> for Credentials {
    async fn execute(
        &self,
        input: Capability<credential::Sign>,
    ) -> Result<Vec<u8>, credential::CredentialError> {
        let payload = credential::Sign::of(&input).payload.as_slice();
        let sig: Ed25519Signature = VarsigSigner::sign(self, payload)
            .await
            .map_err(|e| credential::CredentialError::SigningFailed(e.to_string()))?;
        Ok(sig.to_bytes().to_vec())
    }
}
