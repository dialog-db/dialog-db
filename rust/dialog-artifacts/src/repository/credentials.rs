use std::fmt;
use std::hash::{Hash, Hasher};

use dialog_capability::authorization::Authorized;
use dialog_capability::{
    Capability, Constraint, Did, Effect, Issuer, Policy, Principal, Provider, credential,
};
use dialog_common::ConditionalSync;
use dialog_credentials::{Ed25519Signer, Ed25519Verifier};
use dialog_s3_credentials::capability::S3Request;
use dialog_s3_credentials::s3::Credentials as S3Credentials;
use dialog_s3_credentials::s3::site::S3Access;
use dialog_varsig::Signer as VarsigSigner;
use dialog_varsig::eddsa::Ed25519Signature;

#[cfg(feature = "ucan")]
use dialog_common::ConditionalSend;
#[cfg(feature = "ucan")]
use dialog_s3_credentials::ucan::{DelegationChain, UcanAccess, authorize as ucan_authorize};

use super::RepositoryError;

/// Credentials for operating on repositories.
///
/// Wraps an optional `Ed25519Signer` for signing, a cached DID for identity,
/// optional UCAN delegations, and a generic `Store` for persistence.
///
/// Authorization (S3, UCAN) is delegated to the `Store`. In-memory UCAN
/// delegations can be provided alongside the store for immediate use.
pub struct Credentials<Store> {
    /// Signing key (identity). None when only the DID is known.
    signer: Option<Ed25519Signer>,
    /// Cached DID — always populated at construction time.
    did: Did,
    /// In-memory UCAN delegation chains.
    #[cfg(feature = "ucan")]
    delegations: Vec<DelegationChain>,
    /// Credential storage backend.
    store: Store,
}

impl<Store: Clone> Clone for Credentials<Store> {
    fn clone(&self) -> Self {
        Self {
            signer: self.signer.clone(),
            did: self.did.clone(),
            #[cfg(feature = "ucan")]
            delegations: self.delegations.clone(),
            store: self.store.clone(),
        }
    }
}

impl<Store> fmt::Debug for Credentials<Store> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("did", &self.did)
            .field("has_signer", &self.signer.is_some())
            .finish_non_exhaustive()
    }
}

impl<Store> Credentials<Store> {
    /// Creates credentials from a signer and a store.
    pub fn new(signer: Ed25519Signer, store: Store) -> Self {
        let did = Principal::did(&signer);
        Self {
            signer: Some(signer),
            did,
            #[cfg(feature = "ucan")]
            delegations: Vec::new(),
            store,
        }
    }

    /// Creates credentials from a passphrase and a store.
    pub async fn from_passphrase(passphrase: &str, store: Store) -> Result<Self, RepositoryError> {
        let bytes = blake3::hash(passphrase.as_bytes());
        let signer = Ed25519Signer::import(bytes.as_bytes())
            .await
            .map_err(|e| RepositoryError::StorageError(format!("{:?}", e)))?;
        Ok(Self::new(signer, store))
    }

    /// Creates credentials from a DID and a store (no signing capability).
    pub fn from_did(did: Did, store: Store) -> Self {
        Self {
            signer: None,
            did,
            #[cfg(feature = "ucan")]
            delegations: Vec::new(),
            store,
        }
    }

    /// Add in-memory delegation chains for UCAN authorization.
    #[cfg(feature = "ucan")]
    pub fn with_delegations(mut self, delegations: Vec<DelegationChain>) -> Self {
        self.delegations = delegations;
        self
    }

    /// Returns the DID (Decentralized Identifier) for these credentials.
    pub fn did(&self) -> Did {
        self.did.clone()
    }

    /// Returns the verifier (public key identity), if a signer is available.
    pub fn verifier(&self) -> Option<&Ed25519Verifier> {
        self.signer.as_ref().map(|s| s.ed25519_did())
    }

    /// Returns a reference to the inner signer, if available.
    pub fn signer(&self) -> Option<&Ed25519Signer> {
        self.signer.as_ref()
    }

    /// Returns a reference to the store.
    pub fn store(&self) -> &Store {
        &self.store
    }
}

impl<Store> PartialEq for Credentials<Store> {
    fn eq(&self, other: &Self) -> bool {
        self.did == other.did
    }
}

impl<Store> Eq for Credentials<Store> {}

impl<Store> Hash for Credentials<Store> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.did.hash(state);
    }
}

impl<Store> Principal for Credentials<Store> {
    fn did(&self) -> Did {
        self.did.clone()
    }
}

impl<Store: ConditionalSync> VarsigSigner<Ed25519Signature> for Credentials<Store> {
    async fn sign(&self, payload: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        match &self.signer {
            Some(signer) => signer.sign(payload).await,
            None => Err(signature::Error::new()),
        }
    }
}

impl<Store: ConditionalSync> Issuer for Credentials<Store> {
    type Signature = Ed25519Signature;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Store: ConditionalSync> Provider<credential::Identify> for Credentials<Store> {
    async fn execute(
        &self,
        _input: Capability<credential::Identify>,
    ) -> Result<credential::Identity, credential::CredentialError> {
        Ok(credential::Identity {
            profile: self.did.clone(),
            operator: self.did.clone(),
            account: None,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Store: ConditionalSync> Provider<credential::Sign> for Credentials<Store> {
    async fn execute(
        &self,
        input: Capability<credential::Sign>,
    ) -> Result<Vec<u8>, credential::CredentialError> {
        let signer = self.signer.as_ref().ok_or_else(|| {
            credential::CredentialError::SigningFailed("no signer available".into())
        })?;
        let payload = credential::Sign::of(&input).payload.as_slice();
        let sig: Ed25519Signature = VarsigSigner::sign(signer, payload)
            .await
            .map_err(|e| credential::CredentialError::SigningFailed(e.to_string()))?;
        Ok(sig.to_bytes().to_vec())
    }
}

// S3 Authorization: delegate to the store.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Store, Fx> Provider<credential::Authorize<Fx, S3Access>> for Credentials<Store>
where
    Fx: Effect + Clone + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: S3Request,
    Store: Provider<credential::Authorize<Fx, S3Access>> + ConditionalSync,
{
    async fn execute(
        &self,
        input: credential::Authorize<Fx, S3Access>,
    ) -> Result<Authorized<Fx, S3Access>, credential::AuthorizeError> {
        self.store.execute(input).await
    }
}

// UCAN Authorization: check in-memory delegations first, then delegate to store.
#[cfg(feature = "ucan")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Store: ConditionalSync, Fx> Provider<credential::Authorize<Fx, UcanAccess>>
    for Credentials<Store>
where
    Fx: Effect + Constraint + Clone + ConditionalSend + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
{
    async fn execute(
        &self,
        input: credential::Authorize<Fx, UcanAccess>,
    ) -> Result<Authorized<Fx, UcanAccess>, credential::AuthorizeError> {
        let authority_did = &self.did;
        let endpoint = input.access.endpoint.clone();
        let subject_did = input.capability.subject().clone();

        let delegation = if subject_did == *authority_did {
            None
        } else {
            let chain = self
                .delegations
                .iter()
                .find(|c| c.audience() == authority_did)
                .cloned()
                .ok_or_else(|| {
                    credential::AuthorizeError::Configuration(format!(
                        "No delegation chain for audience '{}'",
                        authority_did
                    ))
                })?;
            Some(chain)
        };

        ucan_authorize(self, delegation, endpoint, input.capability).await
    }
}

// Import S3 credentials — delegate to store.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Store> Provider<credential::Import<S3Credentials>> for Credentials<Store>
where
    Store: Provider<credential::Import<S3Credentials>> + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Import<S3Credentials>>,
    ) -> Result<(), credential::CredentialError> {
        self.store.execute(input).await
    }
}

// Import UCAN delegation chains — delegate to store.
#[cfg(feature = "ucan")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Store> Provider<credential::Import<DelegationChain>> for Credentials<Store>
where
    Store: Provider<credential::Import<DelegationChain>> + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Import<DelegationChain>>,
    ) -> Result<(), credential::CredentialError> {
        self.store.execute(input).await
    }
}
