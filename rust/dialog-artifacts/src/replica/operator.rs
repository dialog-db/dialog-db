pub use super::Replica;
use super::principal::Principal;
use super::{
    Formatter, PlatformBackend, ReplicaError, SECRET_KEY_LENGTH, Signature, SignerMut, SigningKey,
};
use async_trait::async_trait;
pub use dialog_capability::Did;
use dialog_capability::{Authority, Principal as PrincipalTrait, SignError};

/// Operator represents some authorized principal that can operate one or many
/// replicas through the authorization session. Currently it is used to sign
/// writes to remotes, but in the future I expect to also be used for signing
/// db commits to provide provenance.
///
/// Operator also offers an entry point interface for working with replicas by
/// providing `open` method that is used to open specific replica.
#[derive(Clone, PartialEq, Eq)]
pub struct Operator {
    key: SigningKey,
    principal: Principal,
}
impl std::fmt::Debug for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did())
    }
}

impl Operator {
    /// Creates a new issuer from a passphrase by hashing it to derive a signing key.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let bytes = passphrase.as_bytes();
        Self::from_secret(blake3::hash(bytes).as_bytes())
    }
    /// Creates a new issuer from a secret key.
    pub fn from_secret(secret: &[u8; SECRET_KEY_LENGTH]) -> Self {
        Operator::new(SigningKey::from_bytes(secret))
    }
    /// Creates a new issuer from a signing key.
    pub fn new(key: SigningKey) -> Self {
        let principal = Principal::new(key.verifying_key().to_bytes());

        Self { key, principal }
    }
    /// Generates a new issuer with a random signing key.
    pub fn generate() -> Result<Self, ReplicaError> {
        Ok(Self::new(SigningKey::generate(&mut rand::thread_rng())))
    }

    /// Signs a payload with this issuer's signing key.
    pub fn sign(&mut self, payload: &[u8]) -> Signature {
        self.key.sign(payload)
    }

    /// Returns the DID (Decentralized Identifier) for this issuer.
    pub fn did(&self) -> &Did {
        self.principal.did()
    }

    /// Returns the principal (public key bytes) for this issuer.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the raw secret key bytes.
    pub fn secret_key_bytes(&self) -> [u8; SECRET_KEY_LENGTH] {
        self.key.to_bytes()
    }

    /// Opens a replica with this operator acting as an issuer. If replice with
    /// a given `subject` already persisted in the given `backend` loads it,
    /// otherwise creates one and persists it in the given `backend`.
    pub fn open<Backend: PlatformBackend + 'static>(
        &self,
        subject: impl Into<Did>,
        backend: Backend,
    ) -> Result<Replica<Backend>, ReplicaError> {
        Replica::open(self.clone(), subject.into(), backend)
    }
}

impl PrincipalTrait for Operator {
    fn did(&self) -> &Did {
        self.principal.did()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Authority for Operator {
    async fn sign(&mut self, payload: &[u8]) -> Result<Vec<u8>, SignError> {
        Ok(self.key.sign(payload).to_bytes().to_vec())
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        Some(self.key.to_bytes())
    }
}
