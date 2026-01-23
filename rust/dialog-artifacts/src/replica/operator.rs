pub use super::Replica;
use super::principal::Principal;
use super::remote::RemoteCredentials;
use super::{
    Formatter, PlatformBackend, RemoteSite, ReplicaError, SECRET_KEY_LENGTH, Signature, SignerMut,
    SigningKey,
};
pub use dialog_common::capability::Did;
use dialog_common::capability::Principal as PrincipalTrait;
use dialog_common::Authority;

/// Represents a principal operating a replica.
#[derive(Clone, PartialEq, Eq)]
pub struct Operator {
    id: String,
    key: SigningKey,
    principal: Principal,
}
impl std::fmt::Debug for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.did())
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
        let principal = Principal(key.verifying_key().to_bytes());

        Self {
            id: principal.did(),
            key,
            principal,
        }
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
    pub fn did(&self) -> &str {
        &self.id
    }

    /// Returns the principal (public key bytes) for this issuer.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the raw secret key bytes.
    pub fn secret_key_bytes(&self) -> [u8; SECRET_KEY_LENGTH] {
        self.key.to_bytes()
    }

    pub fn open<Backend: PlatformBackend + 'static>(
        &self,
        subject: impl Into<Did>,
        backend: Backend,
    ) -> Result<impl Repository, ReplicaError> {
        Replica::open(self.clone(), subject.into(), backend)
    }
}

impl PrincipalTrait for Operator {
    fn did(&self) -> &Did {
        &self.id
    }
}

impl Authority for Operator {
    fn sign(&mut self, payload: &[u8]) -> Vec<u8> {
        self.key.sign(payload).to_bytes().to_vec()
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        Some(self.key.to_bytes())
    }
}

trait Repository {
    fn remotes(&self) -> impl Remotes;
}
impl<Backend: PlatformBackend> Repository for Replica<Backend> {
    fn remotes(&self) -> impl Remotes {
        self
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
trait Remotes {
    async fn add(
        &mut self,
        name: impl Into<String>,
        credentials: RemoteCredentials,
    ) -> Result<RemoteSite, ReplicaError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Backend: PlatformBackend> Remotes for Replica<Backend> {
    async fn add(
        &mut self,
        name: impl Into<String>,
        credentials: RemoteCredentials,
    ) -> Result<RemoteSite, ReplicaError> {
        let site = RemoteSite::new(name, credentials, self.issuer.clone());
        let address = format!("site/{}", site.name);
        let memory = self
            .storage
            .open::<RemoteSite>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        // Check if remote already exists with different config
        if let Some(existing) = memory.read().clone() {
            if existing.credentials != site.credentials {
                return Err(ReplicaError::RemoteAlreadyExists {
                    remote: site.name.clone(),
                });
            }
            // Already exists with same config, return it
            return Ok(existing);
        }

        // Store the new remote site
        memory
            .replace(Some(site.clone()), &self.storage)
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(site)
    }
}

#[cfg(test)]
mod tests {
    use super::super::remote::RemoteCredentials;
    use super::*;
    use dialog_common::{self, Blake3Hash};
    use dialog_storage::{CborEncoder, MemoryStorageBackend};

    #[dialog_common::test]
    async fn it_opens_repository() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();

        let operator = Operator::from_passphrase("secret");
        let subject = Operator::from_passphrase("repo").did().to_string();
        let repository = operator.open(subject, backend)?;

        let origin = repository
            .remotes()
            .add(
                "origin",
                RemoteCredentials::ucan("https://ucan.tonk.workers.dev", None),
            )
            .await?;

        let upstream = origin.repository(subject).branch("main");
        let index = upstream.index();

        index.get(b"hello");

        Ok(())
    }
}
