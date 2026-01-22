pub use super::Replica;
use super::principal::Principal;
use super::{Formatter, PlatformBackend, ReplicaError, SECRET_KEY_LENGTH, Signature, SignerMut, SigningKey};
pub use dialog_common::capability::Did;

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

    pub fn open<Backend: PlatformBackend + 'static>(
        &self,
        subject: impl Into<Did>,
        backend: Backend,
    ) -> Result<Replica<Backend>, ReplicaError> {
        Replica::open(self.clone(), subject.into(), backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_common;
    use dialog_storage::{CborEncoder, MemoryStorageBackend};

    #[dialog_common::test]
    async fn it_opens_repository() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();

        let operator = Operator::from_passphrase("secret");
        let subject = Operator::from_passphrase("repo").did().to_string();
        let _repository = operator.open(subject, backend)?;

        Ok(())
    }
}
