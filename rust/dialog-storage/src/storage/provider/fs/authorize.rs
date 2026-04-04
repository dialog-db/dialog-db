//! Authorization provider for filesystem storage.
//!
//! Implements [`ProofStore`](dialog_capability::access::ProofStore) for [`FileStore`]
//! and `Provider<Save<P>>` for storing permits.
//!
//! Proofs are stored at:
//! - Subject-specific: `{root}/permit/{audience}/{subject}/{issuer}.{id}`
//! - Powerline (`sub: *`): `{root}/permit/{audience}/_/{issuer}.{id}`

use std::path::PathBuf;

use async_trait::async_trait;
use dialog_capability::access::{
    AuthorizeError, Claim, Delegation, ProofChain, ProofStore, Protocol, Save,
};
use dialog_capability::{Ability, Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_varsig::Did;

use super::FileStore;

fn permit_dir(fs: &FileStore) -> Result<PathBuf, AuthorizeError> {
    let location = fs
        .permit()
        .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
    PathBuf::try_from(location).map_err(|e| AuthorizeError::Configuration(e.to_string()))
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P: Protocol> ProofStore<P> for FileStore
where
    P::Proof: ConditionalSend,
{
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Proof>, AuthorizeError> {
        let base = permit_dir(self)?;
        let subject_segment = match subject {
            Some(did) => did.to_string(),
            None => "_".to_string(),
        };

        let dir = base.join(audience.to_string()).join(subject_segment);

        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut proofs = Vec::new();
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| AuthorizeError::Configuration(format!("Failed to list dir: {e}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AuthorizeError::Configuration(format!("Failed to read entry: {e}")))?
        {
            let ft = entry.file_type().await.map_err(|e| {
                AuthorizeError::Configuration(format!("Failed to get file type: {e}"))
            })?;

            if !ft.is_file() {
                continue;
            }

            let bytes = match tokio::fs::read(entry.path()).await {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(AuthorizeError::Configuration(format!(
                        "Failed to read proof: {e}"
                    )));
                }
            };

            match <P::Proof as Delegation>::decode(&bytes) {
                Ok(proof) => proofs.push(proof),
                Err(_) => continue,
            }
        }

        Ok(proofs)
    }

    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        let base = permit_dir(self)?;

        for proof in P::proofs(delegation) {
            let bytes = proof.encode()?;
            let id = base58::ToBase58::to_base58(blake3::hash(&bytes).as_bytes().as_slice());

            let audience = proof.audience().to_string();
            let subject_segment = match proof.subject() {
                Some(did) => did.to_string(),
                None => "_".to_string(),
            };
            let issuer = proof.issuer().to_string();

            let dir = base.join(&audience).join(&subject_segment);
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| AuthorizeError::Configuration(format!("Failed to create dir: {e}")))?;

            let path = dir.join(format!("{issuer}.{id}"));
            tokio::fs::write(&path, bytes).await.map_err(|e| {
                AuthorizeError::Configuration(format!("Failed to write proof: {e}"))
            })?;
        }

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P> Provider<Claim<P>> for FileStore
where
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Proof: Clone + ConditionalSend + ConditionalSync,
    P::ProofChain: ConditionalSend,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: dialog_capability::Capability<Claim<P>>,
    ) -> Result<P::ProofChain, AuthorizeError> {
        let auth = Claim::<P>::of(&input);
        let authorize = Claim::new(auth.by.clone(), auth.access.clone());
        ProofStore::<P>::authorize(self, authorize).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P> Provider<Save<P>> for FileStore
where
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: dialog_capability::Capability<Save<P>>,
    ) -> Result<(), AuthorizeError> {
        let delegation = &Save::<P>::of(&input).delegation;
        ProofStore::<P>::save(self, delegation).await
    }
}
