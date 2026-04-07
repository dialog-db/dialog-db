//! Access provider for filesystem storage.
//!
//! Implements [`ProofStore`](dialog_capability::access::ProofStore) for [`FileStore`]
//! and `Provider<Retain<P>>` for granting access.
//!
//! Proofs are stored at:
//! - Subject-specific: `{root}/permit/{audience}/{subject}/{issuer}.{id}`
//! - Powerline (`sub: *`): `{root}/permit/{audience}/_/{issuer}.{id}`

use std::path::PathBuf;

use async_trait::async_trait;
use dialog_capability::access::{
    AuthorizeError, Certificate, CertificateStore, Delegation, Protocol, Prove, Retain,
};
use dialog_capability::{Policy, Provider};
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
impl<P: Protocol> CertificateStore<P> for FileStore
where
    P::Certificate: ConditionalSend,
{
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Certificate>, AuthorizeError> {
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

            match <P::Certificate as Certificate>::decode(&bytes) {
                Ok(proof) => proofs.push(proof),
                Err(_) => continue,
            }
        }

        Ok(proofs)
    }

    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        let base = permit_dir(self)?;

        for proof in delegation.certificates() {
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
impl<P> Provider<Prove<P>> for FileStore
where
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Certificate: Clone + ConditionalSend + ConditionalSync,
    P::Proof: ConditionalSend,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: dialog_capability::Capability<Prove<P>>,
    ) -> Result<P::Proof, AuthorizeError> {
        let auth = Prove::<P>::of(&input);
        let mut authorize = Prove::new(auth.principal.clone(), auth.access.clone());
        authorize.duration = auth.duration;
        CertificateStore::<P>::prove(self, authorize).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P> Provider<Retain<P>> for FileStore
where
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: dialog_capability::Capability<Retain<P>>,
    ) -> Result<(), AuthorizeError> {
        let delegation = &Retain::<P>::of(&input).delegation;
        CertificateStore::<P>::save(self, delegation).await
    }
}
