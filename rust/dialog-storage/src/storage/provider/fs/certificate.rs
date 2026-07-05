//! CertificateStore for filesystem storage.
//!
//! Layout: `{space_root}/certificate/{audience}/{subject}/{issuer}.{hash}`

use base58::ToBase58;
use dialog_capability::access::{
    AuthorizeError, Certificate, CertificateStore, Delegation, Protocol, Prove, Retain,
};
use dialog_capability::{Capability, Policy, Provider};
use dialog_varsig::Did;

use super::{FileSystem, FileSystemError, FileSystemHandle};

const CERTIFICATE: &str = "certificate";

impl FileSystem {
    /// Returns the handle for this space's certificate directory.
    pub fn certificate(&self) -> Result<FileSystemHandle, FileSystemError> {
        self.resolve(CERTIFICATE)
    }
}

#[async_trait::async_trait]
impl<P: Protocol> CertificateStore<P> for FileSystem
where
    P::Certificate: Send + Sync,
{
    /// List certificates where `audience` is the recipient and `subject`
    /// is either the specific subject DID or, when `None`, a
    /// [powerline](https://github.com/ucan-wg/delegation?tab=readme-ov-file#powerline)
    /// delegation that applies to any subject.
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Certificate>, AuthorizeError> {
        let subject_segment = match subject {
            Some(did) => did.to_string(),
            None => "_".to_string(),
        };

        // Resolve certificate/{audience}/{subject} directory
        let dir = match self
            .certificate()
            .and_then(|c| c.resolve(audience.as_ref()))
            .and_then(|c| c.resolve(&subject_segment))
        {
            Ok(dir) => dir,
            Err(_) => return Ok(Vec::new()),
        };

        let entries = match dir.list().await {
            Ok(entries) => entries,
            Err(_) => return Ok(Vec::new()),
        };

        let mut certificates = Vec::new();
        for entry in entries {
            let handle = dir.resolve(&entry)?;
            if let Ok(bytes) = handle.read().await
                && let Ok(cert) = <P::Certificate as Certificate>::decode(&bytes)
            {
                certificates.push(cert);
            }
        }

        Ok(certificates)
    }

    /// Store a delegation's certificates as files for future lookups.
    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        for cert in delegation.certificates() {
            let bytes = cert.encode()?;
            let id = blake3::hash(&bytes).as_bytes().to_base58();

            let audience = cert.audience();
            let subject_segment = match cert.subject() {
                Some(did) => did.to_string(),
                None => "_".to_string(),
            };
            let issuer = cert.issuer();

            // Write to certificate/{audience}/{subject}/{issuer}.{hash}
            // write() creates parent directories automatically
            let filename = format!("{issuer}.{id}");
            let file_handle = self
                .certificate()?
                .resolve(audience.as_ref())?
                .resolve(&subject_segment)?
                .resolve(&filename)?;
            file_handle.write(&bytes).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<P> Provider<Prove<P>> for FileSystem
where
    P: Protocol,
    P::Access: Clone + Send + Sync,
    P::Certificate: Clone + Send + Sync,
    P::Proof: Send,
{
    async fn execute(&self, input: Capability<Prove<P>>) -> Result<P::Proof, AuthorizeError> {
        let auth = Prove::<P>::of(&input);
        let mut prove = Prove::new(auth.principal.clone(), auth.access.clone());
        prove.duration = auth.duration;
        CertificateStore::<P>::prove(self, prove).await
    }
}

#[async_trait::async_trait]
impl<P> Provider<Retain<P>> for FileSystem
where
    P: Protocol,
    P::Delegation: Send + Sync,
{
    async fn execute(&self, input: Capability<Retain<P>>) -> Result<(), AuthorizeError> {
        let delegation = &Retain::<P>::of(&input).delegation;
        CertificateStore::<P>::save(self, delegation).await
    }
}
