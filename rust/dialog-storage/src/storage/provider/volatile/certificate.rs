//! CertificateStore for volatile (in-memory) storage.
//!
//! Certificates are stored in-memory keyed by
//! `{audience}/{subject}/{issuer}.{hash}` (or `{audience}/_/{issuer}.{hash}`
//! for [powerlines](https://github.com/ucan-wg/delegation?tab=readme-ov-file#powerline)).

use dialog_capability::access::{
    AuthorizeError, Certificate, CertificateStore, Delegation, Protocol,
};
use dialog_varsig::Did;

use super::Volatile;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<P: Protocol> CertificateStore<P> for Volatile
where
    P::Certificate: dialog_common::ConditionalSend,
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
        let prefix = match subject {
            Some(did) => format!("{}/{}/", audience, did),
            None => format!("{}/_/", audience),
        };

        let sessions = self.sessions.read();
        let mut certificates = Vec::new();

        for session in sessions.values() {
            for (key, bytes) in &session.certificates {
                if key.starts_with(&prefix)
                    && let Ok(cert) = <P::Certificate as Certificate>::decode(bytes)
                {
                    certificates.push(cert);
                }
            }
        }

        Ok(certificates)
    }

    /// Store a delegation's certificates for future lookups.
    ///
    /// Each certificate is stored keyed by
    /// `{audience}/{subject}/{issuer}.{hash}` where the hash is the
    /// base58-encoded BLAKE3 hash of the encoded certificate bytes.
    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        let mut sessions = self.sessions.write();

        for cert in delegation.certificates() {
            let bytes = cert.encode()?;
            let id = base58::ToBase58::to_base58(blake3::hash(&bytes).as_bytes().as_slice());

            let audience = cert.audience().to_string();
            let subject_segment = match cert.subject() {
                Some(did) => did.to_string(),
                None => "_".to_string(),
            };
            let issuer = cert.issuer().to_string();

            let key = format!("{audience}/{subject_segment}/{issuer}.{id}");
            let session = sessions.entry(cert.audience().clone()).or_default();
            session.certificates.insert(key, bytes);
        }

        Ok(())
    }
}
