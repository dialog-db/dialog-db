//! Access provider for volatile (in-memory) storage.
//!
//! Implements [`ProofStore`](dialog_capability::access::ProofStore) for [`Volatile`]
//! and `Provider<Retain<P>>` for granting access.
//!
//! Proofs are stored in-memory keyed by `{audience}/{subject}/{issuer}.{hash}`
//! (or `{audience}/_/{issuer}.{hash}` for powerlines).

use async_trait::async_trait;
use dialog_capability::access::{
    AuthorizeError, Certificate, CertificateStore, Delegation, Protocol, Prove, Retain,
};
use dialog_capability::{Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_varsig::Did;

use super::Volatile;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P: Protocol> CertificateStore<P> for Volatile
where
    P::Certificate: ConditionalSend,
{
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
        let mut proofs = Vec::new();

        for session in sessions.values() {
            for (key, bytes) in &session.proofs {
                if key.starts_with(&prefix)
                    && let Ok(proof) = <P::Certificate as Certificate>::decode(bytes)
                {
                    proofs.push(proof);
                }
            }
        }

        Ok(proofs)
    }

    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        let mut sessions = self.sessions.write();

        for proof in delegation.certificates() {
            let bytes = proof.encode()?;
            let id = base58::ToBase58::to_base58(blake3::hash(&bytes).as_bytes().as_slice());

            let audience = proof.audience().to_string();
            let subject_segment = match proof.subject() {
                Some(did) => did.to_string(),
                None => "_".to_string(),
            };
            let issuer = proof.issuer().to_string();

            let key = format!("{audience}/{subject_segment}/{issuer}.{id}");
            let session = sessions.entry(proof.audience().clone()).or_default();
            session.proofs.insert(key, bytes);
        }

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P> Provider<Prove<P>> for Volatile
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
impl<P> Provider<Retain<P>> for Volatile
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
