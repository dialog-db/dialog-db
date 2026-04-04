//! Authorization provider for volatile (in-memory) storage.
//!
//! Implements [`ProofStore`](dialog_capability::access::ProofStore) for [`Volatile`]
//! and `Provider<Save<P>>` for storing permits.
//!
//! Permits are stored in-memory keyed by `{audience}/{subject}/{issuer}.{hash}`
//! (or `{audience}/_/{issuer}.{hash}` for powerlines).

use async_trait::async_trait;
use dialog_capability::access::{
    Claim, AuthorizeError, Delegation, ProofChain, ProofStore, Protocol, Save, Scope,
};
use dialog_capability::{Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_varsig::Did;

use super::Volatile;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P: Protocol> ProofStore<P> for Volatile
where
    P::Proof: ConditionalSend,
{
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Proof>, AuthorizeError> {
        let prefix = match subject {
            Some(did) => format!("{}/{}/", audience, did),
            None => format!("{}/_/", audience),
        };

        let sessions = self.sessions.read();
        let mut proofs = Vec::new();

        for session in sessions.values() {
            for (key, bytes) in &session.proofs {
                if key.starts_with(&prefix) {
                    if let Ok(proof) = <P::Proof as Delegation>::decode(bytes) {
                        proofs.push(proof);
                    }
                }
            }
        }

        Ok(proofs)
    }

    async fn save(&self, permit: &P::ProofChain) -> Result<(), AuthorizeError> {
        let subject_did = permit.access().subject().clone();
        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject_did).or_default();

        for proof in permit.proofs() {
            let bytes = proof.encode()?;
            let id = base58::ToBase58::to_base58(blake3::hash(&bytes).as_bytes().as_slice());

            let audience = proof.audience().to_string();
            let subject_segment = match proof.subject() {
                Some(did) => did.to_string(),
                None => "_".to_string(),
            };
            let issuer = proof.issuer().to_string();

            let key = format!("{audience}/{subject_segment}/{issuer}.{id}");
            session.proofs.insert(key, bytes);
        }

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P> Provider<Claim<P>> for Volatile
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
impl<P> Provider<Save<P>> for Volatile
where
    P: Protocol,
    P::Proof: ConditionalSend + ConditionalSync,
    P::ProofChain: ConditionalSend + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: dialog_capability::Capability<Save<P>>,
    ) -> Result<(), AuthorizeError> {
        let proof_chain = &Save::<P>::of(&input).proof_chain;
        ProofStore::<P>::save(self, proof_chain).await
    }
}
