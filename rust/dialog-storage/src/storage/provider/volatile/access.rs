//! Access provider for volatile (in-memory) storage.
//!
//! Implements [`ProofStore`](dialog_capability::access::ProofStore) for [`Volatile`]
//! and `Provider<Save<P>>` for granting access.
//!
//! Proofs are stored in-memory keyed by `{audience}/{subject}/{issuer}.{hash}`
//! (or `{audience}/_/{issuer}.{hash}` for powerlines).

use async_trait::async_trait;
use dialog_capability::access::{AuthorizeError, Claim, Delegation, ProofStore, Protocol, Save};
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
                if key.starts_with(&prefix)
                    && let Ok(proof) = <P::Proof as Delegation>::decode(bytes)
                {
                    proofs.push(proof);
                }
            }
        }

        Ok(proofs)
    }

    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        let mut sessions = self.sessions.write();

        for proof in P::proofs(delegation) {
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
        let mut authorize = Claim::new(auth.by.clone(), auth.access.clone());
        authorize.duration = auth.duration;
        ProofStore::<P>::authorize(self, authorize).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P> Provider<Save<P>> for Volatile
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
