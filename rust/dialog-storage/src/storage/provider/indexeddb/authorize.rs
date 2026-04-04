//! Authorization provider for IndexedDB storage.
//!
//! Implements [`ProofStore`](dialog_capability::access::ProofStore) for [`IndexedDb`]
//! and `Provider<Save<P>>` for storing permits.
//!
//! Permits are stored in a `permit` object store with keys
//! `{audience}/{subject}/{issuer}.{hash}` (or `{audience}/_/{issuer}.{hash}`
//! for powerlines). Uses IDBKeyRange for efficient prefix queries.

use async_trait::async_trait;
use dialog_capability::access::{
    Claim, AuthorizeError, Delegation, ProofChain, ProofStore, Protocol, Save, Scope,
};
use dialog_capability::{Policy, Provider};
use dialog_varsig::Did;
use rexie::KeyRange;
use wasm_bindgen::JsValue;

use super::{IndexedDb, IndexedDbError, to_uint8array};

const PERMIT_STORE: &str = "permit";

struct Err(AuthorizeError);

impl From<IndexedDbError> for Err {
    fn from(e: IndexedDbError) -> Self {
        Self(AuthorizeError::Configuration(e.to_string()))
    }
}

impl From<AuthorizeError> for Err {
    fn from(e: AuthorizeError) -> Self {
        Self(e)
    }
}

fn prefix_range(prefix: &str) -> Result<KeyRange, AuthorizeError> {
    let lower = JsValue::from_str(prefix);
    let upper = JsValue::from_str(&format!("{prefix}\u{ffff}"));
    KeyRange::bound(&lower, &upper, None, None)
        .map_err(|e| AuthorizeError::Configuration(format!("key range error: {e:?}")))
}

#[async_trait(?Send)]
impl<P: Protocol> ProofStore<P> for IndexedDb {
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Proof>, AuthorizeError> {
        let prefix = match subject {
            Some(did) => format!("{}/{}/", audience, did),
            None => format!("{}/_/", audience),
        };

        let subject_str = audience.to_string();
        self.open(&subject_str).await.map_err(|e| {
            AuthorizeError::Configuration(format!("Failed to open store: {e}"))
        })?;

        let mut session = self.take_session(&subject_str).map_err(|e| {
            AuthorizeError::Configuration(format!("Failed to take session: {e}"))
        })?;

        if !session.stores.contains(PERMIT_STORE) {
            self.return_session(&subject_str, session);
            return Ok(Vec::new());
        }

        let idb_store = session
            .store(PERMIT_STORE)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let range = prefix_range(&prefix)?;

        let result: Result<Vec<P::Proof>, Err> = idb_store
            .query(|object_store| async move {
                let values = object_store
                    .get_all(Some(range), None)
                    .await
                    .map_err(|e| {
                        Err(AuthorizeError::Configuration(format!("query: {e:?}")))
                    })?;

                let mut proofs = Vec::new();
                for value in values {
                    let array = js_sys::Uint8Array::new(&value);
                    let bytes = array.to_vec();
                    if let Ok(proof) = <P::Proof as Delegation>::decode(&bytes) {
                        proofs.push(proof);
                    }
                }
                Ok::<_, Err>(proofs)
            })
            .await;

        self.return_session(&subject_str, session);
        result.map_err(|e| e.0)
    }

    async fn save(&self, permit: &P::ProofChain) -> Result<(), AuthorizeError> {
        let subject_str = permit.access().subject().to_string();

        self.open(&subject_str).await.map_err(|e| {
            AuthorizeError::Configuration(format!("Failed to open store: {e}"))
        })?;

        let mut session = self.take_session(&subject_str).map_err(|e| {
            AuthorizeError::Configuration(format!("Failed to take session: {e}"))
        })?;

        let idb_store = session
            .store(PERMIT_STORE)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

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

            let js_key = JsValue::from_str(&key);
            let js_val = JsValue::from(to_uint8array(&bytes));

            idb_store
                .transact(|object_store| async move {
                    object_store.put(&js_val, Some(&js_key)).await.map_err(|e| {
                        Err(AuthorizeError::Configuration(format!("write: {e:?}")))
                    })?;
                    Ok::<(), Err>(())
                })
                .await
                .map_err(|e| e.0)?;
        }

        self.return_session(&subject_str, session);
        Ok(())
    }
}

#[async_trait(?Send)]
impl<P> Provider<Claim<P>> for IndexedDb
where
    P: Protocol,
    P::Access: Clone,
    P::Proof: Clone,
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

#[async_trait(?Send)]
impl<P: Protocol> Provider<Save<P>> for IndexedDb {
    async fn execute(
        &self,
        input: dialog_capability::Capability<Save<P>>,
    ) -> Result<(), AuthorizeError> {
        let proof_chain = &Save::<P>::of(&input).proof_chain;
        ProofStore::<P>::save(self, proof_chain).await
    }
}
