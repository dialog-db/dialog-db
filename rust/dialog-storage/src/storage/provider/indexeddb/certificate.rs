//! CertificateStore for IndexedDB storage.
//!
//! Certificates are stored in a `certificate` object store with keys
//! `{audience}/{subject}/{issuer}.{hash}` (or `{audience}/_/{issuer}.{hash}`
//! for powerlines). Uses IDBKeyRange for efficient prefix queries.

use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::access::{
    AuthorizeError, Certificate, CertificateStore, Delegation, Protocol, Prove, Retain,
};
use dialog_capability::{Capability, Policy, Provider};
use dialog_varsig::Did;
use rexie::KeyRange;
use wasm_bindgen::JsValue;

use super::{IndexedDb, to_uint8array};

const CERTIFICATE: &str = "certificate";

#[async_trait(?Send)]
impl<P: Protocol> CertificateStore<P> for IndexedDb {
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Certificate>, AuthorizeError> {
        let prefix = match subject {
            Some(did) => format!("{}/{}/", audience, did),
            None => format!("{}/_/", audience),
        };

        let has_store = self.connection.borrow().stores.contains(CERTIFICATE);
        if !has_store {
            return Ok(Vec::new());
        }

        let store = self.store(CERTIFICATE).await?;
        let lower = JsValue::from_str(&prefix);
        let upper = JsValue::from_str(&format!("{prefix}\u{ffff}"));
        let range = KeyRange::bound(&lower, &upper, None, None)
            .map_err(|e| AuthorizeError::Configuration(format!("key range error: {e:?}")))?;

        store
            .query(|object_store| async move {
                let values = object_store
                    .get_all(Some(range), None)
                    .await
                    .map_err(|e| AuthorizeError::Configuration(format!("query: {e:?}")))?;

                let mut certs = Vec::new();
                for value in values {
                    let array = js_sys::Uint8Array::new(&value);
                    let bytes = array.to_vec();
                    if let Ok(cert) = <P::Certificate as Certificate>::decode(&bytes) {
                        certs.push(cert);
                    }
                }
                Ok(certs)
            })
            .await
    }

    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        let certs = delegation.certificates();
        if certs.is_empty() {
            return Ok(());
        }

        let store = self.store(CERTIFICATE).await?;

        for cert in &certs {
            let bytes = cert.encode()?;
            let id = blake3::hash(&bytes).as_bytes().to_base58();

            let audience = cert.audience().to_string();
            let subject_segment = match cert.subject() {
                Some(did) => did.to_string(),
                None => "_".to_string(),
            };
            let issuer = cert.issuer().to_string();
            let key = format!("{audience}/{subject_segment}/{issuer}.{id}");

            let js_key = JsValue::from_str(&key);
            let js_val = JsValue::from(to_uint8array(&bytes));

            store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_val, Some(&js_key))
                        .await
                        .map_err(|e| AuthorizeError::Configuration(format!("write: {e:?}")))?;
                    Ok::<(), AuthorizeError>(())
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl<P> Provider<Prove<P>> for IndexedDb
where
    P: Protocol,
    P::Access: Clone,
    P::Certificate: Clone,
{
    async fn execute(&self, input: Capability<Prove<P>>) -> Result<P::Proof, AuthorizeError> {
        let auth = Prove::<P>::of(&input);
        let mut prove = Prove::new(auth.principal.clone(), auth.access.clone());
        prove.duration = auth.duration;
        CertificateStore::<P>::prove(self, prove).await
    }
}

#[async_trait(?Send)]
impl<P: Protocol> Provider<Retain<P>> for IndexedDb {
    async fn execute(&self, input: Capability<Retain<P>>) -> Result<(), AuthorizeError> {
        let delegation = &Retain::<P>::of(&input).delegation;
        CertificateStore::<P>::save(self, delegation).await
    }
}
