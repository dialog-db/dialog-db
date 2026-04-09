//! CertificateStore for IndexedDB storage.
//!
//! Certificates are stored in a `certificate` object store with keys
//! `{audience}/{subject}/{issuer}.{hash}` (or `{audience}/_/{issuer}.{hash}`
//! for powerlines). Uses IDBKeyRange for efficient prefix queries.

use async_trait::async_trait;
use dialog_capability::access::{
    AuthorizeError, Certificate, CertificateStore, Delegation, Protocol,
};
use dialog_varsig::Did;
use rexie::KeyRange;
use wasm_bindgen::JsValue;

use super::{IndexedDb, to_uint8array};

const CERTIFICATE_STORE: &str = "certificate";

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

        let did: Did = audience.clone();
        self.open(&did).await?;

        let mut session = self.take_session(&did)?;

        if !session.stores.contains(CERTIFICATE_STORE) {
            self.return_session(did, session);
            return Ok(Vec::new());
        }

        let store = session.store(CERTIFICATE_STORE).await?;
        let lower = JsValue::from_str(&prefix);
        let upper = JsValue::from_str(&format!("{prefix}\u{ffff}"));
        let range = KeyRange::bound(&lower, &upper, None, None)
            .map_err(|e| AuthorizeError::Configuration(format!("key range error: {e:?}")))?;

        let result = store
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
            .await;

        self.return_session(did, session);
        result
    }

    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError> {
        let certs = delegation.certificates();
        if certs.is_empty() {
            return Ok(());
        }

        let did = certs[0].audience().clone();
        self.open(&did).await?;
        let mut session = self.take_session(&did)?;
        let store = session.store(CERTIFICATE_STORE).await?;

        for cert in &certs {
            let bytes = cert.encode()?;
            let id = base58::ToBase58::to_base58(blake3::hash(&bytes).as_bytes().as_slice());

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

        self.return_session(did, session);
        Ok(())
    }
}
