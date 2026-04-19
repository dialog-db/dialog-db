//! Credential capability providers for IndexedDb.

use super::{IndexedDb, IndexedDbError, to_uint8array};
use async_trait::async_trait;
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::Credential;
use dialog_credentials::credential::CredentialExport;
use dialog_effects::credential::{CredentialError, Key, Load, Save, Secret, Site};
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};

const CREDENTIAL: &str = "credential";

#[async_trait(?Send)]
impl Provider<Load<Credential>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        let address = Key::of(&input).address.clone();
        let idb_key = format!("key/{address}");

        let store = self.store(CREDENTIAL).await?;
        let key = JsValue::from_str(&idb_key);

        let value = store
            .query(|object_store| async move {
                object_store
                    .get(key)
                    .await
                    .map_err(|e| CredentialError::Storage(e.to_string()))
            })
            .await?;

        match value {
            Some(js_val) => {
                let export = CredentialExport::from(js_val);
                Credential::import(export)
                    .await
                    .map_err(|e| CredentialError::Corrupted(e.to_string()))
            }
            None => Err(CredentialError::NotFound(idb_key)),
        }
    }
}

#[async_trait(?Send)]
impl Provider<Save<Credential>> for IndexedDb {
    async fn execute(&self, input: Capability<Save<Credential>>) -> Result<(), CredentialError> {
        let address = Key::of(&input).address.clone();
        let idb_key = format!("key/{address}");
        let credential = &Save::<Credential>::of(&input).credential;

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let js_val: JsValue = export.into();

        let store = self.store(CREDENTIAL).await?;
        let key = JsValue::from_str(&idb_key);

        store
            .transact(|object_store| async move {
                object_store
                    .put(&js_val, Some(&key))
                    .await
                    .map_err(|e| CredentialError::Storage(e.to_string()))?;
                Ok(())
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<Load<Secret>> for IndexedDb {
    async fn execute(&self, input: Capability<Load<Secret>>) -> Result<Secret, CredentialError> {
        let address = Site::of(&input).address.clone();
        let idb_key = format!("site/{address}");

        let store = self.store(CREDENTIAL).await?;
        let key = JsValue::from_str(&idb_key);

        let value = store
            .query(|object_store| async move {
                object_store
                    .get(key)
                    .await
                    .map_err(|e| CredentialError::Storage(e.to_string()))
            })
            .await?;

        match value {
            Some(js_val) => {
                let bytes = js_val
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| CredentialError::Corrupted("Value is not Uint8Array".into()))?
                    .to_vec();
                Ok(Secret::from(bytes))
            }
            None => Err(CredentialError::NotFound(idb_key)),
        }
    }
}

#[async_trait(?Send)]
impl Provider<Save<Secret>> for IndexedDb {
    async fn execute(&self, input: Capability<Save<Secret>>) -> Result<(), CredentialError> {
        let address = Site::of(&input).address.clone();
        let idb_key = format!("site/{address}");
        let secret = &Save::<Secret>::of(&input).credential;

        let js_val: JsValue = to_uint8array(secret.as_bytes()).into();

        let store = self.store(CREDENTIAL).await?;
        let key = JsValue::from_str(&idb_key);

        store
            .transact(|object_store| async move {
                object_store
                    .put(&js_val, Some(&key))
                    .await
                    .map_err(|e| CredentialError::Storage(e.to_string()))?;
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{test_credential, unique_did, unique_name};
    use dialog_effects::prelude::*;
    use dialog_varsig::Principal;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    async fn it_returns_not_found_for_missing_credential() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("cred-missing")).await?;
        let did = unique_did().await;

        let result = did.credential().key("self").load().perform(&provider).await;

        assert!(result.is_err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_saves_and_loads_credential() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("cred-save-load")).await?;
        let did = unique_did().await;
        let cred = test_credential().await;
        let expected_did = cred.did();

        did.clone()
            .credential()
            .key("self")
            .save(cred)
            .perform(&provider)
            .await?;

        let loaded = did
            .credential()
            .key("self")
            .load()
            .perform(&provider)
            .await?;

        assert_eq!(loaded.did(), expected_did);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_garbage_credential() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("cred-garbage")).await?;
        let did = unique_did().await;

        // Write garbage directly via store
        let store = provider.store(CREDENTIAL).await?;
        store
            .transact(|object_store| async move {
                let key = JsValue::from_str("key/self");
                let garbage = JsValue::from_str("not a credential");
                object_store
                    .put(&garbage, Some(&key))
                    .await
                    .map_err(|e| IndexedDbError::Store(e.to_string()))?;
                Ok::<(), IndexedDbError>(())
            })
            .await?;
        drop(store);

        let result = did.credential().key("self").load().perform(&provider).await;

        assert!(result.is_err(), "should reject garbage credential data");
        Ok(())
    }
}
