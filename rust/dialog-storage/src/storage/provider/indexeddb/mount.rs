//! Mount and Location providers for IndexedDb.

use super::{Address, IndexedDb, IndexedDbError};
use async_trait::async_trait;
use dialog_capability::storage::{self, Location, StorageError};
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::credential::{Credential, CredentialExport};
use wasm_bindgen::JsValue;

const DATA_STORE: &str = "data";

struct Err(StorageError);

impl From<IndexedDbError> for Err {
    fn from(e: IndexedDbError) -> Self {
        Self(StorageError::Storage(e.to_string()))
    }
}

impl From<StorageError> for Err {
    fn from(e: StorageError) -> Self {
        Self(e)
    }
}

#[async_trait(?Send)]
impl Provider<storage::Mount<IndexedDb, Address>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<storage::Mount<IndexedDb, Address>>,
    ) -> Result<IndexedDb, StorageError> {
        let prefix = Location::of(&input).address().prefix();
        Ok(IndexedDb {
            mount: self.prefixed(prefix),
            sessions: self.sessions.clone(),
        })
    }
}

#[async_trait(?Send)]
impl Provider<storage::Load<Credential, Address>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<storage::Load<Credential, Address>>,
    ) -> Result<Credential, StorageError> {
        let prefix = Location::of(&input).address().prefix().to_owned();
        let subject = input.subject().to_string();

        self.open(&subject)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        let mut session = self
            .take_session(&subject)
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let result: Result<_, Err> = async {
            let store = session.store(DATA_STORE).await?;
            let js_key = JsValue::from_str(&prefix);

            let value = store
                .query(|object_store| async move {
                    object_store
                        .get(js_key)
                        .await
                        .map_err(|e| Err(StorageError::Storage(e.to_string())))
                })
                .await?;

            match value {
                Some(js_val) => {
                    let export = CredentialExport::from(js_val);
                    let credential = Credential::import(export)
                        .await
                        .map_err(|e| StorageError::Storage(e.to_string()))?;
                    Ok(credential)
                }
                None => Err(StorageError::Storage(format!("not found: {}", prefix)).into()),
            }
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}

#[async_trait(?Send)]
impl Provider<storage::Save<Credential, Address>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<storage::Save<Credential, Address>>,
    ) -> Result<(), StorageError> {
        let prefix = Location::of(&input).address().prefix().to_owned();
        let credential = &storage::Save::<Credential, Address>::of(&input).content;
        let subject = input.subject().to_string();

        let export = credential
            .export()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        let js_val: JsValue = export.into();

        self.open(&subject)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        let mut session = self
            .take_session(&subject)
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let result: Result<_, Err> = async {
            let store = session.store(DATA_STORE).await?;
            let js_key = JsValue::from_str(&prefix);

            store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_val, Some(&js_key))
                        .await
                        .map_err(|e| Err(StorageError::Storage(e.to_string())))?;
                    Ok::<(), Err>(())
                })
                .await?;
            Ok(())
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}
