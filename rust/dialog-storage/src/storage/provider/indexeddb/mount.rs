//! Mount and Location providers for IndexedDb.

use super::{IndexedDb, IndexedDbError};
use async_trait::async_trait;
use dialog_capability::storage::{self, StorageError};
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
impl Provider<storage::Mount<IndexedDb>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<storage::Mount<IndexedDb>>,
    ) -> Result<IndexedDb, StorageError> {
        let path = &storage::Location::of(&input).path();
        Ok(IndexedDb {
            mount: self.prefixed(path),
            sessions: self.sessions.clone(),
        })
    }
}

#[async_trait(?Send)]
impl Provider<storage::Load<Credential>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<storage::Load<Credential>>,
    ) -> Result<Credential, StorageError> {
        let path = storage::Location::of(&input).path().to_owned();
        let subject = input.subject().to_string();
        let db_name = self.prefixed(&subject);

        self.open(&subject)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        let mut session = self
            .take_session(&subject)
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let result: Result<_, Err> = async {
            let store = session.store(DATA_STORE).await?;
            let js_key = JsValue::from_str(&path);

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
                None => Err(StorageError::Storage(format!("not found: {}", path)).into()),
            }
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}

#[async_trait(?Send)]
impl Provider<storage::Save<Credential>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<storage::Save<Credential>>,
    ) -> Result<(), StorageError> {
        let path = storage::Location::of(&input).path().to_owned();
        let credential = &storage::Save::<Credential>::of(&input).content;
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
            let js_key = JsValue::from_str(&path);

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
