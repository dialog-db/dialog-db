use super::Storage;
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::repository::{
    Credential, CredentialExport, Load, LoadCapability, RepositoryError, Save, SaveCapability,
};
use dialog_storage::provider::IndexedDb;
use dialog_storage::provider::indexeddb::IndexedDbError;
use wasm_bindgen::JsValue;

const CREDENTIALS_STORE: &str = "credentials";
const SELF_KEY: &str = "self";

fn to_err(e: impl std::fmt::Display) -> RepositoryError {
    RepositoryError::Storage(e.to_string())
}

struct Err(RepositoryError);

impl From<IndexedDbError> for Err {
    fn from(e: IndexedDbError) -> Self {
        Self(RepositoryError::Storage(e.to_string()))
    }
}

impl From<RepositoryError> for Err {
    fn from(e: RepositoryError) -> Self {
        Self(e)
    }
}

#[async_trait(?Send)]
impl Provider<Load> for Storage<'_, IndexedDb> {
    async fn execute(
        &self,
        input: Capability<Load>,
    ) -> Result<Option<Credential>, RepositoryError> {
        let name = input.name();

        self.open(name).await.map_err(to_err)?;
        let mut session = self.take_session(name).map_err(to_err)?;

        let result: Result<_, Err> = async {
            let store = session.store(CREDENTIALS_STORE).await?;
            let js_key = JsValue::from_str(SELF_KEY);

            let existing = store
                .query(|object_store| async move {
                    object_store
                        .get(js_key)
                        .await
                        .map_err(|e| Err(RepositoryError::Storage(e.to_string())))
                })
                .await?;

            match existing {
                Some(js_val) => {
                    let credential = Credential::import(CredentialExport::from(js_val))
                        .await
                        .map_err(|e| RepositoryError::Corrupted(e.to_string()))?;
                    Ok(Some(credential))
                }
                None => Ok(None),
            }
        }
        .await;

        self.return_session(name, session);
        result.map_err(|e| e.0)
    }
}

#[async_trait(?Send)]
impl Provider<Save> for Storage<'_, IndexedDb> {
    async fn execute(&self, input: Capability<Save>) -> Result<(), RepositoryError> {
        let name = input.name();
        let credential = input.credential();

        self.open(name).await.map_err(to_err)?;
        let mut session = self.take_session(name).map_err(to_err)?;

        let result: Result<_, Err> = async {
            let store = session.store(CREDENTIALS_STORE).await?;
            let js_key = JsValue::from_str(SELF_KEY);
            let export = credential
                .export()
                .await
                .map_err(|e| RepositoryError::Storage(e.to_string()))?;
            let js_val: JsValue = export.into();

            store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_val, Some(&js_key))
                        .await
                        .map_err(|e| Err(RepositoryError::Storage(e.to_string())))?;
                    Ok::<(), Err>(())
                })
                .await?;
            Ok(())
        }
        .await;

        self.return_session(name, session);
        result.map_err(|e| e.0)
    }
}
