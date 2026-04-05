//! Credential capability providers for IndexedDb.

use super::{IndexedDb, IndexedDbError};
use async_trait::async_trait;
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::credential::{Credential, CredentialExport};
use dialog_effects::credential::{self, CredentialError};
use wasm_bindgen::JsValue;

const DATA_STORE: &str = "data";

struct Err(CredentialError);

impl From<IndexedDbError> for Err {
    fn from(e: IndexedDbError) -> Self {
        Self(CredentialError::Storage(e.to_string()))
    }
}

impl From<CredentialError> for Err {
    fn from(e: CredentialError) -> Self {
        Self(e)
    }
}

#[async_trait(?Send)]
impl Provider<credential::Load> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Credential, CredentialError> {
        let address = credential::Address::of(&input).address.clone();
        let subject = input.subject().to_string();

        self.open(&subject)
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let mut session = self
            .take_session(&subject)
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        let result: Result<_, Err> = async {
            let store = session.store(DATA_STORE).await?;
            let js_key = JsValue::from_str(&address);

            let value = store
                .query(|object_store| async move {
                    object_store
                        .get(js_key)
                        .await
                        .map_err(|e| Err(CredentialError::Storage(e.to_string())))
                })
                .await?;

            match value {
                Some(js_val) => {
                    let export = CredentialExport::from(js_val);
                    let credential = Credential::import(export)
                        .await
                        .map_err(|e| CredentialError::Corrupted(e.to_string()))?;
                    Ok(credential)
                }
                None => Result::Err(CredentialError::NotFound(address).into()),
            }
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}

#[async_trait(?Send)]
impl Provider<credential::Save> for IndexedDb {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let address = credential::Address::of(&input).address.clone();
        let credential = &credential::Save::of(&input).credential;
        let subject = input.subject().to_string();

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let js_val: JsValue = export.into();

        self.open(&subject)
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let mut session = self
            .take_session(&subject)
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        let result: Result<_, Err> = async {
            let store = session.store(DATA_STORE).await?;
            let js_key = JsValue::from_str(&address);

            store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_val, Some(&js_key))
                        .await
                        .map_err(|e| Err(CredentialError::Storage(e.to_string())))?;
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
