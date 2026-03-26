//! Credential capability provider for IndexedDB.
//!
//! Stores credentials as exported JsValues in a `credentials` object store,
//! keyed by credential name.

use super::{IndexedDb, IndexedDbError};
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::credential::{
    self, CredentialError, CredentialExport, Identity, LoadCapability, SaveCapability,
};
use wasm_bindgen::JsValue;

/// The single object store used for all credential operations.
const CREDENTIALS_STORE: &str = "credentials";

impl From<IndexedDbError> for CredentialError {
    fn from(e: IndexedDbError) -> Self {
        CredentialError::Storage(e.to_string())
    }
}

#[async_trait(?Send)]
impl Provider<credential::Load> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Option<Identity>, CredentialError> {
        let subject: dialog_capability::Did = input.subject().into();
        let name = input.name().to_string();

        self.open(subject.as_ref()).await?;
        let mut session = self.take_session(subject.as_ref())?;

        let result = async {
            let store = session.store(CREDENTIALS_STORE).await?;
            let key = JsValue::from_str(&name);

            store
                .query(|object_store| async move {
                    let value = object_store
                        .get(key)
                        .await
                        .map_err(|e| CredentialError::Storage(e.to_string()))?;

                    let Some(js_val) = value else {
                        return Ok(None);
                    };

                    let export = CredentialExport::from(js_val);
                    let credential = Identity::import(export)
                        .await
                        .map_err(|e| CredentialError::Corrupted(e.to_string()))?;

                    Ok(Some(credential))
                })
                .await
        }
        .await;

        self.return_session(subject.as_ref(), session);
        result
    }
}

#[async_trait(?Send)]
impl Provider<credential::Save> for IndexedDb {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let subject: dialog_capability::Did = input.subject().into();
        let name = input.name().to_string();
        let credential = input.credential();

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let js_value: JsValue = export.into();

        self.open(subject.as_ref()).await?;
        let mut session = self.take_session(subject.as_ref())?;

        let result = async {
            let store = session.store(CREDENTIALS_STORE).await?;
            let key = JsValue::from_str(&name);

            store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_value, Some(&key))
                        .await
                        .map_err(|e| CredentialError::Storage(e.to_string()))?;
                    Ok(())
                })
                .await
        }
        .await;

        self.return_session(subject.as_ref(), session);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::credential;

    fn unique_subject(prefix: &str) -> Subject {
        let did: Did = format!("did:test:{}-{}", prefix, js_sys::Date::now() as u64)
            .parse()
            .unwrap();
        Subject::from(did)
    }

    fn build_load_cap(subject: Subject, name: &str) -> Capability<credential::Load> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(name))
            .invoke(credential::Load)
    }

    fn _build_save_cap(
        subject: Subject,
        name: &str,
        credential: Identity,
    ) -> Capability<credential::Save> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(name))
            .invoke(credential::Save::new(credential))
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_credentials() -> anyhow::Result<()> {
        let provider = IndexedDb::new();
        let subject = unique_subject("idb-cred-missing");

        let result = <IndexedDb as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "s3-bucket"),
        )
        .await?;

        assert!(result.is_none());

        Ok(())
    }
}
