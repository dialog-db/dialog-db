use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::credential::CredentialError;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{SignerCredential, SignerCredentialExport};
use dialog_storage::provider::IndexedDb;
use wasm_bindgen::JsValue;

use crate::credentials::open::{Open, ProfileSigner};

const CREDENTIALS_STORE: &str = "credentials";

#[async_trait(?Send)]
impl Provider<Open> for IndexedDb {
    async fn execute(&self, input: Open) -> Result<ProfileSigner, CredentialError> {
        let store_key = "self";

        self.open(&input.name)
            .await
            .map_err(|e| CredentialError::NotFound(e.to_string()))?;
        let mut session = self
            .take_session(&input.name)
            .map_err(|e| CredentialError::NotFound(e.to_string()))?;

        let result = async {
            let store = session
                .store(CREDENTIALS_STORE)
                .await
                .map_err(|e| CredentialError::NotFound(e.to_string()))?;

            let js_key = JsValue::from_str(store_key);

            let existing = store
                .query(|object_store| async move {
                    object_store
                        .get(js_key)
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))
                })
                .await?;

            if let Some(js_val) = existing {
                let export = SignerCredentialExport(js_val);
                let credential = SignerCredential::import(export)
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;
                return Ok(ProfileSigner::new(credential.0));
            }

            let signer = Ed25519Signer::generate()
                .await
                .map_err(|e| CredentialError::NotFound(e.to_string()))?;

            let credential = SignerCredential::from(signer);
            let export = credential
                .export()
                .await
                .map_err(|e| CredentialError::NotFound(e.to_string()))?;

            let js_key = JsValue::from_str(store_key);
            let js_val: JsValue = export.0;
            store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_val, Some(&js_key))
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;
                    Ok::<(), CredentialError>(())
                })
                .await?;

            Ok(ProfileSigner::new(credential.0))
        }
        .await;

        self.return_session(&input.name, session);
        result
    }
}
