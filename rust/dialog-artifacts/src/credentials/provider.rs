//! Provider implementations for credential commands.

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use async_trait::async_trait;
    use dialog_capability::Provider;
    use dialog_capability::credential::CredentialError;
    use dialog_credentials::Ed25519Signer;
    use dialog_credentials::key::KeyExport;
    use dialog_storage::provider::FileSystem;

    use crate::credentials::open::{Open, ProfileSigner};

    #[async_trait]
    impl Provider<Open> for FileSystem {
        async fn execute(&self, input: Open) -> Result<ProfileSigner, CredentialError> {
            let location = self
                .resolve("profile")
                .and_then(|loc| loc.resolve(&input.name))
                .and_then(|loc| loc.resolve("key"))
                .map_err(|e| CredentialError::NotFound(e.to_string()))?;

            match location.read().await {
                Ok(data) if data.len() == 32 => {
                    let seed: [u8; 32] = data
                        .try_into()
                        .map_err(|_| CredentialError::NotFound("invalid seed length".into()))?;
                    let signer = Ed25519Signer::import(&seed)
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;
                    Ok(ProfileSigner::new(signer))
                }
                Ok(data) => Err(CredentialError::NotFound(format!(
                    "profile key has invalid length: {} (expected 32)",
                    data.len()
                ))),
                Err(_) => {
                    let signer = Ed25519Signer::generate()
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                    let KeyExport::Extractable(ref bytes) = signer
                        .export()
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                    location
                        .write(bytes)
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                    Ok(ProfileSigner::new(signer))
                }
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web {
    use async_trait::async_trait;
    use dialog_capability::Provider;
    use dialog_capability::credential::CredentialError;
    use dialog_credentials::Ed25519Signer;
    use dialog_credentials::key::KeyExport;
    use dialog_storage::provider::IndexedDb;
    use wasm_bindgen::JsValue;

    use crate::credentials::open::{Open, ProfileSigner};

    const CREDENTIALS_STORE: &str = "credentials";

    #[async_trait(?Send)]
    impl Provider<Open> for IndexedDb {
        async fn execute(&self, input: Open) -> Result<ProfileSigner, CredentialError> {
            let store_key = format!("profile/{}", input.name);

            self.open("dialog")
                .await
                .map_err(|e| CredentialError::NotFound(e.to_string()))?;
            let mut session = self
                .take_session("dialog")
                .map_err(|e| CredentialError::NotFound(e.to_string()))?;

            let result = async {
                let store = session
                    .store(CREDENTIALS_STORE)
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                let js_key = JsValue::from_str(&store_key);

                // Try to load existing key pair
                let existing = store
                    .query(|object_store| async move {
                        object_store
                            .get(js_key)
                            .await
                            .map_err(|e| CredentialError::NotFound(e.to_string()))
                    })
                    .await?;

                if let Some(js_val) = existing {
                    let export = KeyExport::try_from(js_val)
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;
                    let signer = Ed25519Signer::import(export)
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;
                    return Ok(ProfileSigner::new(signer));
                }

                // Generate new non-extractable key and store it
                let signer = Ed25519Signer::generate()
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                let export = signer
                    .export()
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                let js_key = JsValue::from_str(&store_key);
                let js_val: JsValue = export.into();
                store
                    .transact(|object_store| async move {
                        object_store
                            .put(&js_val, Some(&js_key))
                            .await
                            .map_err(|e| CredentialError::NotFound(e.to_string()))?;
                        Ok::<(), CredentialError>(())
                    })
                    .await?;

                Ok(ProfileSigner::new(signer))
            }
            .await;

            self.return_session("dialog", session);
            result
        }
    }
}
