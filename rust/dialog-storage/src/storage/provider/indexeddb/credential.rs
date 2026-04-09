//! Credential capability providers for IndexedDb.

use super::{IndexedDb, IndexedDbError};
use async_trait::async_trait;
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::Credential;
use dialog_credentials::credential::CredentialExport;
use dialog_effects::credential::{self, CredentialError};
use wasm_bindgen::JsValue;

const CREDENTIAL_STORE: &str = "credential";

#[async_trait(?Send)]
impl Provider<credential::Load> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Credential, CredentialError> {
        let address = credential::Name::of(&input).name.clone();
        let subject = input.subject().into();

        self.open(&subject).await?;
        let mut session = self.take_session(&subject)?;

        let result = async {
            let store = session.store(CREDENTIAL_STORE).await?;
            let key = JsValue::from_str(&address);

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
                None => Err(CredentialError::NotFound(address)),
            }
        }
        .await;

        self.return_session(subject, session);
        result
    }
}

#[async_trait(?Send)]
impl Provider<credential::Save> for IndexedDb {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let address = credential::Name::of(&input).name.clone();
        let credential = &credential::Save::of(&input).credential;
        let subject = input.subject().into();

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let js_val: JsValue = export.into();

        self.open(&subject).await?;
        let mut session = self.take_session(&subject)?;

        let result = async {
            let store = session.store(CREDENTIAL_STORE).await?;
            let key = JsValue::from_str(&address);

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
        .await;

        self.return_session(subject, session);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_credentials::{Ed25519Signer, SignerCredential};
    use dialog_effects::prelude::*;
    use dialog_varsig::Principal;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    async fn unique_did() -> dialog_capability::Did {
        let signer = Ed25519Signer::generate().await.unwrap();
        Principal::did(&signer)
    }

    async fn test_credential() -> Credential {
        let signer = Ed25519Signer::generate().await.unwrap();
        Credential::Signer(SignerCredential::from(signer))
    }

    #[dialog_common::test]
    async fn it_returns_not_found_for_missing_credential() -> anyhow::Result<()> {
        let provider = IndexedDb::new();
        let did = unique_did().await;

        let result = did.credential("self").load().perform(&provider).await;

        assert!(result.is_err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_saves_and_loads_credential() -> anyhow::Result<()> {
        let provider = IndexedDb::new();
        let did = unique_did().await;
        let cred = test_credential().await;
        let expected_did = cred.did();

        did.clone()
            .credential("self")
            .save(cred)
            .perform(&provider)
            .await?;

        let loaded = did.credential("self").load().perform(&provider).await?;

        assert_eq!(loaded.did(), expected_did);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_garbage_credential() -> anyhow::Result<()> {
        let provider = IndexedDb::new();
        let did = unique_did().await;
        let subject = did.clone().into();

        provider
            .open(&subject)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut session = provider
            .take_session(&subject)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        {
            let store = session
                .store(CREDENTIAL_STORE)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            store
                .transact(|object_store| async move {
                    let key = JsValue::from_str("self");
                    let garbage = JsValue::from_str("not a credential");
                    object_store
                        .put(&garbage, Some(&key))
                        .await
                        .map_err(|e| IndexedDbError::Store(e.to_string()))
                })
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        provider.return_session(subject, session);

        let result = did.credential("self").load().perform(&provider).await;

        assert!(result.is_err(), "should reject garbage credential data");
        Ok(())
    }
}
