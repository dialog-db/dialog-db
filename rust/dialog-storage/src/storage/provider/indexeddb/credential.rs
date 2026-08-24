//! Credential capability providers for IndexedDb.

use super::{IndexedDb, IndexedDbError, to_uint8array};
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_credentials::Credential;
use dialog_credentials::credential::CredentialExport;
use dialog_effects::credential::prelude::{
    LoadCredentialExt, LoadSecretExt, RetractSecretExt, SaveCredentialExt, SaveSecretExt,
};
use dialog_effects::credential::{CredentialError, Load, Retract, Save, Secret};
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};

const CREDENTIAL: &str = "credential";

#[async_trait(?Send)]
impl Provider<Load<Credential>> for IndexedDb {
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        let idb_key = format!("key/{}", input.address());

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
        let idb_key = format!("key/{}", input.address());
        let export = input
            .credential()
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
        let idb_key = format!("site/{}", input.address());

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
        let idb_key = format!("site/{}", input.address());
        let js_val: JsValue = to_uint8array(input.secret().as_bytes()).into();

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
impl Provider<Retract<Secret>> for IndexedDb {
    async fn execute(&self, input: Capability<Retract<Secret>>) -> Result<(), CredentialError> {
        let idb_key = format!("site/{}", input.address());

        let store = self.store(CREDENTIAL).await?;
        let key = JsValue::from_str(&idb_key);

        store
            .transact(|object_store| async move {
                object_store
                    .delete(key)
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
    async fn it_retracts_a_stored_site_secret() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("cred-retract")).await?;
        let did = unique_did().await;

        did.clone()
            .credential()
            .site("example.com")
            .save(Secret::from(vec![1u8, 2, 3]))
            .perform(&provider)
            .await?;

        did.clone()
            .credential()
            .site("example.com")
            .retract()
            .perform(&provider)
            .await?;

        let result = did
            .credential()
            .site("example.com")
            .load()
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(CredentialError::NotFound(_))));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_a_missing_site_secret_without_error() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("cred-retract-absent")).await?;
        let did = unique_did().await;

        did.credential()
            .site("never-saved.example")
            .retract()
            .perform(&provider)
            .await?;

        Ok(())
    }

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

    // The agreement key must outlive a session. A browser signer cannot
    // re-derive it (its Ed25519 key is non-extractable and yields no seed), so
    // it has to survive IndexedDb's structured clone as part of the stored
    // credential. Without this, every reload would produce a signer that can
    // sign but cannot open anything previously sealed to its DID.
    #[dialog_common::test]
    async fn it_preserves_the_agreement_key_across_a_session() -> anyhow::Result<()> {
        use dialog_credentials::Ed25519Signer;
        use dialog_credentials::secret::Context;
        use dialog_credentials::{Ed25519Verifier, Signer as CredentialSigner};

        const VAULT: Context = Context::new("dialog/vault/test");

        let provider = IndexedDb::connect(unique_name("cred-agreement")).await?;
        let address = unique_did().await;

        // Session 1: a profile is generated and stored.
        let profile = Ed25519Signer::generate().await.unwrap();
        let profile_did = profile.ed25519_did().to_string();
        address
            .clone()
            .credential()
            .key("self")
            .save(Credential::Signer(profile.into()))
            .perform(&provider)
            .await?;

        // The account seals a vault secret knowing only the DID.
        let sealed = profile_did
            .parse::<Ed25519Verifier>()
            .unwrap()
            .secret(VAULT)
            .conceal(&[9u8; 32])
            .await
            .unwrap();

        // Session 2: the profile is loaded back out of storage.
        let loaded = address
            .credential()
            .key("self")
            .load()
            .perform(&provider)
            .await?;

        let Credential::Signer(signer) = loaded else {
            panic!("expected a signer credential");
        };
        let CredentialSigner::Ed25519(signer) = signer.into_signer() else {
            panic!("expected an ed25519 signer");
        };

        assert_eq!(
            signer.ed25519_did().to_string(),
            profile_did,
            "the reloaded credential should be the same identity"
        );
        assert_eq!(
            signer.secret(VAULT).reveal(&sealed).await.unwrap(),
            [9u8; 32],
            "a reloaded credential should open a secret sealed to its DID"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_the_agreement_key_across_two_sessions() -> anyhow::Result<()> {
        use dialog_credentials::Ed25519Signer;
        use dialog_credentials::secret::Context;
        use dialog_credentials::{Ed25519Verifier, Signer as CredentialSigner};

        const VAULT: Context = Context::new("dialog/vault/test");

        let provider = IndexedDb::connect(unique_name("cred-agreement-twice")).await?;
        let address = unique_did().await;

        let profile = Ed25519Signer::generate().await.unwrap();
        let profile_did = profile.ed25519_did().to_string();
        address
            .clone()
            .credential()
            .key("self")
            .save(Credential::Signer(profile.into()))
            .perform(&provider)
            .await?;

        let sealed = profile_did
            .parse::<Ed25519Verifier>()
            .unwrap()
            .secret(VAULT)
            .conceal(b"two sessions")
            .await
            .unwrap();

        // Load and re-save, as a long-lived app would across restarts.
        let first = address
            .clone()
            .credential()
            .key("self")
            .load()
            .perform(&provider)
            .await?;
        address
            .clone()
            .credential()
            .key("self")
            .save(first)
            .perform(&provider)
            .await?;

        let second = address
            .credential()
            .key("self")
            .load()
            .perform(&provider)
            .await?;

        let Credential::Signer(signer) = second else {
            panic!("expected a signer credential");
        };
        let CredentialSigner::Ed25519(signer) = signer.into_signer() else {
            panic!("expected an ed25519 signer");
        };

        assert_eq!(
            signer.secret(VAULT).reveal(&sealed).await.unwrap(),
            b"two sessions",
            "the agreement key should survive a second storage round trip"
        );

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
