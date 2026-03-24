//! Credential capability provider for IndexedDB.
//!
//! Stores credentials as JSON bytes in a `credentials` object store,
//! keyed by credential address ID.

use super::{IndexedDb, IndexedDbError, to_uint8array};
use async_trait::async_trait;
use dialog_capability::credential::{self, CredentialError};
use dialog_capability::{Capability, Policy, Provider};
use dialog_common::ConditionalSend;
use js_sys::Uint8Array;
use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::{JsCast, JsValue};

/// The single object store used for all credential operations.
const CREDENTIALS_STORE: &str = "credentials";

impl From<IndexedDbError> for CredentialError {
    fn from(e: IndexedDbError) -> Self {
        CredentialError::NotFound(e.to_string())
    }
}

#[async_trait(?Send)]
impl<C> Provider<credential::Retrieve<C>> for IndexedDb
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    async fn execute(
        &self,
        input: Capability<credential::Retrieve<C>>,
    ) -> Result<C, CredentialError> {
        let subject = input.subject().into();
        let address_id = credential::Retrieve::<C>::of(&input)
            .address
            .id()
            .to_string();

        self.open(&subject).await?;
        let mut session = self.take_session(&subject)?;

        let result = async {
            let store = session.store(CREDENTIALS_STORE).await?;
            let key = JsValue::from_str(&address_id);

            store
                .query(|object_store| async move {
                    let value = object_store
                        .get(key)
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                    let Some(value) = value else {
                        return Err(CredentialError::NotFound(format!(
                            "no credentials at '{address_id}'"
                        )));
                    };

                    let bytes = value
                        .dyn_into::<Uint8Array>()
                        .map_err(|_| {
                            CredentialError::NotFound("value is not Uint8Array".to_string())
                        })?
                        .to_vec();

                    serde_json::from_slice(&bytes).map_err(|e| {
                        CredentialError::NotFound(format!("deserialization error: {e}"))
                    })
                })
                .await
        }
        .await;

        self.return_session(subject, session);
        result
    }
}

#[async_trait(?Send)]
impl<C> Provider<credential::Save<C>> for IndexedDb
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    async fn execute(&self, input: Capability<credential::Save<C>>) -> Result<(), CredentialError> {
        let subject = input.subject().into();
        let effect = credential::Save::<C>::of(&input);
        let address_id = effect.address.id().to_string();
        let value = serde_json::to_vec(&effect.credentials)
            .map_err(|e| CredentialError::NotFound(format!("serialization error: {e}")))?;

        self.open(&subject).await?;
        let mut session = self.take_session(&subject)?;

        let result = async {
            let store = session.store(CREDENTIALS_STORE).await?;
            let key = JsValue::from_str(&address_id);
            let js_value: JsValue = to_uint8array(&value).into();

            store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_value, Some(&key))
                        .await
                        .map_err(|e| CredentialError::NotFound(e.to_string()))?;
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
    use dialog_capability::{Did, Subject, credential};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestCredentials {
        access_key: String,
        secret_key: String,
    }

    fn unique_subject(prefix: &str) -> Subject {
        let did: Did = format!("did:test:{}-{}", prefix, js_sys::Date::now() as u64)
            .parse()
            .unwrap();
        Subject::from(did)
    }

    fn build_retrieve_cap<C: Serialize + DeserializeOwned + ConditionalSend + 'static>(
        subject: Subject,
        address_id: &str,
    ) -> Capability<credential::Retrieve<C>> {
        subject
            .attenuate(credential::Credential)
            .invoke(credential::Retrieve {
                address: credential::Address::new(address_id),
            })
    }

    fn build_save_cap<C: Serialize + DeserializeOwned + ConditionalSend + 'static>(
        subject: Subject,
        address_id: &str,
        credentials: C,
    ) -> Capability<credential::Save<C>> {
        subject
            .attenuate(credential::Credential)
            .invoke(credential::Save {
                address: credential::Address::new(address_id),
                credentials,
            })
    }

    #[dialog_common::test]
    async fn it_returns_not_found_for_missing_credentials() -> anyhow::Result<()> {
        let provider = IndexedDb::new();
        let subject = unique_subject("idb-cred-missing");

        let result: Result<TestCredentials, _> =
            <IndexedDb as Provider<credential::Retrieve<TestCredentials>>>::execute(
                &provider,
                build_retrieve_cap(subject, "s3://my-bucket"),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(CredentialError::NotFound(_))));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_credentials() -> anyhow::Result<()> {
        let provider = IndexedDb::new();
        let subject = unique_subject("idb-cred-roundtrip");
        let creds = TestCredentials {
            access_key: "AKIA123".to_string(),
            secret_key: "secret456".to_string(),
        };

        <IndexedDb as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "s3://my-bucket", creds.clone()),
        )
        .await?;

        let result: TestCredentials = <IndexedDb as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject, "s3://my-bucket")
        )
        .await?;

        assert_eq!(result, creds);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_by_address() -> anyhow::Result<()> {
        let provider = IndexedDb::new();
        let subject = unique_subject("idb-cred-isolation");

        let creds1 = TestCredentials {
            access_key: "key1".to_string(),
            secret_key: "secret1".to_string(),
        };
        let creds2 = TestCredentials {
            access_key: "key2".to_string(),
            secret_key: "secret2".to_string(),
        };

        <IndexedDb as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-a", creds1.clone()),
        )
        .await?;
        <IndexedDb as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-b", creds2.clone()),
        )
        .await?;

        let result1: TestCredentials = <IndexedDb as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject.clone(), "bucket-a")
        )
        .await?;
        let result2: TestCredentials = <IndexedDb as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject, "bucket-b")
        )
        .await?;

        assert_eq!(result1, creds1);
        assert_eq!(result2, creds2);

        Ok(())
    }
}
