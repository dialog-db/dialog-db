//! Credential capability provider for volatile storage.
//!
//! Implements credential retrieve/save by storing serialized JSON bytes
//! in the session's credentials HashMap, keyed by address ID.

use super::Volatile;
use async_trait::async_trait;
use dialog_capability::credential::{self, CredentialError};
use dialog_capability::{Capability, Policy, Provider};
use dialog_common::ConditionalSend;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Provider<credential::Retrieve<C>> for Volatile
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

        let sessions = self.sessions.read();
        let bytes = sessions
            .get(&subject)
            .and_then(|session| session.credentials.get(&address_id));

        match bytes {
            Some(data) => serde_json::from_slice(data)
                .map_err(|e| CredentialError::NotFound(format!("deserialization error: {e}"))),
            None => Err(CredentialError::NotFound(format!(
                "no credentials at '{address_id}'"
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Provider<credential::Save<C>> for Volatile
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    async fn execute(&self, input: Capability<credential::Save<C>>) -> Result<(), CredentialError> {
        let subject = input.subject().into();
        let effect = credential::Save::<C>::of(&input);
        let address_id = effect.address.id().to_string();
        let value = serde_json::to_vec(&effect.credentials)
            .map_err(|e| CredentialError::NotFound(format!("serialization error: {e}")))?;

        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.credentials.insert(address_id, value);

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Provider<credential::List<C>> for Volatile
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    async fn execute(
        &self,
        input: Capability<credential::List<C>>,
    ) -> Result<Vec<credential::Address<C>>, CredentialError> {
        let subject = input.subject().into();
        let prefix = credential::List::<C>::of(&input).prefix.id().to_string();

        let sessions = self.sessions.read();
        let addresses = sessions
            .get(&subject)
            .map(|session| {
                session
                    .credentials
                    .keys()
                    .filter(|key| key.starts_with(&prefix))
                    .map(|key| credential::Address::new(key.as_str()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(addresses)
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
        let did: Did = format!(
            "did:test:{}-{}",
            prefix,
            dialog_common::time::now()
                .duration_since(dialog_common::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
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
            .attenuate(credential::Profile::default())
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
            .attenuate(credential::Profile::default())
            .invoke(credential::Save {
                address: credential::Address::new(address_id),
                credentials,
            })
    }

    #[dialog_common::test]
    async fn it_returns_not_found_for_missing_credentials() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("cred-missing");

        let cap = build_retrieve_cap::<TestCredentials>(subject, "s3://my-bucket");
        let result: Result<TestCredentials, _> =
            <Volatile as Provider<credential::Retrieve<TestCredentials>>>::execute(&provider, cap)
                .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(CredentialError::NotFound(_))));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_credentials() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("cred-roundtrip");
        let creds = TestCredentials {
            access_key: "AKIA123".to_string(),
            secret_key: "secret456".to_string(),
        };

        <Volatile as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "s3://my-bucket", creds.clone()),
        )
        .await?;

        let result: TestCredentials = <Volatile as Provider<
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
        let provider = Volatile::new();
        let subject = unique_subject("cred-isolation");

        let creds1 = TestCredentials {
            access_key: "key1".to_string(),
            secret_key: "secret1".to_string(),
        };
        let creds2 = TestCredentials {
            access_key: "key2".to_string(),
            secret_key: "secret2".to_string(),
        };

        <Volatile as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "s3://bucket-a", creds1.clone()),
        )
        .await?;
        <Volatile as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "s3://bucket-b", creds2.clone()),
        )
        .await?;

        let result1: TestCredentials =
            <Volatile as Provider<credential::Retrieve<TestCredentials>>>::execute(
                &provider,
                build_retrieve_cap(subject.clone(), "s3://bucket-a"),
            )
            .await?;
        let result2: TestCredentials = <Volatile as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject, "s3://bucket-b")
        )
        .await?;

        assert_eq!(result1, creds1);
        assert_eq!(result2, creds2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_by_subject() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject1 = unique_subject("cred-subj-1");
        let subject2 = unique_subject("cred-subj-2");

        let creds = TestCredentials {
            access_key: "key".to_string(),
            secret_key: "secret".to_string(),
        };

        <Volatile as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject1.clone(), "s3://bucket", creds.clone()),
        )
        .await?;

        // Subject 1 should find it
        let result: TestCredentials = <Volatile as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject1, "s3://bucket")
        )
        .await?;
        assert_eq!(result, creds);

        // Subject 2 should not
        let result2: Result<TestCredentials, _> =
            <Volatile as Provider<credential::Retrieve<TestCredentials>>>::execute(
                &provider,
                build_retrieve_cap(subject2, "s3://bucket"),
            )
            .await;
        assert!(result2.is_err());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_overwrites_existing_credentials() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("cred-overwrite");

        let creds1 = TestCredentials {
            access_key: "old".to_string(),
            secret_key: "old-secret".to_string(),
        };
        let creds2 = TestCredentials {
            access_key: "new".to_string(),
            secret_key: "new-secret".to_string(),
        };

        <Volatile as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "addr", creds1),
        )
        .await?;
        <Volatile as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "addr", creds2.clone()),
        )
        .await?;

        let result: TestCredentials = <Volatile as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject, "addr")
        )
        .await?;

        assert_eq!(result, creds2);

        Ok(())
    }
}
