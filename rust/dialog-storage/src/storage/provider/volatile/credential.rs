//! Credential capability provider for volatile storage.
//!
//! Implements credential load/save by storing raw exported bytes
//! in the session's credentials HashMap, keyed by credential name.

use super::Volatile;
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::credential::{self, CredentialError, Identity, LoadCapability, SaveCapability};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<credential::Load> for Volatile {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Option<Identity>, CredentialError> {
        let subject = input.subject().into();
        let name = input.name().to_string();

        let export = {
            let sessions = self.sessions.read();
            sessions
                .get(&subject)
                .and_then(|session| session.credentials.get(&name))
                .cloned()
        };

        let Some(export) = export else {
            return Ok(None);
        };

        let credential = Identity::import(export)
            .await
            .map_err(|e| CredentialError::Corrupted(e.to_string()))?;

        Ok(Some(credential))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<credential::Save> for Volatile {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let subject = input.subject().into();
        let name = input.name().to_string();
        let credential = input.credential();

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.credentials.insert(name, export);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::credential;
    use dialog_varsig::Principal;

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

    fn build_load_cap(subject: Subject, name: &str) -> Capability<credential::Load> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(name))
            .invoke(credential::Load)
    }

    fn build_save_cap(
        subject: Subject,
        name: &str,
        credential: Identity,
    ) -> Capability<credential::Save> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(name))
            .invoke(credential::Save::new(credential))
    }

    async fn make_signer_credential() -> Identity {
        let signer = dialog_credentials::Ed25519Signer::generate().await.unwrap();
        Identity::from(signer)
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_credentials() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("cred-missing");

        let result = <Volatile as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "s3-bucket"),
        )
        .await?;

        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_credentials() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("cred-roundtrip");
        let cred = make_signer_credential().await;
        let expected_did = cred.did();

        <Volatile as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "s3-bucket", cred),
        )
        .await?;

        let result = <Volatile as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "s3-bucket"),
        )
        .await?;

        let loaded = result.expect("credential should exist");
        assert_eq!(loaded.did(), expected_did);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_by_name() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("cred-isolation");

        let cred_a = make_signer_credential().await;
        let cred_b = make_signer_credential().await;
        let did_a = cred_a.did();
        let did_b = cred_b.did();

        <Volatile as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-a", cred_a),
        )
        .await?;
        <Volatile as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-b", cred_b),
        )
        .await?;

        let result1 = <Volatile as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject.clone(), "bucket-a"),
        )
        .await?;
        let result2 = <Volatile as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "bucket-b"),
        )
        .await?;

        assert_eq!(result1.unwrap().did(), did_a);
        assert_eq!(result2.unwrap().did(), did_b);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_by_subject() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject1 = unique_subject("cred-subj-1");
        let subject2 = unique_subject("cred-subj-2");

        let cred = make_signer_credential().await;
        let expected_did = cred.did();

        <Volatile as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject1.clone(), "bucket", cred),
        )
        .await?;

        // Subject 1 should find it
        let result = <Volatile as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject1, "bucket"),
        )
        .await?;
        assert_eq!(result.unwrap().did(), expected_did);

        // Subject 2 should not
        let result2 = <Volatile as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject2, "bucket"),
        )
        .await?;
        assert!(result2.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_overwrites_existing_credentials() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("cred-overwrite");

        let cred1 = make_signer_credential().await;
        let cred2 = make_signer_credential().await;
        let expected_did = cred2.did();

        <Volatile as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "addr", cred1),
        )
        .await?;
        <Volatile as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "addr", cred2),
        )
        .await?;

        let result = <Volatile as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "addr"),
        )
        .await?;

        assert_eq!(result.unwrap().did(), expected_did);

        Ok(())
    }
}
