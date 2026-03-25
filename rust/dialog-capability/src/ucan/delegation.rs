//! UCAN delegation storage and authorization extensions.
//!
//! Stores individual UCAN delegations in the credential store using credential
//! effects, and provides blanket extension traits for UCAN authorization.
//!
//! # Storage Layout
//!
//! Delegations are stored as credentials with addresses:
//!
//! - Subject-specific: `ucan/{audience}/{subject}/{issuer}.{cid}`
//! - Powerline (`sub: *`): `ucan/{audience}/_/{issuer}.{cid}`
//!
//! This layout enables efficient lookup: list by `ucan/{audience}/{subject}/`
//! to find all delegations granted to an operator for a given subject.
//! Entries where `issuer == subject` are direct grants (no further chain needed).

use crate::credential::{self, CredentialError};
use crate::{Capability, Did, Policy, Provider};
use dialog_common::ConditionalSync;
use dialog_ucan::DelegationChain;
use dialog_ucan::subject::Subject;

pub fn delegation_prefix(audience: &Did, subject: &Did) -> String {
    format!("ucan/{}/{}/", audience, subject)
}

pub fn powerline_prefix(audience: &Did) -> String {
    format!("ucan/{}/_/", audience)
}

fn delegation_key(
    audience: &Did,
    subject: &Did,
    issuer: &Did,
    cid: &ipld_core::cid::Cid,
) -> String {
    format!("ucan/{}/{}/{}.{}", audience, subject, issuer, cid)
}

fn powerline_key(audience: &Did, issuer: &Did, cid: &ipld_core::cid::Cid) -> String {
    format!("ucan/{}/_/{}.{}", audience, issuer, cid)
}

/// Parse an issuer DID and CID string from a key's filename portion.
///
/// Given `ucan/{aud}/{sub}/{issuer}.{cid}`, extracts `(issuer, cid)` from the last segment.
pub fn parse_key_suffix(key: &str) -> Option<(String, String)> {
    let filename = key.rsplit('/').next()?;
    // DIDs contain colons (did:key:z...), so split on the last dot only
    let dot_pos = filename.rfind('.')?;
    let issuer = &filename[..dot_pos];
    let cid_str = &filename[dot_pos + 1..];
    if issuer.is_empty() || cid_str.is_empty() {
        return None;
    }
    Some((issuer.to_string(), cid_str.to_string()))
}

/// Build a credential capability for delegation storage.
pub fn cred_cap(subject: &Did) -> Capability<credential::Credential> {
    crate::Subject::from(subject.clone()).attenuate(credential::Credential)
}

/// Store a delegation chain's individual delegations into the credential store.
///
/// Each delegation is stored separately at its computed key path so that
/// chain discovery can find them by listing prefixes.
pub async fn import_delegation_chain<Env>(
    env: &Env,
    subject: &Did,
    chain: &DelegationChain,
) -> Result<(), credential::CredentialError>
where
    Env: Provider<credential::Save<Vec<u8>>> + ConditionalSync,
{
    for (cid, delegation) in chain.delegations() {
        let audience = delegation.audience();
        let key = match delegation.subject() {
            Subject::Specific(did) => delegation_key(audience, did, delegation.issuer(), cid),
            Subject::Any => powerline_key(audience, delegation.issuer(), cid),
        };

        let bytes = serde_ipld_dagcbor::to_vec(delegation.as_ref())
            .map_err(|e| credential::CredentialError::SigningFailed(e.to_string()))?;

        let save_cap = cred_cap(subject).invoke(credential::Save {
            address: credential::Address::new(key),
            credentials: bytes,
        });

        env.execute(save_cap).await?;
    }
    Ok(())
}

/// Blanket impl: any type that can save credentials can import delegation chains.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> Provider<credential::Import<DelegationChain>> for Env
where
    Env: Provider<credential::Save<Vec<u8>>> + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Import<DelegationChain>>,
    ) -> Result<(), CredentialError> {
        let subject = input.subject().clone();
        let chain = &credential::Import::<DelegationChain>::of(&input).material;
        import_delegation_chain(self, &subject, chain).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_ucan::DelegationChain;
    use dialog_ucan::delegation::builder::DelegationBuilder;
    use dialog_ucan::subject::Subject;
    use dialog_varsig::Principal;
    use dialog_varsig::eddsa::Ed25519Signature;

    // Minimal test provider that stores credentials in-memory using HashMaps
    // (no dependency on dialog-storage or Volatile)
    mod test_provider {
        use crate::authority;
        use crate::credential::{self, CredentialError};
        use crate::{Capability, Policy, Provider, Subject};
        use async_trait::async_trait;
        use dialog_common::ConditionalSend;
        use serde::Serialize;
        use serde::de::DeserializeOwned;
        use std::collections::HashMap;
        use std::sync::RwLock;

        pub struct TestStore {
            data: RwLock<HashMap<String, HashMap<String, Vec<u8>>>>,
            signer: dialog_credentials::Ed25519Signer,
        }

        impl TestStore {
            pub fn new(signer: dialog_credentials::Ed25519Signer) -> Self {
                Self {
                    data: RwLock::new(HashMap::new()),
                    signer,
                }
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl<C> Provider<credential::Save<C>> for TestStore
        where
            C: Serialize + DeserializeOwned + ConditionalSend + 'static,
        {
            async fn execute(
                &self,
                input: Capability<credential::Save<C>>,
            ) -> Result<(), CredentialError> {
                let subject: String = input.subject().to_string();
                let effect = credential::Save::<C>::of(&input);
                let address_id = effect.address.id().to_string();
                let value = serde_json::to_vec(&effect.credentials)
                    .map_err(|e| CredentialError::NotFound(format!("serialization error: {e}")))?;

                let mut data = self.data.write().unwrap();
                let entry = data.entry(subject).or_default();
                entry.insert(address_id, value);
                Ok(())
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl<C> Provider<credential::Retrieve<C>> for TestStore
        where
            C: Serialize + DeserializeOwned + ConditionalSend + 'static,
        {
            async fn execute(
                &self,
                input: Capability<credential::Retrieve<C>>,
            ) -> Result<C, CredentialError> {
                let subject: String = input.subject().to_string();
                let address_id = credential::Retrieve::<C>::of(&input)
                    .address
                    .id()
                    .to_string();

                let data = self.data.read().unwrap();
                let bytes = data
                    .get(&subject)
                    .and_then(|session| session.get(&address_id));

                match bytes {
                    Some(data) => serde_json::from_slice(data).map_err(|e| {
                        CredentialError::NotFound(format!("deserialization error: {e}"))
                    }),
                    None => Err(CredentialError::NotFound(format!(
                        "no credentials at '{address_id}'"
                    ))),
                }
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl<C> Provider<credential::List<C>> for TestStore
        where
            C: Serialize + DeserializeOwned + ConditionalSend + 'static,
        {
            async fn execute(
                &self,
                input: Capability<credential::List<C>>,
            ) -> Result<Vec<credential::Address<C>>, CredentialError> {
                let subject: String = input.subject().to_string();
                let prefix = credential::List::<C>::of(&input).prefix.id().to_string();

                let data = self.data.read().unwrap();
                let addresses = data
                    .get(&subject)
                    .map(|session| {
                        session
                            .keys()
                            .filter(|key| key.starts_with(&prefix))
                            .map(|key| credential::Address::new(key.as_str()))
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(addresses)
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl Provider<authority::Identify> for TestStore {
            async fn execute(
                &self,
                input: Capability<authority::Identify>,
            ) -> Result<authority::Authority, CredentialError> {
                let did = dialog_varsig::Principal::did(&self.signer);
                let subject_did = input.subject().clone();
                Ok(Subject::from(subject_did)
                    .attenuate(authority::Profile::local(did.clone()))
                    .attenuate(authority::Operator::new(did)))
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl Provider<authority::Sign> for TestStore {
            async fn execute(
                &self,
                input: Capability<authority::Sign>,
            ) -> Result<Vec<u8>, CredentialError> {
                use dialog_varsig::Signer;
                let payload = authority::Sign::of(&input).payload.clone();
                let sig = self
                    .signer
                    .sign(&payload)
                    .await
                    .map_err(|e| CredentialError::SigningFailed(e.to_string()))?;
                Ok(sig.to_bytes().to_vec())
            }
        }
    }

    use test_provider::TestStore;

    async fn signer_async(seed: u8) -> dialog_credentials::Ed25519Signer {
        dialog_credentials::Ed25519Signer::import(&[seed; 32])
            .await
            .expect("Failed to import signer")
    }

    fn test_env(signer: dialog_credentials::Ed25519Signer) -> TestStore {
        TestStore::new(signer)
    }

    async fn build_delegation(
        issuer: &dialog_credentials::Ed25519Signer,
        audience: &impl Principal,
        subject: &impl Principal,
        cmd_segments: Vec<String>,
    ) -> dialog_ucan::Delegation<Ed25519Signature> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(Subject::Specific(subject.did()))
            .command(cmd_segments)
            .try_build()
            .await
            .expect("Failed to build delegation")
    }

    fn cmd(s: &str) -> Vec<String> {
        if s == "/" {
            vec![]
        } else {
            s.trim_start_matches('/')
                .split('/')
                .map(|seg| seg.to_string())
                .collect()
        }
    }

    async fn import_single(
        env: &TestStore,
        subject: &Did,
        delegation: dialog_ucan::Delegation<Ed25519Signature>,
    ) {
        let chain = DelegationChain::new(delegation);
        import_delegation_chain(env, subject, &chain)
            .await
            .expect("Failed to import delegation");
    }

    #[dialog_common::test]
    fn parse_key_suffix_valid() {
        let key = "ucan/did:key:zOperator/did:key:zSubject/did:key:zIssuer.bafy123";
        let (issuer, cid) = parse_key_suffix(key).expect("Should parse valid key");
        assert_eq!(issuer, "did:key:zIssuer");
        assert_eq!(cid, "bafy123");
    }

    #[dialog_common::test]
    fn parse_key_suffix_did_with_colons() {
        let key = "ucan/did:key:z6MkOperator/did:key:z6MkSubject/did:key:z6MkfFJBxSBFgoAqTQLS7bTfP8MgyDypva5i6CL5PJN8RJZr.bafyreihxyz";
        let (issuer, cid) = parse_key_suffix(key).expect("Should parse DID with colons");
        assert_eq!(
            issuer,
            "did:key:z6MkfFJBxSBFgoAqTQLS7bTfP8MgyDypva5i6CL5PJN8RJZr"
        );
        assert_eq!(cid, "bafyreihxyz");
    }

    #[dialog_common::test]
    fn parse_key_suffix_invalid_no_dot() {
        assert!(parse_key_suffix("ucan/aud/sub/issuernodot").is_none());
    }

    #[dialog_common::test]
    fn parse_key_suffix_empty_issuer() {
        assert!(parse_key_suffix("ucan/aud/sub/.cid").is_none());
    }

    #[dialog_common::test]
    fn parse_key_suffix_empty_cid() {
        assert!(parse_key_suffix("ucan/aud/sub/issuer.").is_none());
    }

    #[dialog_common::test]
    fn parse_key_suffix_empty_string() {
        assert!(parse_key_suffix("").is_none());
    }

    #[dialog_common::test]
    async fn import_single_delegation_stores_at_correct_key() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_delegation(&subject_signer, &operator_signer, &subject_signer, cmd("/")).await;
        let cid = delegation.to_cid();

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        let expected_key = format!(
            "ucan/{}/{}/{}.{}",
            operator_did, subject_did, subject_did, cid
        );

        // Retrieve via credential effect
        let retrieve_cap = cred_cap(&subject_did).invoke(credential::Retrieve::<Vec<u8>> {
            address: credential::Address::new(&expected_key),
        });
        let result =
            <TestStore as Provider<credential::Retrieve<Vec<u8>>>>::execute(&env, retrieve_cap)
                .await;
        assert!(
            result.is_ok(),
            "Expected key '{}' not found in store",
            expected_key
        );
    }

    #[dialog_common::test]
    async fn import_multi_hop_chain_stores_each_delegation() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();

        // subject -> account
        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;
        let d1_cid = d1.to_cid();

        // account -> operator
        let d2 =
            build_delegation(&account_signer, &operator_signer, &subject_signer, cmd("/")).await;
        let d2_cid = d2.to_cid();

        // Build chain: [d2, d1] (closest to invoker first)
        let chain = DelegationChain::try_from(vec![d2.clone(), d1.clone()]).expect("valid chain");

        let env = test_env(operator_signer.clone());
        import_delegation_chain(&env, &subject_did, &chain)
            .await
            .expect("import should succeed");

        // Verify both delegations are stored
        let key1 = delegation_key(
            &account_signer.did(),
            &subject_did,
            &subject_signer.did(),
            &d1_cid,
        );
        let key2 = delegation_key(
            &operator_signer.did(),
            &subject_did,
            &account_signer.did(),
            &d2_cid,
        );

        let r1 = <TestStore as Provider<credential::Retrieve<Vec<u8>>>>::execute(
            &env,
            cred_cap(&subject_did).invoke(credential::Retrieve::<Vec<u8>> {
                address: credential::Address::new(&key1),
            }),
        )
        .await;
        let r2 = <TestStore as Provider<credential::Retrieve<Vec<u8>>>>::execute(
            &env,
            cred_cap(&subject_did).invoke(credential::Retrieve::<Vec<u8>> {
                address: credential::Address::new(&key2),
            }),
        )
        .await;
        assert!(r1.is_ok(), "d1 should be stored at {}", key1);
        assert!(r2.is_ok(), "d2 should be stored at {}", key2);
    }
}
