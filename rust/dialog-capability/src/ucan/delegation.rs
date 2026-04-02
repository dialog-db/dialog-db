//! UCAN delegation storage and authorization extensions.
//!
//! Stores individual UCAN delegations in the credential store using storage
//! effects, and provides blanket extension traits for UCAN authorization.
//!
//! # Storage Layout
//!
//! Delegations are stored in the `"ucan"` store with keys:
//!
//! - Subject-specific: `{audience}/{subject}/{issuer}.{cid}`
//! - Powerline (`sub: *`): `{audience}/_/{issuer}.{cid}`
//!
//! This layout enables efficient lookup: list by `{audience}/{subject}/`
//! to find all delegations granted to an operator for a given subject.
//! Entries where `issuer == subject` are direct grants (no further chain needed).

use crate::storage::{self, Storage, StorageError, Store};
use crate::{Capability, Did, Provider};
use dialog_common::ConditionalSync;
use dialog_ucan::DelegationChain;
use dialog_ucan::subject::Subject;

const UCAN_STORE: &str = "ucan";

pub fn delegation_prefix(audience: &Did, subject: &Did) -> String {
    format!("{}/{}/", audience, subject)
}

pub fn powerline_prefix(audience: &Did) -> String {
    format!("{}/_/", audience)
}

fn delegation_key(
    audience: &Did,
    subject: &Did,
    issuer: &Did,
    cid: &ipld_core::cid::Cid,
) -> String {
    format!("{}/{}/{}.{}", audience, subject, issuer, cid)
}

fn powerline_key(audience: &Did, issuer: &Did, cid: &ipld_core::cid::Cid) -> String {
    format!("{}/_/{}.{}", audience, issuer, cid)
}

/// Parse an issuer DID and CID string from a key's filename portion.
///
/// Given `{aud}/{sub}/{issuer}.{cid}`, extracts `(issuer, cid)` from the last segment.
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

/// Build a storage capability scoped to the "ucan" store for the given subject.
fn ucan_store(subject: &Did) -> Capability<Store> {
    crate::Subject::from(subject.clone())
        .attenuate(Storage)
        .attenuate(Store::new(UCAN_STORE))
}

/// Store a delegation chain's individual delegations into the storage backend.
///
/// Each delegation is stored separately at its computed key path so that
/// chain discovery can find them by listing prefixes.
pub async fn import_delegation_chain<Env>(
    env: &Env,
    subject: &Did,
    chain: &DelegationChain,
) -> Result<(), StorageError>
where
    Env: Provider<storage::Set> + ConditionalSync,
{
    for (cid, delegation) in chain.delegations() {
        let audience = delegation.audience();
        let key = match delegation.subject() {
            Subject::Specific(did) => delegation_key(audience, did, delegation.issuer(), cid),
            Subject::Any => powerline_key(audience, delegation.issuer(), cid),
        };

        let bytes = serde_ipld_dagcbor::to_vec(delegation.as_ref())
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        ucan_store(subject)
            .invoke(storage::Set::new(key.as_bytes(), bytes))
            .perform(env)
            .await?;
    }
    Ok(())
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
    mod test_provider {
        use crate::authority;
        use crate::storage::{self, StorageError};
        use crate::{Capability, Policy, Provider, Subject};
        use async_trait::async_trait;
        use std::collections::HashMap;
        use std::sync::RwLock;

        use crate::storage::{GetCapability, ListCapability, SetCapability};

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
        impl Provider<storage::Set> for TestStore {
            async fn execute(&self, input: Capability<storage::Set>) -> Result<(), StorageError> {
                let subject = input.subject().to_string();
                let key = String::from_utf8_lossy(input.key()).to_string();
                let value = input.value().to_vec();

                let mut data = self.data.write().unwrap();
                let entry = data.entry(subject).or_default();
                entry.insert(key, value);
                Ok(())
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl Provider<storage::Get> for TestStore {
            async fn execute(
                &self,
                input: Capability<storage::Get>,
            ) -> Result<Option<Vec<u8>>, StorageError> {
                let subject = input.subject().to_string();
                let key = String::from_utf8_lossy(input.key()).to_string();

                let data = self.data.read().unwrap();
                let value = data
                    .get(&subject)
                    .and_then(|session| session.get(&key))
                    .cloned();

                Ok(value)
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl Provider<storage::List> for TestStore {
            async fn execute(
                &self,
                input: Capability<storage::List>,
            ) -> Result<storage::ListResult, StorageError> {
                let subject = input.subject().to_string();
                let store_name = input.store();

                // The continuation_token is used as a prefix filter in tests
                // We use the store name to scope, but the prefix is passed via
                // continuation_token in the real API. For tests, we extract the
                // prefix from the store's keys.
                let _ = store_name;

                let data = self.data.read().unwrap();
                let keys = data
                    .get(&subject)
                    .map(|session| session.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();

                Ok(storage::ListResult {
                    keys,
                    is_truncated: false,
                    next_continuation_token: None,
                })
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl Provider<authority::Identify> for TestStore {
            async fn execute(
                &self,
                input: Capability<authority::Identify>,
            ) -> Result<Capability<authority::Operator>, authority::AuthorityError> {
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
            ) -> Result<Vec<u8>, authority::AuthorityError> {
                use dialog_varsig::Signer;
                let payload = authority::Sign::of(&input).payload.clone();
                let sig = self
                    .signer
                    .sign(&payload)
                    .await
                    .map_err(|e| authority::AuthorityError::SigningFailed(e.to_string()))?;
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
        let key = "did:key:zOperator/did:key:zSubject/did:key:zIssuer.bafy123";
        let (issuer, cid) = parse_key_suffix(key).expect("Should parse valid key");
        assert_eq!(issuer, "did:key:zIssuer");
        assert_eq!(cid, "bafy123");
    }

    #[dialog_common::test]
    fn parse_key_suffix_did_with_colons() {
        let key = "did:key:z6MkOperator/did:key:z6MkSubject/did:key:z6MkfFJBxSBFgoAqTQLS7bTfP8MgyDypva5i6CL5PJN8RJZr.bafyreihxyz";
        let (issuer, cid) = parse_key_suffix(key).expect("Should parse DID with colons");
        assert_eq!(
            issuer,
            "did:key:z6MkfFJBxSBFgoAqTQLS7bTfP8MgyDypva5i6CL5PJN8RJZr"
        );
        assert_eq!(cid, "bafyreihxyz");
    }

    #[dialog_common::test]
    fn parse_key_suffix_invalid_no_dot() {
        assert!(parse_key_suffix("aud/sub/issuernodot").is_none());
    }

    #[dialog_common::test]
    fn parse_key_suffix_empty_issuer() {
        assert!(parse_key_suffix("aud/sub/.cid").is_none());
    }

    #[dialog_common::test]
    fn parse_key_suffix_empty_cid() {
        assert!(parse_key_suffix("aud/sub/issuer.").is_none());
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

        let expected_key = format!("{}/{}/{}.{}", operator_did, subject_did, subject_did, cid);

        // Retrieve via storage Get effect
        let get_cap = ucan_store(&subject_did).invoke(storage::Get::new(expected_key.as_bytes()));
        let result = <TestStore as Provider<storage::Get>>::execute(&env, get_cap).await;
        assert!(
            result.is_ok(),
            "Expected key '{}' not found in store",
            expected_key
        );
        assert!(
            result.unwrap().is_some(),
            "Expected key '{}' to have a value",
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

        // Build chain: [d1, d2] (subject-first, root-to-leaf)
        let chain = DelegationChain::try_from(vec![d1.clone(), d2.clone()]).expect("valid chain");

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

        let r1 = <TestStore as Provider<storage::Get>>::execute(
            &env,
            ucan_store(&subject_did).invoke(storage::Get::new(key1.as_bytes())),
        )
        .await;
        let r2 = <TestStore as Provider<storage::Get>>::execute(
            &env,
            ucan_store(&subject_did).invoke(storage::Get::new(key2.as_bytes())),
        )
        .await;
        assert!(
            r1.is_ok() && r1.unwrap().is_some(),
            "d1 should be stored at {}",
            key1
        );
        assert!(
            r2.is_ok() && r2.unwrap().is_some(),
            "d2 should be stored at {}",
            key2
        );
    }
}
