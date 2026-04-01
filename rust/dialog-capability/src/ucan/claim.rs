//! UCAN delegation chain discovery and invocation building.
//!
//! Searches stored delegations to find a valid chain from an operator
//! back to a subject for a given capability, then builds a signed
//! UCAN invocation proving the operator's authority.

use crate::access::AuthorizeError;
use crate::authority;
use crate::storage;
use crate::{Ability, Capability, Constraint, Did, Policy, Provider};
use dialog_common::ConditionalSync;
use dialog_ucan::command::Command;
use dialog_ucan::{Delegation, DelegationChain, InvocationBuilder, InvocationChain};
use dialog_varsig::eddsa::Ed25519Signature;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

use super::UcanInvocation;
use super::delegation::{delegation_prefix, parse_key_suffix, powerline_prefix};
use super::issuer::Issuer;
use super::parameters::{parameters, parameters_to_args};

const MAX_CHAIN_DEPTH: usize = 10;

struct Candidate {
    issuer: Did,
    delegation: Delegation<Ed25519Signature>,
}

/// Build a storage capability scoped to the "ucan" store for the given subject.
fn ucan_store(subject: &Did) -> Capability<storage::Store> {
    crate::Subject::from(subject.clone())
        .attenuate(storage::Storage)
        .attenuate(storage::Store::new("ucan"))
}

/// Discover a delegation chain for the given capability and build a signed
/// UCAN invocation proving the operator's authority.
///
/// Takes a pre-constructed [`Issuer`] that provides the operator identity
/// and signing capability.
///
/// 1. If operator == subject, builds a self-authorized invocation (no proofs)
/// 2. Otherwise, searches stored delegations for a valid chain
/// 3. Signs the invocation via the provided [`Issuer`]
/// 4. Returns `Err(Denied)` if no chain is found
pub async fn claim<C, Env>(
    env: &Env,
    issuer: Issuer<'_, Env>,
    capability: &Capability<C>,
) -> Result<UcanInvocation, AuthorizeError>
where
    C: Constraint,
    Capability<C>: Ability,
    Env: Provider<authority::Sign>
        + Provider<storage::List>
        + Provider<storage::Get>
        + ConditionalSync,
{
    let subject_did = capability.subject().clone();
    let ability = capability.ability();
    let params = parameters(capability);

    let profile_did = authority::Profile::of(issuer.capability()).profile.clone();
    let operator_did = authority::Operator::of(issuer.capability())
        .operator
        .clone();

    // Find delegation chain (or None if self-authorized)
    let delegation_chain = if subject_did == operator_did {
        None
    } else {
        let command = Command::parse(&ability)
            .map_err(|e| AuthorizeError::Configuration(format!("Invalid command: {}", e)))?;

        let now = dialog_ucan::time::Timestamp::now();

        let chain = find_chain(
            env,
            &profile_did,
            &operator_did,
            &subject_did,
            &command,
            &params,
            &now,
        )
        .await?;

        match chain {
            Some(c) => Some(c),
            None => {
                return Err(AuthorizeError::Denied(format!(
                    "No delegation chain found for operator '{}' to act on subject '{}'",
                    operator_did, subject_did
                )));
            }
        }
    };

    // Build signed UCAN invocation
    let (proofs, delegation) = match &delegation_chain {
        Some(chain) => {
            let chain_audience = chain.audience();
            if &operator_did != chain_audience {
                return Err(AuthorizeError::Configuration(format!(
                    "Authority '{}' does not match delegation chain audience '{}'",
                    operator_did, chain_audience
                )));
            }
            (chain.proof_cids().into(), Some(chain))
        }
        None => (vec![], None),
    };

    let command: Vec<String> = ability
        .trim_start_matches('/')
        .split('/')
        .map(|s| s.to_string())
        .collect();

    let args = parameters_to_args(params);

    let invocation = InvocationBuilder::new()
        .issuer(issuer)
        .audience(&subject_did)
        .subject(&subject_did)
        .command(command)
        .arguments(args)
        .proofs(proofs)
        .try_build()
        .await
        .map_err(|e| AuthorizeError::Denied(format!("{:?}", e)))?;

    let delegations = delegation
        .map(|c| c.delegations().clone())
        .unwrap_or_default();

    let chain = InvocationChain::new(invocation, delegations);

    Ok(UcanInvocation {
        chain: Box::new(chain),
        subject: subject_did,
        ability,
    })
}

/// Find a delegation chain from `operator` back to `subject` for the given command.
///
/// Uses iterative BFS with `MAX_CHAIN_DEPTH` limit. Prioritizes direct grants
/// (where issuer == subject) before following intermediate delegations.
pub async fn find_chain<Env>(
    env: &Env,
    subject: &Did,
    operator_did: &Did,
    subject_did: &Did,
    command: &Command,
    args: &BTreeMap<String, Ipld>,
    now: &dialog_ucan::time::Timestamp,
) -> Result<Option<DelegationChain>, AuthorizeError>
where
    Env: Provider<storage::List> + Provider<storage::Get> + ConditionalSync,
{
    let mut queue: Vec<(Did, Vec<Delegation<Ed25519Signature>>, usize)> =
        vec![(operator_did.clone(), vec![], 0)];

    while let Some((current_audience, chain_so_far, depth)) = queue.pop() {
        if depth >= MAX_CHAIN_DEPTH {
            continue;
        }

        // Subject-specific delegations
        let candidates = fetch_and_validate(
            env,
            subject,
            &delegation_prefix(&current_audience, subject_did),
            command,
            args,
            now,
        )
        .await?;

        let (direct, indirect): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|c| &c.issuer == subject_did);

        for candidate in direct.into_iter().chain(indirect) {
            let mut new_chain = chain_so_far.clone();
            // Prepend: each discovered delegation is closer to the subject
            // than what we already have, building subject-first order.
            new_chain.insert(0, candidate.delegation);

            if &candidate.issuer == subject_did {
                return build_chain(new_chain).map(Some);
            }

            queue.push((candidate.issuer, new_chain, depth + 1));
        }

        // Powerline delegations
        let powerline = fetch_and_validate(
            env,
            subject,
            &powerline_prefix(&current_audience),
            command,
            args,
            now,
        )
        .await?;

        for candidate in powerline {
            let mut new_chain = chain_so_far.clone();
            new_chain.insert(0, candidate.delegation);

            if &candidate.issuer == subject_did {
                return build_chain(new_chain).map(Some);
            }

            queue.push((candidate.issuer, new_chain, depth + 1));
        }
    }

    Ok(None)
}

/// Fetch delegations by key prefix and validate each against command/policy/time.
async fn fetch_and_validate<Env>(
    env: &Env,
    subject: &Did,
    prefix: &str,
    command: &Command,
    args: &BTreeMap<String, Ipld>,
    now: &dialog_ucan::time::Timestamp,
) -> Result<Vec<Candidate>, AuthorizeError>
where
    Env: Provider<storage::List> + Provider<storage::Get> + ConditionalSync,
{
    // List all keys in the ucan store
    let list_cap = ucan_store(subject).invoke(storage::List::new(None));

    let list_result: storage::ListResult = <Env as Provider<storage::List>>::execute(env, list_cap)
        .await
        .map_err(|e| AuthorizeError::Configuration(format!("List failed: {:?}", e)))?;

    // Filter keys by prefix
    let matching_keys: Vec<String> = list_result
        .keys
        .into_iter()
        .filter(|key| key.starts_with(prefix))
        .collect();

    let mut candidates = Vec::new();

    for key in matching_keys {
        let (issuer_str, _) = match parse_key_suffix(&key) {
            Some(pair) => pair,
            None => continue,
        };

        let get_cap = ucan_store(subject).invoke(storage::Get::new(key.as_bytes()));

        let bytes: Vec<u8> = match <Env as Provider<storage::Get>>::execute(env, get_cap).await {
            Ok(Some(b)) => b,
            Ok(None) => continue,
            Err(_) => continue,
        };

        let delegation: Delegation<Ed25519Signature> = match serde_ipld_dagcbor::from_slice(&bytes)
        {
            Ok(d) => d,
            Err(_) => continue,
        };

        // Validate command attenuation
        if !command.starts_with(delegation.command()) {
            continue;
        }

        // Validate policy predicates
        let args_ipld = Ipld::Map(args.clone());
        let policies_pass = delegation
            .policy()
            .iter()
            .all(|pred| pred.clone().run(&args_ipld).unwrap_or(false));
        if !policies_pass {
            continue;
        }

        // Validate time bounds
        if let Some(exp) = delegation.expiration()
            && *now > exp
        {
            continue;
        }
        if let Some(nbf) = delegation.not_before()
            && *now < nbf
        {
            continue;
        }

        // Verify issuer matches key
        let issuer = delegation.issuer().clone();
        if issuer.to_string() != issuer_str {
            continue;
        }

        candidates.push(Candidate { issuer, delegation });
    }

    Ok(candidates)
}

fn build_chain(
    delegations: Vec<Delegation<Ed25519Signature>>,
) -> Result<DelegationChain, AuthorizeError> {
    DelegationChain::try_from(delegations).map_err(|e| AuthorizeError::Configuration(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority;
    use dialog_ucan::DelegationChain;
    use dialog_ucan::command::Command;
    use dialog_ucan::delegation::builder::DelegationBuilder;
    use dialog_ucan::delegation::policy::predicate::Predicate;
    use dialog_ucan::delegation::policy::selector::filter::Filter;
    use dialog_ucan::delegation::policy::selector::select::Select;
    use dialog_ucan::subject::Subject;
    use dialog_ucan::time::Timestamp;
    use dialog_varsig::Principal;
    use dialog_varsig::eddsa::Ed25519Signature;
    use ipld_core::ipld::Ipld;
    use std::collections::BTreeMap;

    use dialog_ucan::time::{Duration, SystemTime};

    mod test_provider {
        use crate::authority;
        use crate::storage::{self, StorageError};
        use crate::{Capability, Policy, Provider, Subject};
        use async_trait::async_trait;
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
                let _ = input.store();

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
            ) -> Result<authority::Authority, authority::AuthorityError> {
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

    fn now() -> Timestamp {
        Timestamp::now()
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

    async fn build_powerline_delegation(
        issuer: &dialog_credentials::Ed25519Signer,
        audience: &impl Principal,
        cmd_segments: Vec<String>,
    ) -> dialog_ucan::Delegation<Ed25519Signature> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(Subject::Any)
            .command(cmd_segments)
            .try_build()
            .await
            .expect("Failed to build powerline delegation")
    }

    async fn build_delegation_with_expiration(
        issuer: &dialog_credentials::Ed25519Signer,
        audience: &impl Principal,
        subject: &impl Principal,
        cmd_segments: Vec<String>,
        expiration: Timestamp,
    ) -> dialog_ucan::Delegation<Ed25519Signature> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(Subject::Specific(subject.did()))
            .command(cmd_segments)
            .expiration(expiration)
            .try_build()
            .await
            .expect("Failed to build delegation with expiration")
    }

    async fn build_delegation_with_not_before(
        issuer: &dialog_credentials::Ed25519Signer,
        audience: &impl Principal,
        subject: &impl Principal,
        cmd_segments: Vec<String>,
        not_before: Timestamp,
    ) -> dialog_ucan::Delegation<Ed25519Signature> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(Subject::Specific(subject.did()))
            .command(cmd_segments)
            .not_before(not_before)
            .try_build()
            .await
            .expect("Failed to build delegation with not_before")
    }

    async fn build_delegation_with_policy(
        issuer: &dialog_credentials::Ed25519Signer,
        audience: &impl Principal,
        subject: &impl Principal,
        cmd_segments: Vec<String>,
        policy: Vec<Predicate>,
    ) -> dialog_ucan::Delegation<Ed25519Signature> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(Subject::Specific(subject.did()))
            .command(cmd_segments)
            .policy(policy)
            .try_build()
            .await
            .expect("Failed to build delegation with policy")
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
        super::super::delegation::import_delegation_chain(env, subject, &chain)
            .await
            .expect("Failed to import delegation");
    }

    #[dialog_common::test]
    async fn find_chain_direct_grant() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_delegation(&subject_signer, &operator_signer, &subject_signer, cmd("/")).await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        let command = Command::parse("/storage/get").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(chain.is_some(), "Should find direct grant chain");
        assert_eq!(chain.unwrap().delegations().len(), 1);
    }

    #[dialog_common::test]
    async fn find_chain_two_hop() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;
        let d2 =
            build_delegation(&account_signer, &operator_signer, &subject_signer, cmd("/")).await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, d1).await;
        import_single(&env, &operator_did, d2).await;

        let command = Command::parse("/storage/get").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(chain.is_some(), "Should find 2-hop chain");
        assert_eq!(chain.unwrap().delegations().len(), 2);
    }

    #[dialog_common::test]
    async fn find_chain_no_grant() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let env = test_env(operator_signer.clone());

        let command = Command::parse("/storage/get").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(chain.is_none(), "Should not find chain when none exists");
    }

    #[dialog_common::test]
    async fn find_chain_command_attenuation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let delegation = build_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage/get"),
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        // Should find for /storage/get
        let command_get = Command::parse("/storage/get").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command_get,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(chain.is_some(), "Should find chain for /storage/get");

        // Should not find for /storage/set
        let command_set = Command::parse("/storage/set").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command_set,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(chain.is_none(), "Should not find chain for /storage/set");
    }

    #[dialog_common::test]
    async fn find_chain_expired_delegation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let past = Timestamp::try_from(SystemTime::now() - Duration::from_secs(3600)).unwrap();
        let delegation = build_delegation_with_expiration(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            past,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        let command = Command::parse("/storage/get").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            chain.is_none(),
            "Should not find chain for expired delegation"
        );
    }

    #[dialog_common::test]
    async fn find_chain_not_before_future() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let future = Timestamp::try_from(SystemTime::now() + Duration::from_secs(3600)).unwrap();
        let delegation = build_delegation_with_not_before(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            future,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        let command = Command::parse("/storage/get").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            chain.is_none(),
            "Should not find chain for not-yet-valid delegation"
        );
    }

    #[dialog_common::test]
    async fn find_chain_powerline_delegation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_powerline_delegation(&subject_signer, &operator_signer, cmd("/")).await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        let command = Command::parse("/storage/get").unwrap();
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            chain.is_some(),
            "Should find chain via powerline delegation"
        );
    }

    #[dialog_common::test]
    async fn find_chain_with_policy() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let policy = vec![Predicate::Equal(
            Select::new(vec![Filter::Field("bucket".to_string())]),
            Ipld::String("my-bucket".to_string()),
        )];

        let delegation = build_delegation_with_policy(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage"),
            policy,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        // Matching policy
        let command = Command::parse("/storage/get").unwrap();
        let mut args = BTreeMap::new();
        args.insert("bucket".to_string(), Ipld::String("my-bucket".to_string()));
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &args,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(chain.is_some(), "Should find chain with matching policy");

        // Non-matching policy
        let mut bad_args = BTreeMap::new();
        bad_args.insert(
            "bucket".to_string(),
            Ipld::String("wrong-bucket".to_string()),
        );
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &command,
            &bad_args,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_none(),
            "Should not find chain with non-matching policy"
        );
    }

    #[dialog_common::test]
    async fn claim_self_authorized() {
        let signer = signer_async(1).await;
        let subject_did = signer.did();

        let env = test_env(signer);
        let authority = crate::Subject::from(subject_did.clone())
            .invoke(authority::Identify)
            .perform(&env)
            .await
            .unwrap();
        let issuer = Issuer::new(&env, authority);

        let cap = crate::Subject::from(subject_did).invoke(authority::Identify);

        let result = claim(&env, issuer, &cap).await;
        assert!(
            result.is_ok(),
            "Self-authorized claim should succeed: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn claim_with_delegation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_delegation(&subject_signer, &operator_signer, &subject_signer, cmd("/")).await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        let authority = crate::Subject::from(subject_did.clone())
            .invoke(authority::Identify)
            .perform(&env)
            .await
            .unwrap();
        let issuer = Issuer::new(&env, authority);

        let cap = crate::Subject::from(subject_did).invoke(authority::Identify);

        let result = claim(&env, issuer, &cap).await;
        assert!(
            result.is_ok(),
            "Delegated claim should succeed: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn claim_denied_without_delegation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();

        let env = test_env(operator_signer.clone());
        let authority = crate::Subject::from(subject_did.clone())
            .invoke(authority::Identify)
            .perform(&env)
            .await
            .unwrap();
        let issuer = Issuer::new(&env, authority);

        let cap = crate::Subject::from(subject_did).invoke(authority::Identify);

        let result = claim(&env, issuer, &cap).await;
        assert!(result.is_err(), "Should be denied without delegation");
        assert!(
            matches!(result, Err(AuthorizeError::Denied(_))),
            "Error should be Denied variant"
        );
    }

    #[dialog_common::test]
    async fn find_chain_with_parameters_to_policy() {
        use crate::storage::{self, Storage, Store};
        use crate::ucan::parameters::{parameters, parameters_to_policy};

        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // Build a capability with policy constraints
        let cap = crate::Subject::from(subject_did.clone())
            .attenuate(Storage)
            .attenuate(Store::new("index"));

        // Convert capability parameters to delegation policy
        let policy = parameters_to_policy(parameters(&cap));
        assert!(!policy.is_empty(), "should produce policy from capability");

        // Create delegation with the generated policy
        let delegation = build_delegation_with_policy(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage"),
            policy,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        // Claim with matching store should succeed
        let matching_cap = crate::Subject::from(subject_did.clone())
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(storage::Get::new(b"some-key"));

        let matching_args = crate::ucan::parameters::parameters(&matching_cap);
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &matching_args,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_some(),
            "should find chain when store matches delegated policy"
        );

        // Claim with different store should fail
        let wrong_cap = crate::Subject::from(subject_did.clone())
            .attenuate(Storage)
            .attenuate(Store::new("secret"))
            .invoke(storage::Get::new(b"some-key"));

        let wrong_args = crate::ucan::parameters::parameters(&wrong_cap);
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &wrong_args,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_none(),
            "should not find chain when store differs from delegated policy"
        );
    }

    #[dialog_common::test]
    async fn find_chain_rejects_command_escalation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // Delegate only /storage
        let delegation = build_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage"),
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        // Requesting / (root, broader than /storage) should fail
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/").unwrap(),
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_none(),
            "should not allow escalation from /storage to /"
        );
    }

    #[dialog_common::test]
    async fn find_chain_allows_narrower_command_under_delegation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // Delegate /storage (broad)
        let delegation = build_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage"),
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        // Requesting /storage/get (narrower) should succeed
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_some(),
            "/storage delegation should cover /storage/get"
        );

        // Requesting /storage/set should also succeed
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/set").unwrap(),
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_some(),
            "/storage delegation should cover /storage/set"
        );

        // But /memory/resolve should fail (different branch)
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/memory/resolve").unwrap(),
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_none(),
            "/storage delegation should not cover /memory"
        );
    }

    #[dialog_common::test]
    async fn find_chain_policy_multi_field_partial_match_fails() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // Delegate with store=data AND key=foo
        let policy = vec![
            Predicate::Equal(
                Select::new(vec![Filter::Field("store".to_string())]),
                Ipld::String("data".to_string()),
            ),
            Predicate::Equal(
                Select::new(vec![Filter::Field("key".to_string())]),
                Ipld::Bytes(b"foo".to_vec()),
            ),
        ];

        let delegation = build_delegation_with_policy(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage"),
            policy,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        // Matching both fields should succeed
        let mut matching = BTreeMap::new();
        matching.insert("store".to_string(), Ipld::String("data".to_string()));
        matching.insert("key".to_string(), Ipld::Bytes(b"foo".to_vec()));
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &matching,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(chain.is_some(), "both fields matching should succeed");

        // Matching store but wrong key should fail
        let mut wrong_key = BTreeMap::new();
        wrong_key.insert("store".to_string(), Ipld::String("data".to_string()));
        wrong_key.insert("key".to_string(), Ipld::Bytes(b"bar".to_vec()));
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &wrong_key,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_none(),
            "mismatched key should reject even if store matches"
        );

        // Matching store but missing key should fail (policy requires key)
        let mut store_only = BTreeMap::new();
        store_only.insert("store".to_string(), Ipld::String("data".to_string()));
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &store_only,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(chain.is_none(), "missing required key field should reject");
    }

    #[dialog_common::test]
    async fn find_chain_powerline_covers_any_command() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // Powerline delegation (Subject::Any, command /)
        let delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(&operator_signer)
            .subject(dialog_ucan::subject::Subject::Any)
            .command(vec![])
            .try_build()
            .await
            .expect("build powerline");

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, delegation).await;

        // Should match any command
        for path in ["/", "/storage", "/storage/get", "/memory/resolve"] {
            let chain = find_chain(
                &env,
                &operator_did,
                &operator_did,
                &subject_did,
                &Command::parse(path).unwrap(),
                &BTreeMap::new(),
                &now(),
            )
            .await
            .expect("find_chain should not error");
            assert!(chain.is_some(), "powerline should cover command '{}'", path);
        }
    }

    #[dialog_common::test]
    async fn find_chain_two_hop_with_policy_narrowing() {
        let subject_signer = signer_async(1).await;
        let intermediate_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> intermediate: grant /storage with store=data
        let d1 = build_delegation_with_policy(
            &subject_signer,
            &intermediate_signer,
            &subject_signer,
            cmd("/storage"),
            vec![Predicate::Equal(
                Select::new(vec![Filter::Field("store".to_string())]),
                Ipld::String("data".to_string()),
            )],
        )
        .await;

        // intermediate -> operator: grant /storage/get (narrower command, no extra policy)
        let d2 = build_delegation(
            &intermediate_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage/get"),
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &operator_did, d1).await;
        import_single(&env, &operator_did, d2).await;

        // Should find chain for /storage/get with store=data
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Ipld::String("data".to_string()));
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &args,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_some(),
            "should find two-hop chain matching narrowed command and policy"
        );

        // Should NOT find chain for /storage/get with store=secret
        // (first hop's policy requires store=data)
        let mut wrong_args = BTreeMap::new();
        wrong_args.insert("store".to_string(), Ipld::String("secret".to_string()));
        let chain = find_chain(
            &env,
            &operator_did,
            &operator_did,
            &subject_did,
            &Command::parse("/storage/get").unwrap(),
            &wrong_args,
            &now(),
        )
        .await
        .expect("find_chain should not error");
        assert!(
            chain.is_none(),
            "two-hop chain should reject when first hop's policy doesn't match"
        );
    }
}
