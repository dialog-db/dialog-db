//! UCAN delegation storage and chain discovery.
//!
//! Stores individual UCAN delegations in the credential store using storage
//! effects, and discovers delegation chains for authorization.
//!
//! # Storage Layout
//!
//! Delegations are stored in the `"credentials"` store with keys:
//!
//! - Subject-specific: `ucan/{audience}/{subject}/{issuer}.{cid}`
//! - Powerline (`sub: *`): `ucan/{audience}/_/{issuer}.{cid}`
//!
//! This layout enables efficient lookup: list by `ucan/{audience}/{subject}/`
//! to find all delegations granted to an operator for a given subject.
//! Entries where `issuer == subject` are direct grants (no further chain needed).

use dialog_capability::credential::{self, Authorization, Authorize, AuthorizeError};
use dialog_capability::{Capability, Constraint, Did, Effect, Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::storage;
use dialog_ucan::command::Command;
use dialog_ucan::subject::Subject;
use dialog_ucan::{Delegation, DelegationChain};
use dialog_varsig::eddsa::Ed25519Signature;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

use crate::site::UcanFormat;

const STORE_NAME: &str = "credentials";
const MAX_CHAIN_DEPTH: usize = 10;

fn delegation_prefix(audience: &Did, subject: &Did) -> String {
    format!("ucan/{}/{}/", audience, subject)
}

fn powerline_prefix(audience: &Did) -> String {
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
fn parse_key_suffix(key: &str) -> Option<(String, String)> {
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

/// Build a storage capability for the credentials store.
fn store_cap(subject: &Did) -> Capability<storage::Store> {
    dialog_capability::Subject::from(subject.clone())
        .attenuate(storage::Storage)
        .attenuate(storage::Store::new(STORE_NAME))
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
    Env: Provider<storage::Set> + ConditionalSync,
{
    for (cid, delegation) in chain.delegations() {
        let audience = delegation.audience();
        let key = match delegation.subject() {
            Subject::Specific(did) => delegation_key(audience, did, delegation.issuer(), cid),
            Subject::Any => powerline_key(audience, delegation.issuer(), cid),
        };

        let bytes = serde_ipld_dagcbor::to_vec(delegation.as_ref())
            .map_err(|e| credential::CredentialError::SigningFailed(e.to_string()))?;

        let set_cap = store_cap(subject).invoke(storage::Set::new(key.as_bytes(), bytes));

        env.execute(set_cap).await.map_err(|_| {
            credential::CredentialError::SigningFailed("Failed to store delegation".to_string())
        })?;
    }
    Ok(())
}

struct Candidate {
    issuer: Did,
    delegation: Delegation<Ed25519Signature>,
}

/// Find a delegation chain from `operator` back to `subject` for the given command.
///
/// Uses iterative BFS with `MAX_CHAIN_DEPTH` limit. Prioritizes direct grants
/// (where issuer == subject) before following intermediate delegations.
///
/// The `now` parameter is used to validate time bounds (expiration, not_before).
async fn find_chain<Env>(
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
            new_chain.push(candidate.delegation);

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
            new_chain.push(candidate.delegation);

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
    let keys = list_keys_with_prefix(env, subject, prefix).await?;
    let mut candidates = Vec::new();

    for key in keys {
        let (issuer_str, _) = match parse_key_suffix(&key) {
            Some(pair) => pair,
            None => continue,
        };

        let bytes = match get_value(env, subject, &key).await? {
            Some(b) => b,
            None => continue,
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

/// List all keys with a given prefix from the credentials store.
async fn list_keys_with_prefix<Env>(
    env: &Env,
    subject: &Did,
    prefix: &str,
) -> Result<Vec<String>, AuthorizeError>
where
    Env: Provider<storage::List> + ConditionalSync,
{
    let mut all_keys = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let list_cap = store_cap(subject).invoke(storage::List::new(continuation_token.clone()));

        let result = env
            .execute(list_cap)
            .await
            .map_err(|e| AuthorizeError::Configuration(format!("List failed: {:?}", e)))?;

        for key in &result.keys {
            if key.starts_with(prefix) {
                all_keys.push(key.clone());
            }
        }

        if result.is_truncated {
            continuation_token = result.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(all_keys)
}

/// Get a value by key from the credentials store.
async fn get_value<Env>(
    env: &Env,
    subject: &Did,
    key: &str,
) -> Result<Option<Vec<u8>>, AuthorizeError>
where
    Env: Provider<storage::Get> + ConditionalSync,
{
    let get_cap = store_cap(subject).invoke(storage::Get::new(key.as_bytes()));

    env.execute(get_cap)
        .await
        .map_err(|e| AuthorizeError::Configuration(format!("Get failed: {:?}", e)))
}

/// Authorize a capability using UCAN delegation chain discovery.
///
/// 1. Discovers the operator identity
/// 2. If operator == subject, self-authorizes (no chain needed)
/// 3. Otherwise, searches stored delegations for a valid chain
/// 4. Builds and signs a UCAN invocation
pub async fn authorize_ucan<C, Env>(
    env: &Env,
    capability: Capability<C>,
    endpoint: &str,
) -> Result<credential::Authorization<C, UcanFormat>, AuthorizeError>
where
    C: Constraint + Clone + ConditionalSend + 'static,
    Capability<C>: dialog_capability::Ability + Clone + ConditionalSend,
    Env: Provider<credential::Identify>
        + Provider<credential::Sign>
        + Provider<storage::List>
        + Provider<storage::Get>
        + ConditionalSync,
{
    let subject_did = capability.subject().clone();
    let ability = capability.ability();
    let params = dialog_capability::ucan::parameters(&*capability);

    // Discover operator identity
    let identify_cap = credential::Subject::from(subject_did.clone())
        .attenuate(credential::Credential)
        .attenuate(credential::Profile::default())
        .invoke(credential::Identify);
    let detail = <Env as Provider<credential::Identify>>::execute(env, identify_cap)
        .await
        .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
    let operator_did = detail.operator;

    let command = Command::parse(&ability)
        .map_err(|e| AuthorizeError::Configuration(format!("Invalid command: {}", e)))?;

    let now = dialog_ucan::time::Timestamp::now();

    let delegation_chain = if subject_did == operator_did {
        None
    } else {
        let chain = find_chain(
            env,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &params,
            &now,
        )
        .await?;
        if chain.is_none() {
            return Err(AuthorizeError::Denied(format!(
                "No delegation chain found for operator '{}' to act on subject '{}'",
                operator_did, subject_did
            )));
        }
        chain
    };

    let invocation = crate::credentials::authorize(
        env,
        delegation_chain,
        endpoint.to_string(),
        capability.clone(),
    )
    .await?;

    Ok(credential::Authorization::new(capability, invocation))
}

/// UCAN authorization session that discovers delegation chains from stored
/// delegations and builds signed UCAN invocations.
///
/// Wraps an inner environment that provides credential and storage effects,
/// plus the access service endpoint URL.
///
/// Implements `Provider<Authorize<Fx, UcanFormat>>` so it can be used as
/// the credential store backend in `Credentials<UcanSession<Env>>`.
pub struct UcanSession<Env> {
    env: Env,
    endpoint: String,
}

impl<Env> UcanSession<Env> {
    /// Create a new UCAN session with the given environment and endpoint.
    pub fn new(env: Env, endpoint: impl Into<String>) -> Self {
        Self {
            env,
            endpoint: endpoint.into(),
        }
    }

    /// Get the access service endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl<Env: Clone> Clone for UcanSession<Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}

impl<Env: std::fmt::Debug> std::fmt::Debug for UcanSession<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UcanSession")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env, Fx> Provider<Authorize<Fx, UcanFormat>> for UcanSession<Env>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Fx: Clone + ConditionalSend,
    Capability<Fx>: dialog_capability::Ability + Clone + ConditionalSend,
    Authorize<Fx, UcanFormat>: ConditionalSend + 'static,
    Env: Provider<credential::Identify>
        + Provider<credential::Sign>
        + Provider<storage::List>
        + Provider<storage::Get>
        + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Authorize<Fx, UcanFormat>>,
    ) -> Result<Authorization<Fx, UcanFormat>, AuthorizeError> {
        let authorize = input.into_inner().constraint;
        authorize_ucan(&self.env, authorize.capability, &self.endpoint).await
    }
}

// Delegate credential effects through to the inner env.

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> Provider<credential::Identify> for UcanSession<Env>
where
    Env: Provider<credential::Identify> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Identify>,
    ) -> Result<credential::Identity, credential::CredentialError> {
        self.env.execute(input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> Provider<credential::Sign> for UcanSession<Env>
where
    Env: Provider<credential::Sign> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Sign>,
    ) -> Result<Vec<u8>, credential::CredentialError> {
        self.env.execute(input).await
    }
}

// Delegate Import<DelegationChain> — stores delegations in the credentials store.

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> Provider<credential::Import<DelegationChain>> for UcanSession<Env>
where
    Env: Provider<credential::Identify> + Provider<storage::Set> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Import<DelegationChain>>,
    ) -> Result<(), credential::CredentialError> {
        let subject = input.subject().clone();
        let chain = &credential::Import::of(&input).material;
        import_delegation_chain(&self.env, &subject, chain).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::environment::Environment;
    use dialog_storage::provider::Volatile;
    use dialog_ucan::DelegationChain;
    use dialog_ucan::delegation::builder::DelegationBuilder;
    use dialog_ucan::delegation::policy::predicate::Predicate;
    use dialog_ucan::delegation::policy::selector::filter::Filter;
    use dialog_ucan::delegation::policy::selector::select::Select;
    use dialog_ucan::subject::Subject;
    use dialog_ucan::time::Timestamp;
    use dialog_varsig::Principal;
    use dialog_varsig::eddsa::Ed25519Signature;
    use ipld_core::ipld::Ipld;

    #[cfg(not(target_arch = "wasm32"))]
    use dialog_ucan::time::timestamp::{Duration, SystemTime};

    #[cfg(target_arch = "wasm32")]
    use web_time::{Duration, SystemTime};

    type TestEnv = Environment<Volatile, Ed25519Signer>;

    async fn signer_async(seed: u8) -> Ed25519Signer {
        Ed25519Signer::import(&[seed; 32])
            .await
            .expect("Failed to import signer")
    }

    fn test_env(signer: Ed25519Signer) -> TestEnv {
        Environment::with_credentials(Volatile::new(), signer)
    }

    fn now() -> Timestamp {
        Timestamp::now()
    }

    async fn build_delegation(
        issuer: &Ed25519Signer,
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
        issuer: &Ed25519Signer,
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
        issuer: &Ed25519Signer,
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
        issuer: &Ed25519Signer,
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
        issuer: &Ed25519Signer,
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

    async fn import_single<Env: Provider<storage::Set> + ConditionalSync>(
        env: &Env,
        subject: &Did,
        delegation: dialog_ucan::Delegation<Ed25519Signature>,
    ) {
        let chain = DelegationChain::new(delegation);
        import_delegation_chain(env, subject, &chain)
            .await
            .expect("Failed to import delegation");
    }

    // Key scheme tests

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

    // Import tests

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
        let result = get_value(&env, &subject_did, &expected_key)
            .await
            .expect("get should not error");
        assert!(
            result.is_some(),
            "Expected key '{}' not found in store",
            expected_key,
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
        assert!(
            get_value(&env, &subject_did, &key1)
                .await
                .unwrap()
                .is_some(),
            "First delegation should be stored"
        );
        assert!(
            get_value(&env, &subject_did, &key2)
                .await
                .unwrap()
                .is_some(),
            "Second delegation should be stored"
        );
    }

    #[dialog_common::test]
    async fn import_powerline_delegation_stores_under_underscore() {
        let issuer_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = issuer_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_powerline_delegation(&issuer_signer, &operator_signer, cmd("/")).await;
        let cid = delegation.to_cid();

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        let expected_key = format!("ucan/{}/_/{}.{}", operator_did, issuer_signer.did(), cid);
        let result = get_value(&env, &subject_did, &expected_key)
            .await
            .expect("get should not error");
        assert!(
            result.is_some(),
            "Expected powerline key '{}' not found",
            expected_key,
        );
    }

    // Direct grant chain discovery tests

    async fn setup_and_find(
        subject_signer: &Ed25519Signer,
        operator_signer: &Ed25519Signer,
        delegation_cmd: &str,
        find_cmd: &str,
    ) -> Option<DelegationChain> {
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let delegation = build_delegation(
            subject_signer,
            operator_signer,
            subject_signer,
            cmd(delegation_cmd),
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, delegation).await;

        let command = dialog_ucan::command::Command::parse(find_cmd).unwrap();
        find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error")
    }

    #[dialog_common::test]
    async fn direct_grant_root_command_finds_storage_get() {
        let subject = signer_async(1).await;
        let operator = signer_async(2).await;
        let result = setup_and_find(&subject, &operator, "/", "/storage/get").await;
        assert!(
            result.is_some(),
            "Root command '/' should match '/storage/get'"
        );
    }

    #[dialog_common::test]
    async fn direct_grant_storage_command_finds_storage_get() {
        let subject = signer_async(1).await;
        let operator = signer_async(2).await;
        let result = setup_and_find(&subject, &operator, "/storage", "/storage/get").await;
        assert!(result.is_some(), "'/storage' should match '/storage/get'");
    }

    #[dialog_common::test]
    async fn direct_grant_exact_command_finds_storage_get() {
        let subject = signer_async(1).await;
        let operator = signer_async(2).await;
        let result = setup_and_find(&subject, &operator, "/storage/get", "/storage/get").await;
        assert!(
            result.is_some(),
            "Exact '/storage/get' should match '/storage/get'"
        );
    }

    // Command attenuation rejection tests

    #[dialog_common::test]
    async fn archive_command_does_not_find_storage_get() {
        let subject = signer_async(1).await;
        let operator = signer_async(2).await;
        let result = setup_and_find(&subject, &operator, "/archive", "/storage/get").await;
        assert!(
            result.is_none(),
            "'/archive' should NOT match '/storage/get'"
        );
    }

    #[dialog_common::test]
    async fn storage_set_does_not_find_storage_get() {
        let subject = signer_async(1).await;
        let operator = signer_async(2).await;
        let result = setup_and_find(&subject, &operator, "/storage/set", "/storage/get").await;
        assert!(
            result.is_none(),
            "'/storage/set' should NOT match '/storage/get'"
        );
    }

    // Expiration tests

    #[dialog_common::test]
    async fn expired_delegation_not_found() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let past = Timestamp::new(SystemTime::now() - Duration::from_secs(3600)).unwrap();
        let delegation = build_delegation_with_expiration(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            past,
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(result.is_none(), "Expired delegation should not be found");
    }

    #[dialog_common::test]
    async fn not_yet_valid_delegation_not_found() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let future = Timestamp::new(SystemTime::now() + Duration::from_secs(3600)).unwrap();
        let delegation = build_delegation_with_not_before(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            future,
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_none(),
            "Not-yet-valid delegation should not be found"
        );
    }

    #[dialog_common::test]
    async fn valid_delegation_with_future_expiration_found() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let future = Timestamp::new(SystemTime::now() + Duration::from_secs(3600)).unwrap();
        let delegation = build_delegation_with_expiration(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            future,
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Delegation with future expiration should be found"
        );
    }

    // Multi-hop chain discovery tests

    #[dialog_common::test]
    async fn two_hop_root_and_storage_finds_storage_get() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> account (cmd: /)
        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;

        // account -> operator (cmd: /storage)
        let d2 = build_delegation(
            &account_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage"),
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Two-hop chain (/ then /storage) should find /storage/get"
        );
    }

    #[dialog_common::test]
    async fn two_hop_storage_and_root_finds_storage_get() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> account (cmd: /storage)
        let d1 = build_delegation(
            &subject_signer,
            &account_signer,
            &subject_signer,
            cmd("/storage"),
        )
        .await;

        // account -> operator (cmd: /)
        let d2 =
            build_delegation(&account_signer, &operator_signer, &subject_signer, cmd("/")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Two-hop chain (/storage then /) should find /storage/get"
        );
    }

    #[dialog_common::test]
    async fn two_hop_second_hop_wrong_command_not_found() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> account (cmd: /storage)
        let d1 = build_delegation(
            &subject_signer,
            &account_signer,
            &subject_signer,
            cmd("/storage"),
        )
        .await;

        // account -> operator (cmd: /archive) -- doesn't match /storage/get
        let d2 = build_delegation(
            &account_signer,
            &operator_signer,
            &subject_signer,
            cmd("/archive"),
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_none(),
            "Second hop /archive should NOT match /storage/get"
        );
    }

    #[dialog_common::test]
    async fn two_hop_first_hop_wrong_command_not_found() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> account (cmd: /archive) -- doesn't match /storage/get
        let d1 = build_delegation(
            &subject_signer,
            &account_signer,
            &subject_signer,
            cmd("/archive"),
        )
        .await;

        // account -> operator (cmd: /storage)
        let d2 = build_delegation(
            &account_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage"),
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_none(),
            "First hop /archive should block finding /storage/get"
        );
    }

    // Three-hop chain tests

    #[dialog_common::test]
    async fn three_hop_all_root_finds_archive_get() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let profile_signer = signer_async(3).await;
        let operator_signer = signer_async(4).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;
        let d2 =
            build_delegation(&account_signer, &profile_signer, &subject_signer, cmd("/")).await;
        let d3 =
            build_delegation(&profile_signer, &operator_signer, &subject_signer, cmd("/")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;
        import_single(&store, &subject_did, d3).await;

        let command = dialog_ucan::command::Command::parse("/archive/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Three-hop chain all with / should find /archive/get"
        );
    }

    #[dialog_common::test]
    async fn three_hop_narrowing_commands_finds_archive_get() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let profile_signer = signer_async(3).await;
        let operator_signer = signer_async(4).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;
        let d2 = build_delegation(
            &account_signer,
            &profile_signer,
            &subject_signer,
            cmd("/archive"),
        )
        .await;
        let d3 = build_delegation(
            &profile_signer,
            &operator_signer,
            &subject_signer,
            cmd("/archive/get"),
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;
        import_single(&store, &subject_did, d3).await;

        let command = dialog_ucan::command::Command::parse("/archive/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Three-hop narrowing chain should find /archive/get"
        );
    }

    #[dialog_common::test]
    async fn three_hop_reverse_narrowing_finds_archive_get() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let profile_signer = signer_async(3).await;
        let operator_signer = signer_async(4).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // All commands are prefixes of /archive/get
        let d1 = build_delegation(
            &subject_signer,
            &account_signer,
            &subject_signer,
            cmd("/archive/get"),
        )
        .await;
        let d2 = build_delegation(
            &account_signer,
            &profile_signer,
            &subject_signer,
            cmd("/archive"),
        )
        .await;
        let d3 =
            build_delegation(&profile_signer, &operator_signer, &subject_signer, cmd("/")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;
        import_single(&store, &subject_did, d3).await;

        let command = dialog_ucan::command::Command::parse("/archive/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Three-hop reverse narrowing should find /archive/get"
        );
    }

    // Powerline delegation tests

    #[dialog_common::test]
    async fn powerline_root_command_found_for_same_subject() {
        let account_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let account_did = account_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_powerline_delegation(&account_signer, &operator_signer, cmd("/")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &account_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &account_did,
            &operator_did,
            &account_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Powerline delegation with / should find /storage/get"
        );
    }

    #[dialog_common::test]
    async fn powerline_storage_command_finds_storage_get() {
        let account_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let account_did = account_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_powerline_delegation(&account_signer, &operator_signer, cmd("/storage")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &account_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &account_did,
            &operator_did,
            &account_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Powerline /storage should find /storage/get"
        );
    }

    #[dialog_common::test]
    async fn powerline_storage_command_does_not_find_archive_get() {
        let account_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let account_did = account_signer.did();
        let operator_did = operator_signer.did();

        let delegation =
            build_powerline_delegation(&account_signer, &operator_signer, cmd("/storage")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &account_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/archive/get").unwrap();
        let result = find_chain(
            &store,
            &account_did,
            &operator_did,
            &account_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_none(),
            "Powerline /storage should NOT find /archive/get"
        );
    }

    // Mixed subject-specific + powerline multi-hop tests

    #[dialog_common::test]
    async fn mixed_specific_then_powerline_finds_archive_get() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> account (specific, cmd: /)
        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;

        // account ->* operator (powerline, cmd: /)
        let d2 = build_powerline_delegation(&account_signer, &operator_signer, cmd("/")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;

        let command = dialog_ucan::command::Command::parse("/archive/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Specific + powerline chain should find /archive/get"
        );
    }

    #[dialog_common::test]
    async fn mixed_specific_archive_then_powerline_root_finds_archive_get() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> account (specific, cmd: /archive/get)
        let d1 = build_delegation(
            &subject_signer,
            &account_signer,
            &subject_signer,
            cmd("/archive/get"),
        )
        .await;

        // account ->* operator (powerline, cmd: /)
        let d2 = build_powerline_delegation(&account_signer, &operator_signer, cmd("/")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;

        let command = dialog_ucan::command::Command::parse("/archive/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Specific /archive/get + powerline / should find /archive/get"
        );
    }

    #[dialog_common::test]
    async fn mixed_specific_archive_then_powerline_storage_not_found() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        // subject -> account (specific, cmd: /archive/get)
        let d1 = build_delegation(
            &subject_signer,
            &account_signer,
            &subject_signer,
            cmd("/archive/get"),
        )
        .await;

        // account ->* operator (powerline, cmd: /storage) -- doesn't match /archive/get
        let d2 =
            build_powerline_delegation(&account_signer, &operator_signer, cmd("/storage")).await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, d1).await;
        import_single(&store, &subject_did, d2).await;

        let command = dialog_ucan::command::Command::parse("/archive/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_none(),
            "Powerline /storage should NOT match /archive/get"
        );
    }

    // Policy predicate tests

    #[dialog_common::test]
    async fn policy_equal_matching_found() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let selector = Select::new(vec![Filter::Field("store".to_string())]);
        let policy = vec![Predicate::Equal(
            selector,
            Ipld::String("index".to_string()),
        )];

        let delegation = build_delegation_with_policy(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            policy,
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Ipld::String("index".to_string()));

        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &args,
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Policy equal with matching args should be found"
        );
    }

    #[dialog_common::test]
    async fn policy_equal_non_matching_not_found() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let selector = Select::new(vec![Filter::Field("store".to_string())]);
        let policy = vec![Predicate::Equal(
            selector,
            Ipld::String("index".to_string()),
        )];

        let delegation = build_delegation_with_policy(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            policy,
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, delegation).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Ipld::String("blob".to_string()));

        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &args,
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_none(),
            "Policy equal with non-matching args should NOT be found"
        );
    }

    // Empty store test

    #[dialog_common::test]
    async fn empty_store_returns_none() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let store = test_env(operator_signer.clone());

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(result.is_none(), "Empty store should return None");
    }

    // Multiple candidates: one expired, one valid

    #[dialog_common::test]
    async fn multiple_candidates_picks_valid_one() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let past = Timestamp::new(SystemTime::now() - Duration::from_secs(3600)).unwrap();
        let expired = build_delegation_with_expiration(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            past,
        )
        .await;

        let future = Timestamp::new(SystemTime::now() + Duration::from_secs(3600)).unwrap();
        let valid = build_delegation_with_expiration(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            future,
        )
        .await;

        let store = test_env(operator_signer.clone());
        import_single(&store, &subject_did, expired).await;
        import_single(&store, &subject_did, valid).await;

        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &store,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Should find the valid delegation among expired ones"
        );
    }

    // authorize_ucan tests

    const TEST_ENDPOINT: &str = "https://access.example.com";

    fn storage_get_cap(subject_did: &Did) -> Capability<storage::Get> {
        dialog_capability::Subject::from(subject_did.clone())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Get::new(b"key1"))
    }

    #[dialog_common::test]
    async fn authorize_self_authorization_succeeds() {
        let operator_signer = signer_async(1).await;
        let subject_did = operator_signer.did();

        let env = test_env(operator_signer.clone());
        let cap = storage_get_cap(&subject_did);

        let result = authorize_ucan(&env, cap, TEST_ENDPOINT).await;
        assert!(
            result.is_ok(),
            "Self-authorization (operator == subject) should succeed, got: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn authorize_direct_grant_succeeds() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();

        let delegation =
            build_delegation(&subject_signer, &operator_signer, &subject_signer, cmd("/")).await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        let cap = storage_get_cap(&subject_did);
        let result = authorize_ucan(&env, cap, TEST_ENDPOINT).await;
        assert!(
            result.is_ok(),
            "Direct grant should authorize successfully, got: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn authorize_multi_hop_narrowing_succeeds() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let profile_signer = signer_async(3).await;
        let operator_signer = signer_async(4).await;
        let subject_did = subject_signer.did();

        // subject -> account (cmd: /)
        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;
        // account -> profile (cmd: /storage)
        let d2 = build_delegation(
            &account_signer,
            &profile_signer,
            &subject_signer,
            cmd("/storage"),
        )
        .await;
        // profile -> operator (cmd: /storage/get)
        let d3 = build_delegation(
            &profile_signer,
            &operator_signer,
            &subject_signer,
            cmd("/storage/get"),
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, d1).await;
        import_single(&env, &subject_did, d2).await;
        import_single(&env, &subject_did, d3).await;

        let cap = storage_get_cap(&subject_did);
        let result = authorize_ucan(&env, cap, TEST_ENDPOINT).await;
        assert!(
            result.is_ok(),
            "Multi-hop narrowing chain should authorize, got: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn authorize_ability_mismatch_denied() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();

        // Delegate only /archive
        let delegation = build_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/archive"),
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        // Try to authorize /storage/get
        let cap = storage_get_cap(&subject_did);
        match authorize_ucan(&env, cap, TEST_ENDPOINT).await {
            Err(AuthorizeError::Denied(_)) => {}
            Err(other) => panic!("Expected Denied, got: {:?}", other),
            Ok(_) => panic!("Archive delegation should not authorize storage/get"),
        }
    }

    #[dialog_common::test]
    async fn authorize_expired_delegation_denied() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();

        let past = Timestamp::new(SystemTime::now() - Duration::from_secs(3600)).unwrap();
        let delegation = build_delegation_with_expiration(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            past,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        let cap = storage_get_cap(&subject_did);
        match authorize_ucan(&env, cap, TEST_ENDPOINT).await {
            Err(AuthorizeError::Denied(_)) => {}
            Err(other) => panic!("Expected Denied, got: {:?}", other),
            Ok(_) => panic!("Expired delegation should be denied"),
        }
    }

    #[dialog_common::test]
    async fn authorize_powerline_delegation_succeeds() {
        let account_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = account_signer.did();

        // account ->* operator (powerline, cmd: /)
        let delegation =
            build_powerline_delegation(&account_signer, &operator_signer, cmd("/")).await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        let cap = storage_get_cap(&subject_did);
        let result = authorize_ucan(&env, cap, TEST_ENDPOINT).await;
        assert!(
            result.is_ok(),
            "Powerline delegation should authorize, got: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn authorize_mixed_specific_and_powerline_succeeds() {
        let subject_signer = signer_async(1).await;
        let account_signer = signer_async(2).await;
        let operator_signer = signer_async(3).await;
        let subject_did = subject_signer.did();

        // subject -> account (specific, cmd: /)
        let d1 =
            build_delegation(&subject_signer, &account_signer, &subject_signer, cmd("/")).await;

        // account ->* operator (powerline, cmd: /)
        let d2 = build_powerline_delegation(&account_signer, &operator_signer, cmd("/")).await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, d1).await;
        import_single(&env, &subject_did, d2).await;

        let cap = storage_get_cap(&subject_did);
        let result = authorize_ucan(&env, cap, TEST_ENDPOINT).await;
        assert!(
            result.is_ok(),
            "Mixed specific + powerline chain should authorize, got: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn authorize_no_delegation_denied() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();

        let env = test_env(operator_signer.clone());

        let cap = storage_get_cap(&subject_did);
        match authorize_ucan(&env, cap, TEST_ENDPOINT).await {
            Err(AuthorizeError::Denied(_)) => {}
            Err(other) => panic!("Expected Denied, got: {:?}", other),
            Ok(_) => panic!("No delegation at all should be denied"),
        }
    }

    #[dialog_common::test]
    async fn authorize_policy_matching_succeeds() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();

        let selector = Select::new(vec![Filter::Field("store".to_string())]);
        let policy = vec![Predicate::Equal(
            selector,
            Ipld::String("index".to_string()),
        )];

        let delegation = build_delegation_with_policy(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            policy,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        // storage_get_cap uses store "index", which matches the policy
        let cap = storage_get_cap(&subject_did);
        let result = authorize_ucan(&env, cap, TEST_ENDPOINT).await;
        assert!(
            result.is_ok(),
            "Policy with matching store should authorize, got: {:?}",
            result.err()
        );
    }

    #[dialog_common::test]
    async fn authorize_policy_mismatch_denied() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();

        // Policy requires store == "restricted"
        let selector = Select::new(vec![Filter::Field("store".to_string())]);
        let policy = vec![Predicate::Equal(
            selector,
            Ipld::String("restricted".to_string()),
        )];

        let delegation = build_delegation_with_policy(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            cmd("/"),
            policy,
        )
        .await;

        let env = test_env(operator_signer.clone());
        import_single(&env, &subject_did, delegation).await;

        // storage_get_cap uses store "index", which does NOT match "restricted"
        let cap = storage_get_cap(&subject_did);
        match authorize_ucan(&env, cap, TEST_ENDPOINT).await {
            Err(AuthorizeError::Denied(_)) => {}
            Err(other) => panic!("Expected Denied, got: {:?}", other),
            Ok(_) => panic!("Policy mismatch should be denied"),
        }
    }

    #[dialog_common::test]
    async fn garbage_records_do_not_poison_valid_delegation() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let env = test_env(operator_signer.clone());

        // Write garbage records at keys that look like valid delegation keys.
        // These have the correct prefix but contain invalid CBOR data.
        let garbage_keys = [
            format!(
                "ucan/{}/{}/{}.bafyfake1",
                operator_did, subject_did, subject_did
            ),
            format!(
                "ucan/{}/{}/{}.bafyfake2",
                operator_did, subject_did, subject_did
            ),
            format!("ucan/{}/_/{}.bafyfake3", operator_did, subject_did),
        ];

        for key in &garbage_keys {
            let set_cap = store_cap(&subject_did)
                .invoke(storage::Set::new(key.as_bytes(), b"not valid cbor garbage"));
            <TestEnv as Provider<storage::Set>>::execute(&env, set_cap)
                .await
                .expect("set should succeed");
        }

        // Now import a real valid delegation
        let delegation =
            build_delegation(&subject_signer, &operator_signer, &subject_signer, cmd("/")).await;
        import_single(&env, &subject_did, delegation).await;

        // find_chain should skip the garbage and find the valid one
        let command = dialog_ucan::command::Command::parse("/storage/get").unwrap();
        let result = find_chain(
            &env,
            &subject_did,
            &operator_did,
            &subject_did,
            &command,
            &BTreeMap::new(),
            &now(),
        )
        .await
        .expect("find_chain should not error");

        assert!(
            result.is_some(),
            "Garbage records should be skipped, valid delegation should still be found"
        );
    }

    #[dialog_common::test]
    async fn authorize_succeeds_despite_garbage_records() {
        let subject_signer = signer_async(1).await;
        let operator_signer = signer_async(2).await;
        let subject_did = subject_signer.did();
        let operator_did = operator_signer.did();

        let env = test_env(operator_signer.clone());

        // Write garbage at valid-looking keys
        let garbage_key = format!(
            "ucan/{}/{}/{}.bafygarbage",
            operator_did, subject_did, subject_did
        );
        let set_cap = store_cap(&subject_did)
            .invoke(storage::Set::new(garbage_key.as_bytes(), b"\xff\xfe\x00"));
        <TestEnv as Provider<storage::Set>>::execute(&env, set_cap)
            .await
            .expect("set should succeed");

        // Import a valid delegation
        let delegation =
            build_delegation(&subject_signer, &operator_signer, &subject_signer, cmd("/")).await;
        import_single(&env, &subject_did, delegation).await;

        // authorize_ucan should succeed
        let cap = storage_get_cap(&subject_did);
        let result = authorize_ucan(&env, cap, TEST_ENDPOINT).await;
        assert!(
            result.is_ok(),
            "Should authorize despite garbage records, got: {:?}",
            result.err()
        );
    }
}
