//! UCAN invocation chain management.
//!
//! This module provides [`InvocationChain`], which represents a complete UCAN
//! authorization bundle containing an invocation and its delegation proofs.
//!
//! # Container Format
//!
//! The UCAN container follows the [UCAN Container spec](https://github.com/ucan-wg/container):
//!
//! ```text
//! { "ctn-v1": [token_bytes_0, token_bytes_1, ..., token_bytes_n] }
//! ```
//!
//! Where tokens are DAG-CBOR serialized UCANs, ordered bytewise for determinism.
//! The first token is the invocation, followed by the delegation chain from
//! closest to invoker to root.

use super::check_failed_to_container_error;
use super::{Container, ContainerError};
use crate::{
    Delegation, Invocation,
    command::Command,
    invocation::{Invalid, Unavailable, VerifyError},
    promise::Promised,
    revocation::RevocationChecker,
    time::TimeRange,
    verification::{Verifiable, VerificationContext},
};
use dialog_varsig::AnySignature;
use dialog_varsig::Did;
use dialog_varsig::Resolver;
use dialog_varsig::Signature;
use ipld_core::cid::Cid;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

// On WASM, Ed25519 keys contain JsValue (via WebCrypto) which is !Send,
// so we use Local (LocalBoxFuture) instead of Sendable (BoxFuture).
#[cfg(target_arch = "wasm32")]
use crate::future::Local as Runtime;
#[cfg(not(target_arch = "wasm32"))]
use crate::future::Sendable as Runtime;

/// In-memory delegation store for verification.
pub type ProofStore<S> = Arc<Mutex<HashMap<Cid, Arc<Delegation<S>>>>>;

/// An invocation with its delegation chain, parsed from a UCAN container.
///
/// This represents a complete authorization bundle containing:
/// - The invocation (the signed command to execute)
/// - The delegation chain (proofs of authority from subject to invoker)
///
/// The invocation references its proofs by CID, and the delegation chain
/// provides those proofs for verification.
#[derive(Debug, Clone)]
pub struct InvocationChain<S: Signature> {
    /// The signed invocation containing the command and arguments.
    pub invocation: Invocation<S>,
    /// The delegation chain as a map keyed by CID for proof lookup.
    delegations: HashMap<Cid, Arc<Delegation<S>>>,
}

impl<S: Signature> InvocationChain<S> {
    /// Create a new invocation chain from an invocation and delegations.
    pub fn new(invocation: Invocation<S>, delegations: HashMap<Cid, Arc<Delegation<S>>>) -> Self {
        Self {
            invocation,
            delegations,
        }
    }

    /// Verify this invocation chain.
    ///
    /// Checks, in order: the proof chain's structure (principal alignment,
    /// subject consistency, command attenuation, policy predicates, and time
    /// bounds against `ctx`'s sampled instant), then every signature — the
    /// invocation's and each delegation link's — and every link's revocation
    /// status.
    ///
    /// Structure is checked first and costs no I/O, so a chain that does not
    /// hold up spends no DID resolutions and no crypto. Resolution then runs
    /// once per *distinct* issuer, and signatures and revocation lookups run
    /// concurrently, stopping at the first decisive refusal.
    ///
    /// Returns the chain's effective time window.
    ///
    /// # Errors
    ///
    /// Returns a [`ContainerError`] if the chain does not hold up, a
    /// signature does not verify, an issuer cannot be resolved, or a link
    /// has been revoked.
    pub async fn verify<C>(
        &self,
        ctx: &VerificationContext<'_, C>,
    ) -> Result<TimeRange, ContainerError>
    where
        C: Verifiable<
                Runtime,
                S,
                Proof = Arc<Delegation<S>>,
                Delegations = ProofStore<S>,
                Revocations: RevocationChecker,
            >,
        <C::Resolver as Resolver<S>>::Error: std::error::Error + Clone + 'static,
    {
        self.invocation
            .check::<Runtime, _, _, _>(ctx)
            .await
            .map_err(|err| match err {
                // Their material did not hold up: the decision crosses the
                // boundary as itself.
                VerifyError::Invalid(invalid) => match invalid {
                    Invalid::InvocationSignature(sig_err) => {
                        ContainerError::Invocation(format!("invalid signature: {sig_err}"))
                    }
                    Invalid::DelegationSignature { issuer, source } => {
                        ContainerError::InvalidDelegationSignature {
                            issuer,
                            detail: source.to_string(),
                        }
                    }
                    Invalid::MissingProof(get_err) => {
                        ContainerError::Invocation(format!("proof not found: {get_err}"))
                    }
                    Invalid::Chain(check_err) => check_failed_to_container_error(check_err),
                    Invalid::Revoked { cid, found } => ContainerError::Revoked {
                        cid,
                        revoker: found.principal,
                    },
                },
                // Our own setup being unable to check says nothing about
                // their request, so it must not read as a denial.
                VerifyError::Unavailable(unavailable) => match unavailable {
                    Unavailable::DidResolution { did, detail } => ContainerError::Configuration(
                        format!("could not resolve '{did}': {detail}"),
                    ),
                    Unavailable::RevocationLookup { cid, source } => ContainerError::Configuration(
                        format!("could not determine revocation status of '{cid}': {source}"),
                    ),
                },
            })
    }

    /// Look up a delegation the container carries as a block.
    ///
    /// The container holds every token that travelled with the invocation,
    /// which is not only its `prf` chain: a `/ucan/revoke` also carries the
    /// delegations its `rev` and `pth` arguments name.
    #[must_use]
    pub fn delegation(&self, cid: &Cid) -> Option<&Arc<Delegation<S>>> {
        self.delegations.get(cid)
    }

    /// The proof store this chain's delegations live in.
    ///
    /// Exposed so an environment implementing [`Verifiable`] can hand the
    /// chain's own proofs to the verifier.
    #[must_use]
    pub fn proof_store(&self) -> ProofStore<S> {
        Arc::new(Mutex::new(self.delegations.clone()))
    }

    /// Get the command from the invocation.
    pub fn command(&self) -> &Command {
        self.invocation.command()
    }

    /// Get the arguments from the invocation.
    pub fn arguments(&self) -> &BTreeMap<String, Promised> {
        self.invocation.arguments()
    }

    /// Get the subject (root authority) of the invocation.
    pub fn subject(&self) -> &Did {
        self.invocation.subject()
    }

    /// Get the issuer of the invocation.
    pub fn issuer(&self) -> &Did {
        self.invocation.issuer()
    }

    /// Get the proof CIDs referenced by the invocation.
    pub fn proofs(&self) -> &Vec<Cid> {
        self.invocation.proofs()
    }
}

impl<S: Signature + Serialize> InvocationChain<S>
where
    Delegation<S>: Serialize,
{
    /// Serialize to DAG-CBOR bytes (UCAN container format).
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContainerError> {
        Container::from(self).into_bytes()
    }
}

impl TryFrom<&[u8]> for InvocationChain<AnySignature> {
    type Error = ContainerError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let container = Container::from_bytes(bytes)?;
        InvocationChain::try_from(container)
    }
}

impl TryFrom<Container> for InvocationChain<AnySignature> {
    type Error = ContainerError;

    /// Convert a container to an invocation chain.
    ///
    /// The first token must be the invocation, followed by the delegation chain.
    fn try_from(container: Container) -> Result<Self, Self::Error> {
        let token_bytes = container.into_tokens();

        if token_bytes.is_empty() {
            return Err(ContainerError::Invocation(
                "container must contain at least an invocation".to_string(),
            ));
        }

        // First token is the invocation
        let invocation: Invocation<AnySignature> = serde_ipld_dagcbor::from_slice(&token_bytes[0])
            .map_err(|e| {
                ContainerError::Invocation(format!("failed to decode invocation: {}", e))
            })?;

        // Remaining tokens are delegations - build a map keyed by CID
        let mut delegations: HashMap<Cid, Arc<Delegation<AnySignature>>> =
            HashMap::with_capacity(token_bytes.len() - 1);
        for (i, bytes) in token_bytes.iter().skip(1).enumerate() {
            let delegation: Delegation<AnySignature> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    ContainerError::Invocation(format!("failed to decode delegation {}: {}", i, e))
                })?;
            let cid = delegation.to_cid();
            delegations.insert(cid, Arc::new(delegation));
        }

        Ok(InvocationChain {
            invocation,
            delegations,
        })
    }
}

impl<S: Signature + Serialize> From<&InvocationChain<S>> for Container
where
    Delegation<S>: Serialize,
{
    fn from(chain: &InvocationChain<S>) -> Self {
        let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(1 + chain.delegations.len());

        // First token is the invocation
        if let Ok(invocation_bytes) = serde_ipld_dagcbor::to_vec(&chain.invocation) {
            tokens.push(invocation_bytes);
        }

        // Add delegations in the order they appear in the invocation's proofs
        for cid in chain.invocation.proofs() {
            if let Some(delegation) = chain.delegations.get(cid) {
                tokens.push(delegation.encoded().to_vec());
            }
        }

        Container::new(tokens)
    }
}

impl<Sig: Signature + Serialize> Serialize for InvocationChain<Sig>
where
    Delegation<Sig>: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for InvocationChain<AnySignature> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Use serde_bytes::ByteBuf to properly deserialize CBOR byte strings
        let bytes: serde_bytes::ByteBuf = serde_bytes::ByteBuf::deserialize(deserializer)?;
        InvocationChain::try_from(bytes.as_slice()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::command::Command;
    use crate::helpers::{create_delegation, generate_signer};
    use crate::revocation::UnverifiedRevocations;
    use crate::revocation::{RevocationMatch, RevocationSelector};
    use crate::subject::Subject;
    use crate::verification::Environment;
    use crate::{DelegationBuilder, InvocationBuilder};
    use dialog_credentials::DidKeyResolver;
    use dialog_varsig::Principal;

    /// The environment these tests verify against: the chain's own proofs,
    /// `did:key` resolution, and no revocation lookups.
    type TestEnv = Environment<
        ProofStore<AnySignature>,
        DidKeyResolver,
        UnverifiedRevocations,
        Arc<Delegation<AnySignature>>,
    >;

    fn test_environment(chain: &InvocationChain<AnySignature>) -> TestEnv {
        Environment::new(chain.proof_store(), DidKeyResolver, UnverifiedRevocations)
    }

    /// A context over `env`, judged against the system clock.
    fn test_context(env: &TestEnv) -> VerificationContext<'_, TestEnv> {
        VerificationContext::new(env)
    }

    /// Create a test invocation chain with a valid delegation.
    pub(crate) async fn create_test_invocation_chain() -> (InvocationChain<AnySignature>, Did) {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        // Create delegation: subject -> operator
        let delegation = create_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            &["storage", "get"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation from operator
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        (InvocationChain::new(invocation, delegations), subject_did)
    }

    #[dialog_common::test]
    async fn it_creates_invocation_chain() {
        let (chain, subject_did) = create_test_invocation_chain().await;

        assert_eq!(chain.subject(), &subject_did);
        assert_eq!(chain.proofs().len(), 1);
        assert_eq!(chain.command().to_string(), "/storage/get");
    }

    #[dialog_common::test]
    async fn it_serializes_and_deserializes_roundtrip() {
        let (chain, subject_did) = create_test_invocation_chain().await;

        // Serialize to bytes
        let bytes = chain.to_bytes().expect("Failed to serialize");

        // Deserialize back
        let restored = InvocationChain::try_from(bytes.as_slice()).expect("Failed to deserialize");

        // Verify the chains match
        assert_eq!(restored.subject(), &subject_did);
        assert_eq!(restored.proofs().len(), chain.proofs().len());
        assert_eq!(restored.command().to_string(), chain.command().to_string());
    }

    #[dialog_common::test]
    async fn it_serde_roundtrips_via_dagcbor() {
        let (chain, subject_did) = create_test_invocation_chain().await;

        // Serialize via serde to DAG-CBOR (this uses serialize_bytes internally)
        let cbor_bytes = serde_ipld_dagcbor::to_vec(&chain).expect("Failed to serialize");

        // Deserialize via serde from DAG-CBOR (this uses dialog_common::Bytes)
        let restored: InvocationChain<AnySignature> =
            serde_ipld_dagcbor::from_slice(&cbor_bytes).expect("Failed to deserialize");

        // Verify the chains match
        assert_eq!(restored.subject(), &subject_did);
        assert_eq!(restored.proofs().len(), chain.proofs().len());
        assert_eq!(restored.command().to_string(), chain.command().to_string());
    }

    #[dialog_common::test]
    async fn it_verifies_valid_chain() {
        let (chain, _) = create_test_invocation_chain().await;

        // Should verify successfully
        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed: {:?}",
            result
        );
    }

    /// End-to-end acceptance test for algorithm-agnostic signing: an ed25519
    /// principal delegates to a p256 principal, the p256 principal invokes with
    /// that delegation as its proof, and the full chain verifies. The delegation
    /// link is signed and verified under ed25519, the invocation link under
    /// p256, each algorithm taken from its own varsig header.
    #[dialog_common::test]
    async fn it_verifies_mixed_algorithm_chain() {
        use dialog_credentials::{Ed25519Signer, Es256Signer, Signer};

        let issuer = Signer::from(Ed25519Signer::generate().await.unwrap());
        let audience = Signer::from(Es256Signer::generate().await.unwrap());
        let subject_did = issuer.did();
        let audience_did = audience.did();

        // ed25519 issuer delegates to the p256 audience.
        let delegation = DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(&audience)
            .subject(Subject::Specific(subject_did.clone()))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .expect("Failed to build delegation");
        let delegation_cid = delegation.to_cid();

        // The p256 audience invokes, referencing the delegation as its proof.
        let invocation = InvocationBuilder::new()
            .issuer(audience.clone())
            .audience(&audience_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));
        let chain = InvocationChain::new(invocation, delegations);

        // The full chain verifies: the delegation link under ed25519, the
        // invocation link under p256, one validation pass over mixed algorithms.
        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_ok(),
            "Expected mixed-algorithm chain to verify: {:?}",
            result
        );
    }

    /// Negative counterpart: a signature whose varsig header names one algorithm
    /// but is checked against the other algorithm's key must be refused. A p256
    /// signature verified by an ed25519 verifier is rejected on the tag
    /// mismatch, so a forged or corrupted link cannot slip through.
    #[dialog_common::test]
    async fn it_refuses_mismatched_algorithm_signature() {
        use dialog_credentials::{Ed25519Signer, Es256Signer, Signer};
        use dialog_varsig::{Signer as VarsigSigner, Verifier as VarsigVerifier};

        let es_signer = Signer::from(Es256Signer::generate().await.unwrap());
        let ed_signer = Signer::from(Ed25519Signer::generate().await.unwrap());

        let msg = b"mixed";
        let sig = VarsigSigner::sign(&es_signer, msg).await.unwrap();

        // The matching p256 verifier accepts it.
        assert!(es_signer.verifier().verify(msg, &sig).await.is_ok());

        // An ed25519 verifier refuses the p256-tagged signature.
        assert!(
            ed_signer.verifier().verify(msg, &sig).await.is_err(),
            "Expected a p256 signature to be refused by an ed25519 verifier"
        );
    }

    #[dialog_common::test]
    async fn it_fails_verification_when_proof_is_missing() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        // Create delegation but don't include it in the chain
        let delegation = create_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            &["storage"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation referencing the delegation
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        // Create chain WITHOUT the delegation
        let chain = InvocationChain::new(invocation, HashMap::new());

        // Should fail verification due to missing proof
        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("proof not found"));
    }

    #[dialog_common::test]
    async fn it_fails_verification_when_issuer_is_wrong() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;
        let wrong_operator_signer = generate_signer().await;

        // Create delegation to operator
        let delegation = create_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            &["storage"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation from WRONG operator (not the delegation audience)
        let invocation = InvocationBuilder::new()
            .issuer(wrong_operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        // Should fail verification due to issuer mismatch
        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(result.is_err());
    }

    /// Reports every query as unanswerable.
    #[derive(Debug, thiserror::Error)]
    #[error("revocation service unreachable")]
    struct Unreachable;

    #[derive(Debug, Clone, Copy)]
    struct OfflineRevocations;

    impl RevocationChecker for OfflineRevocations {
        type Error = Unreachable;

        async fn query(
            &self,
            _selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            Err(Unreachable)
        }
    }

    /// Reports one specific delegation as revoked, by a named principal.
    #[derive(Debug, Clone)]
    struct RevokedLink {
        cid: Cid,
        principal: Did,
    }

    impl RevocationChecker for RevokedLink {
        type Error = Unreachable;

        async fn query(
            &self,
            selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            if selector.delegation == self.cid && selector.by.contains(&self.principal) {
                return Ok(Some(RevocationMatch {
                    revocation: selector.delegation,
                    principal: self.principal.clone(),
                }));
            }
            Ok(None)
        }
    }

    // The validity invariant, at its sharpest. A caller may legitimately
    // choose to fail open when the revocation service is unreachable, so
    // "revocation unavailable" must never be what a caller sees for a chain
    // that does not verify — otherwise that choice would also accept forged
    // chains. Tolerance relaxes revocation and nothing else.
    #[dialog_common::test]
    async fn it_reports_a_forged_signature_even_when_revocation_is_unreachable() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let attacker_signer = generate_signer().await;

        // Claims the subject as issuer so principal alignment passes and
        // only the signature is wrong. Structured any other way, the chain
        // would fail alignment first and this would pass with signature
        // checking disabled entirely.
        let forged = Delegation::forge(
            subject_did.clone(),
            attacker_signer.did(),
            Subject::Specific(subject_did.clone()),
            Command::new(vec!["storage".to_string(), "get".to_string()]),
            &attacker_signer,
        )
        .await
        .expect("forged delegation");
        let forged_cid = forged.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(attacker_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![forged_cid])
            .try_build()
            .await
            .expect("invocation");

        let mut delegations = HashMap::new();
        delegations.insert(forged_cid, Arc::new(forged));
        let chain = InvocationChain::new(invocation, delegations);

        // The most permissive revocation policy available.
        let environment = Environment::new(
            chain.proof_store(),
            DidKeyResolver,
            OfflineRevocations.tolerate_unavailable(),
        );
        let ctx = VerificationContext::new(&environment);
        let error = chain
            .verify(&ctx)
            .await
            .expect_err("a forged signature must refuse the chain");

        assert!(
            matches!(error, ContainerError::InvalidDelegationSignature { .. }),
            "expected the signature refusal rather than an unavailable \
             revocation service, got: {error:?}"
        );
    }

    // A refusal names which link was revoked and who withdrew it, so an
    // operator can tell which authority failed rather than only that
    // something did.
    #[dialog_common::test]
    async fn it_names_the_revoked_link_and_its_revoker() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        let delegation = create_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            &["storage", "get"],
        )
        .await
        .expect("delegation");
        let cid = delegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await
            .expect("invocation");

        let mut delegations = HashMap::new();
        delegations.insert(cid, Arc::new(delegation));
        let chain = InvocationChain::new(invocation, delegations);

        let environment = Environment::new(
            chain.proof_store(),
            DidKeyResolver,
            RevokedLink {
                cid,
                principal: subject_did.clone(),
            },
        );
        let ctx = VerificationContext::new(&environment);
        let error = chain
            .verify(&ctx)
            .await
            .expect_err("a revoked link must refuse the chain");

        let rendered = error.to_string();
        assert!(
            rendered.contains(&cid.to_string()),
            "the refusal must name the revoked link: {rendered}"
        );
    }

    // Tolerance is about not knowing, never knowing-and-ignoring: a
    // revocation the checker did find still refuses the chain.
    #[dialog_common::test]
    async fn it_refuses_a_confirmed_revocation_even_when_tolerating() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        let delegation = create_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
            &["storage", "get"],
        )
        .await
        .expect("delegation");
        let cid = delegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await
            .expect("invocation");

        let mut delegations = HashMap::new();
        delegations.insert(cid, Arc::new(delegation));
        let chain = InvocationChain::new(invocation, delegations);

        let environment = Environment::new(
            chain.proof_store(),
            DidKeyResolver,
            RevokedLink {
                cid,
                principal: subject_did,
            }
            .tolerate_unavailable(),
        );
        let ctx = VerificationContext::new(&environment);
        assert!(
            chain.verify(&ctx).await.is_err(),
            "tolerating an unavailable service must not tolerate a \
             confirmed revocation"
        );
    }

    // Pins the delegation-link signature forgery. A structurally-valid
    // delegation whose `iss` claims the subject but whose signature is not
    // the subject's must be rejected: the attacker cannot sign as the
    // subject, so the chain must not authorize them. Before delegation
    // signatures were verified this passed verification, since only the
    // invocation's own signature was checked.
    #[dialog_common::test]
    async fn it_fails_verification_when_delegation_signature_is_forged() {
        use crate::command::Command;
        use crate::subject::Subject;

        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let attacker_signer = generate_signer().await;
        let attacker_did = attacker_signer.did();

        // Forge a delegation: iss claims the subject, aud is the attacker,
        // sub is the subject's resource, but it is signed by the attacker,
        // not the subject. The structure is valid (root issuer == subject),
        // only the signature does not verify against the subject's key.
        let forged = Delegation::forge(
            subject_did.clone(),
            attacker_did.clone(),
            Subject::Specific(subject_did.clone()),
            Command::new(vec!["storage".to_string(), "get".to_string()]),
            &attacker_signer,
        )
        .await
        .expect("Failed to forge delegation");

        let forged_cid = forged.to_cid();

        // Attacker builds a valid invocation, correctly signed by themselves,
        // referencing the forged delegation as its proof.
        let invocation = InvocationBuilder::new()
            .issuer(attacker_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![forged_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(forged_cid, Arc::new(forged));

        let chain = InvocationChain::new(invocation, delegations);

        // Must be rejected: the delegation link's signature is not the
        // subject's, so the chain grants the attacker nothing.
        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_err(),
            "forged delegation-link signature must be rejected, got: {result:?}"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("signature"),
            "rejection should name a signature failure, got: {err}"
        );
    }

    #[dialog_common::test]
    fn it_fails_on_empty_container() {
        let container = Container::new(vec![]);
        let result = InvocationChain::try_from(container);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least an invocation")
        );
    }

    #[dialog_common::test]
    fn it_fails_on_invalid_bytes() {
        let container = Container::new(vec![vec![1, 2, 3, 4]]); // Invalid CBOR
        let result = InvocationChain::try_from(container);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("failed to decode invocation")
        );
    }

    #[dialog_common::test]
    async fn it_verifies_chain_with_powerline_delegation_in_middle() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let device1_signer = generate_signer().await;
        let device2_signer = generate_signer().await;

        // Root delegation: subject -> device1 (with specific subject)
        let root_delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(&device1_signer)
            .subject(Subject::Specific(subject_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .expect("Failed to build root delegation");

        let root_cid = root_delegation.to_cid();

        // Powerline delegation: device1 -> device2 (with sub: null)
        let powerline_delegation = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(&device2_signer)
            .subject(Subject::Any)
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_delegation.to_cid();

        // Invocation from device2
        let invocation = InvocationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![root_cid, powerline_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(root_cid, Arc::new(root_delegation));
        delegations.insert(powerline_cid, Arc::new(powerline_delegation));

        let chain = InvocationChain::new(invocation, delegations);

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed with powerline in middle: {:?}",
            result
        );
    }

    #[dialog_common::test]
    async fn it_fails_verification_with_powerline_at_root_wrong_subject() {
        let device1_signer = generate_signer().await;
        let device2_signer = generate_signer().await;
        let some_other_subject = generate_signer().await.did();

        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(&device2_signer)
            .subject(Subject::Any)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(&some_other_subject)
            .subject(&some_other_subject)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));

        let chain = InvocationChain::new(invocation, delegations);

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_err(),
            "Expected verification to fail when invocation subject doesn't match powerline root issuer"
        );
    }

    #[dialog_common::test]
    async fn it_verifies_chain_with_powerline_at_root_matching_issuer() {
        let device1_signer = generate_signer().await;
        let device1_did = device1_signer.did();
        let device2_signer = generate_signer().await;

        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(&device2_signer)
            .subject(Subject::Any)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(&device1_did)
            .subject(&device1_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));

        let chain = InvocationChain::new(invocation, delegations);

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed when invocation subject matches powerline root issuer: {:?}",
            result
        );
    }

    #[dialog_common::test]
    async fn it_fails_when_redelegation_after_powerline_root_uses_wrong_subject() {
        let device1_signer = generate_signer().await;
        let device2_signer = generate_signer().await;
        let device3_signer = generate_signer().await;
        let some_other_resource = generate_signer().await.did();

        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(&device2_signer)
            .subject(Subject::Any)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        let bad_redelegation = DelegationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(&device3_signer)
            .subject(Subject::Specific(some_other_resource.clone()))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .expect("Failed to build redelegation");

        let bad_cid = bad_redelegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(device3_signer.clone())
            .audience(&some_other_resource)
            .subject(&some_other_resource)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid, bad_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));
        delegations.insert(bad_cid, Arc::new(bad_redelegation));

        let chain = InvocationChain::new(invocation, delegations);

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_err(),
            "Expected verification to fail when redelegation after powerline root uses wrong subject"
        );
    }

    #[dialog_common::test]
    async fn it_verifies_when_redelegation_after_powerline_root_uses_correct_subject() {
        let device1_signer = generate_signer().await;
        let device1_did = device1_signer.did();
        let device2_signer = generate_signer().await;
        let device3_signer = generate_signer().await;

        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(&device2_signer)
            .subject(Subject::Any)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        let valid_redelegation = DelegationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(&device3_signer)
            .subject(Subject::Specific(device1_did.clone()))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .expect("Failed to build redelegation");

        let valid_cid = valid_redelegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(device3_signer.clone())
            .audience(&device1_did)
            .subject(&device1_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid, valid_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));
        delegations.insert(valid_cid, Arc::new(valid_redelegation));

        let chain = InvocationChain::new(invocation, delegations);

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed when redelegation after powerline root uses correct subject: {:?}",
            result
        );
    }

    /// Test invocation chain with archive/put command roundtrips correctly.
    #[dialog_common::test]
    async fn it_roundtrips_archive_put_invocation() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        let delegation =
            create_delegation(&subject_signer, &operator_signer, &subject_signer, &["use"])
                .await
                .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec![
                "use".to_string(),
                "put".to_string(),
                "archive".to_string(),
                "block".to_string(),
            ])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        assert_eq!(chain.command().to_string(), "/use/put/archive/block");

        let bytes = chain.to_bytes().expect("Failed to serialize");
        let restored = InvocationChain::try_from(bytes.as_slice()).expect("Failed to deserialize");

        assert_eq!(restored.command().to_string(), "/use/put/archive/block");
        assert_eq!(restored.subject(), &subject_did);
    }

    /// Test invocation chain with serde DAG-CBOR roundtrip for archive/put.
    #[dialog_common::test]
    async fn it_serde_roundtrips_archive_put_invocation() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        let delegation =
            create_delegation(&subject_signer, &operator_signer, &subject_signer, &["use"])
                .await
                .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec![
                "use".to_string(),
                "put".to_string(),
                "archive".to_string(),
                "block".to_string(),
            ])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        let cbor_bytes = serde_ipld_dagcbor::to_vec(&chain).expect("Failed to serialize");

        let restored: InvocationChain<AnySignature> =
            serde_ipld_dagcbor::from_slice(&cbor_bytes).expect("Failed to deserialize");

        assert_eq!(restored.command().to_string(), "/use/put/archive/block");
        assert_eq!(restored.subject(), &subject_did);
    }

    /// Test that a delegation granting /archive can authorize an /archive/put invocation.
    #[dialog_common::test]
    async fn it_verifies_archive_delegation_authorizes_archive_put_invocation() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        let delegation =
            create_delegation(&subject_signer, &operator_signer, &subject_signer, &["use"])
                .await
                .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec![
                "use".to_string(),
                "put".to_string(),
                "archive".to_string(),
                "block".to_string(),
            ])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_ok(),
            "Expected /archive delegation to authorize /archive/put invocation: {:?}",
            result
        );
    }

    /// Test the full chain: delegation grants /archive, invocation uses /archive/put,
    /// and we verify the chain can be serialized, deserialized, and still verify.
    #[dialog_common::test]
    async fn it_roundtrips_and_verifies_archive_to_put_chain() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        let delegation =
            create_delegation(&subject_signer, &operator_signer, &subject_signer, &["use"])
                .await
                .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(vec![
                "use".to_string(),
                "put".to_string(),
                "archive".to_string(),
                "block".to_string(),
            ])
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let original_chain = InvocationChain::new(invocation, delegations);

        assert!(
            original_chain
                .verify(&test_context(&test_environment(&original_chain)))
                .await
                .is_ok(),
            "Original chain should verify"
        );

        let cbor_bytes = serde_ipld_dagcbor::to_vec(&original_chain).expect("Failed to serialize");

        let restored_chain: InvocationChain<AnySignature> =
            serde_ipld_dagcbor::from_slice(&cbor_bytes).expect("Failed to deserialize");

        let result = restored_chain
            .verify(&test_context(&test_environment(&restored_chain)))
            .await;
        assert!(
            result.is_ok(),
            "Restored chain should still verify: {:?}",
            result
        );

        assert_eq!(
            restored_chain.command().to_string(),
            original_chain.command().to_string()
        );
        assert_eq!(restored_chain.subject(), original_chain.subject());
        assert_eq!(restored_chain.proofs().len(), original_chain.proofs().len());
    }

    #[dialog_common::test]
    async fn it_verifies_self_invocation_with_empty_proofs() {
        let signer = generate_signer().await;
        let did = signer.did();

        let invocation = InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(&did)
            .subject(&did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let chain = InvocationChain::new(invocation, HashMap::new());

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_ok(),
            "Self-invocation (issuer == subject, empty proofs) should verify: {:?}",
            result
        );
    }

    #[dialog_common::test]
    async fn it_fails_self_invocation_with_wrong_subject() {
        let signer = generate_signer().await;
        let other_subject = generate_signer().await.did();

        let invocation = InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(&other_subject)
            .subject(&other_subject)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![])
            .try_build()
            .await
            .expect("Failed to build invocation");

        let chain = InvocationChain::new(invocation, HashMap::new());

        let result = chain.verify(&test_context(&test_environment(&chain))).await;
        assert!(
            result.is_err(),
            "Invocation with issuer != subject and no proofs should fail verification"
        );
    }
}
