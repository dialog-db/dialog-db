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

use super::Container;
use crate::capability::AccessError;
use ipld_core::cid::Cid;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use ucan::{
    Delegation,
    did::Ed25519Did,
    future::Sendable,
    invocation::{CheckFailed, Invocation, InvocationCheckError, StoredCheckError},
};

/// In-memory delegation store for verification.
type ProofStore = Arc<Mutex<HashMap<Cid, Arc<Delegation<Ed25519Did>>>>>;

/// Concrete invocation check error type for our ProofStore.
type InvocationError =
    InvocationCheckError<Sendable, Ed25519Did, Arc<Delegation<Ed25519Did>>, ProofStore>;

/// An invocation with its delegation chain, parsed from a UCAN container.
///
/// This represents a complete authorization bundle containing:
/// - The invocation (the signed command to execute)
/// - The delegation chain (proofs of authority from subject to invoker)
///
/// The invocation references its proofs by CID, and the delegation chain
/// provides those proofs for verification.
#[derive(Debug, Clone)]
pub struct InvocationChain {
    /// The signed invocation containing the command and arguments.
    pub invocation: Invocation<Ed25519Did>,
    /// The delegation chain as a map keyed by CID for proof lookup.
    delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>>,
}

impl InvocationChain {
    /// Create a new invocation chain from an invocation and delegations.
    pub fn new(
        invocation: Invocation<Ed25519Did>,
        delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>>,
    ) -> Self {
        Self {
            invocation,
            delegations,
        }
    }

    /// Verify the invocation chain using rs-ucan's verification.
    ///
    /// This performs complete verification:
    /// 1. Signature verification (issuer signed the invocation)
    /// 2. Proof chain validation (issuer→subject chain via proofs)
    /// 3. Command attenuation checks
    /// 4. Policy predicate evaluation
    ///
    /// The invocation's `proofs` field contains CIDs that reference
    /// delegations in the container. This method builds a store from
    /// those delegations and uses rs-ucan's `Invocation::check` to verify.
    pub async fn verify(&self) -> Result<(), AccessError> {
        // Build delegation store from our map
        let store: ProofStore = Arc::new(Mutex::new(self.delegations.clone()));

        // Use rs-ucan's full verification
        self.invocation
            .check::<Sendable, _, _>(&store)
            .await
            .map_err(Into::into)
    }

    /// Get the command from the invocation.
    pub fn command(&self) -> &ucan::command::Command {
        self.invocation.command()
    }

    /// Get the arguments from the invocation.
    pub fn arguments(&self) -> &BTreeMap<String, ucan::promise::Promised> {
        self.invocation.arguments()
    }

    /// Get the subject (root authority) of the invocation.
    pub fn subject(&self) -> &Ed25519Did {
        self.invocation.subject()
    }

    /// Get the issuer of the invocation.
    pub fn issuer(&self) -> &Ed25519Did {
        self.invocation.issuer()
    }

    /// Get the proof CIDs referenced by the invocation.
    pub fn proofs(&self) -> &Vec<Cid> {
        self.invocation.proofs()
    }

    /// Serialize to DAG-CBOR bytes (UCAN container format).
    pub fn to_bytes(&self) -> Result<Vec<u8>, AccessError> {
        Container::from(self).to_bytes()
    }
}

impl TryFrom<&[u8]> for InvocationChain {
    type Error = AccessError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let container = Container::from_bytes(bytes)?;
        InvocationChain::try_from(container)
    }
}

impl TryFrom<Container> for InvocationChain {
    type Error = AccessError;

    /// Convert a container to an invocation chain.
    ///
    /// The first token must be the invocation, followed by the delegation chain.
    fn try_from(container: Container) -> Result<Self, Self::Error> {
        let token_bytes = container.into_tokens();

        if token_bytes.is_empty() {
            return Err(AccessError::Invocation(
                "container must contain at least an invocation".to_string(),
            ));
        }

        // First token is the invocation
        let invocation: Invocation<Ed25519Did> = serde_ipld_dagcbor::from_slice(&token_bytes[0])
            .map_err(|e| AccessError::Invocation(format!("failed to decode invocation: {}", e)))?;

        // Remaining tokens are delegations - build a map keyed by CID
        let mut delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>> =
            HashMap::with_capacity(token_bytes.len() - 1);
        for (i, bytes) in token_bytes.iter().skip(1).enumerate() {
            let delegation: Delegation<Ed25519Did> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    AccessError::Invocation(format!("failed to decode delegation {}: {}", i, e))
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

impl From<&InvocationChain> for Container {
    fn from(chain: &InvocationChain) -> Self {
        let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(1 + chain.delegations.len());

        // First token is the invocation
        if let Ok(invocation_bytes) = serde_ipld_dagcbor::to_vec(&chain.invocation) {
            tokens.push(invocation_bytes);
        }

        // Add delegations in the order they appear in the invocation's proofs
        for cid in chain.invocation.proofs() {
            if let Some(delegation) = chain.delegations.get(cid) {
                if let Ok(delegation_bytes) = serde_ipld_dagcbor::to_vec(delegation.as_ref()) {
                    tokens.push(delegation_bytes);
                }
            }
        }

        Container::new(tokens)
    }
}

impl Serialize for InvocationChain {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for InvocationChain {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Use serde_bytes::ByteBuf to properly deserialize CBOR byte strings
        let bytes: serde_bytes::ByteBuf = serde_bytes::ByteBuf::deserialize(deserializer)?;
        InvocationChain::try_from(bytes.as_slice()).map_err(serde::de::Error::custom)
    }
}

impl From<CheckFailed> for AccessError {
    fn from(err: CheckFailed) -> Self {
        match err {
            CheckFailed::InvalidProofIssuerChain => {
                AccessError::Invocation("invalid proof issuer chain".to_string())
            }
            CheckFailed::SubjectNotAllowedByProof => {
                AccessError::Invocation("subject not allowed by proof".to_string())
            }
            CheckFailed::RootProofIssuerIsNotSubject => {
                AccessError::Invocation("root proof issuer is not the subject".to_string())
            }
            CheckFailed::CommandMismatch { expected, found } => AccessError::Invocation(format!(
                "command mismatch: expected {:?}, found {:?}",
                expected, found
            )),
            CheckFailed::PredicateFailed(predicate) => {
                AccessError::Invocation(format!("predicate failed: {:?}", predicate))
            }
            CheckFailed::PredicateRunError(run_err) => {
                AccessError::Invocation(format!("predicate run error: {}", run_err))
            }
            CheckFailed::WaitingOnPromise(waiting) => {
                AccessError::Invocation(format!("waiting on promise: {:?}", waiting))
            }
        }
    }
}

impl From<InvocationError> for AccessError {
    fn from(err: InvocationError) -> Self {
        match err {
            InvocationCheckError::SignatureVerification(sig_err) => {
                AccessError::Invocation(format!("invalid signature: {}", sig_err))
            }
            InvocationCheckError::StoredCheck(stored_err) => match stored_err {
                StoredCheckError::GetError(get_err) => {
                    AccessError::Invocation(format!("proof not found: {}", get_err))
                }
                StoredCheckError::CheckFailed(check_err) => check_err.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ucan::delegation::helpers::{create_delegation, generate_signer};
    use ucan::delegation::builder::DelegationBuilder;
    use ucan::delegation::subject::DelegatedSubject;
    use ucan::invocation::builder::InvocationBuilder;

    /// Create a test invocation chain with a valid delegation.
    async fn create_test_invocation_chain() -> (InvocationChain, Ed25519Did) {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();

        // Create delegation: subject -> operator
        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            &["storage", "get"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation from operator
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did.clone())
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![delegation_cid])
            .try_build(&operator_signer)
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
        let restored: InvocationChain =
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
        let result = chain.verify().await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed: {:?}",
            result
        );
    }

    #[dialog_common::test]
    async fn it_fails_verification_when_proof_is_missing() {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();

        // Create delegation but don't include it in the chain
        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            &["storage"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation referencing the delegation
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![delegation_cid])
            .try_build(&operator_signer)
            .await
            .expect("Failed to build invocation");

        // Create chain WITHOUT the delegation
        let chain = InvocationChain::new(invocation, HashMap::new());

        // Should fail verification due to missing proof
        let result = chain.verify().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("proof not found"));
    }

    #[dialog_common::test]
    async fn it_fails_verification_when_issuer_is_wrong() {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();
        let wrong_operator_signer = generate_signer();

        // Create delegation to operator
        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            &["storage"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation from WRONG operator (not the delegation audience)
        let invocation = InvocationBuilder::new()
            .issuer(wrong_operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![delegation_cid])
            .try_build(&wrong_operator_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        // Should fail verification due to issuer mismatch
        let result = chain.verify().await;
        assert!(result.is_err());
    }

    #[test]
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

    #[test]
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
        // Powerline delegation (sub: null) in the middle of the chain.
        // Chain: subject -> device1 (specific subject) -> device2 (powerline)
        // The powerline delegation inherits the subject from the prior delegation.
        //
        // Proofs must be ordered from subject to invoker (root to leaf).

        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let device1_signer = generate_signer();
        let device2_signer = generate_signer();

        // Root delegation: subject -> device1 (with specific subject)
        let root_delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(device1_signer.did().clone())
            .subject(DelegatedSubject::Specific(subject_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build(&subject_signer)
            .await
            .expect("Failed to build root delegation");

        let root_cid = root_delegation.to_cid();

        // Powerline delegation: device1 -> device2 (with sub: null)
        // This allows device1 to delegate to device2 for ANY subject it has access to
        let powerline_delegation = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(device2_signer.did().clone())
            .subject(DelegatedSubject::Any) // 👈 Powerline: sub: null
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build(&device1_signer)
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_delegation.to_cid();

        // Invocation from device2
        // Proofs ordered: root (subject->device1), then powerline (device1->device2)
        let invocation = InvocationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did.clone())
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![root_cid, powerline_cid]) // 👈 root first, then powerline
            .try_build(&device2_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(root_cid, Arc::new(root_delegation));
        delegations.insert(powerline_cid, Arc::new(powerline_delegation));

        let chain = InvocationChain::new(invocation, delegations);

        // Should verify successfully - powerline inherits subject from root delegation
        let result = chain.verify().await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed with powerline in middle: {:?}",
            result
        );
    }

    #[dialog_common::test]
    async fn it_fails_verification_with_powerline_at_root_wrong_subject() {
        // Powerline delegation at root means subject is inferred from issuer.
        // If the invocation's subject doesn't match the powerline issuer, it should fail.
        //
        // Per UCAN spec: "Powerline delegations MUST NOT be used as the root delegation
        // to a resource. A priori there is no such thing as a null subject."
        //
        // When powerline is at root, the issuer of that delegation becomes the subject.
        // So invoking with a different subject should fail.

        let device1_signer = generate_signer();
        let device2_signer = generate_signer();
        let some_other_subject = generate_signer().did().clone();

        // Powerline delegation at root: device1 -> device2 (sub: null)
        // At root, this means subject = device1 (the issuer)
        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(device2_signer.did().clone())
            .subject(DelegatedSubject::Any) // 👈 Powerline at root
            .command(vec!["storage".to_string()])
            .try_build(&device1_signer)
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        // Invocation from device2 trying to act on a DIFFERENT subject
        // This should fail because powerline at root implies subject = device1
        let invocation = InvocationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(some_other_subject.clone())
            .subject(some_other_subject) // 👈 Wrong! Should be device1
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid])
            .try_build(&device2_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));

        let chain = InvocationChain::new(invocation, delegations);

        // Should fail - invocation subject doesn't match powerline issuer
        let result = chain.verify().await;
        assert!(
            result.is_err(),
            "Expected verification to fail when invocation subject doesn't match powerline root issuer"
        );
    }

    #[dialog_common::test]
    async fn it_verifies_chain_with_powerline_at_root_matching_issuer() {
        // Powerline at root is valid when the invocation subject matches the
        // powerline issuer (since sub: null at root implies subject = issuer).

        let device1_signer = generate_signer();
        let device1_did = device1_signer.did().clone();
        let device2_signer = generate_signer();

        // Powerline delegation at root: device1 -> device2 (sub: null)
        // At root, this means subject = device1 (the issuer)
        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(device2_signer.did().clone())
            .subject(DelegatedSubject::Any) // 👈 Powerline at root
            .command(vec!["storage".to_string()])
            .try_build(&device1_signer)
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        // Invocation from device2 with subject = device1 (the powerline issuer)
        let invocation = InvocationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(device1_did.clone())
            .subject(device1_did.clone()) // 👈 Matches powerline issuer
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid])
            .try_build(&device2_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));

        let chain = InvocationChain::new(invocation, delegations);

        // Should succeed - invocation subject matches powerline root issuer
        let result = chain.verify().await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed when invocation subject matches powerline root issuer: {:?}",
            result
        );
    }

    #[dialog_common::test]
    async fn it_fails_when_redelegation_after_powerline_root_uses_wrong_subject() {
        // Scenario: Powerline at root, then a redelegation that tries to claim
        // authority over a different subject than the powerline root issuer.
        //
        // Chain:
        // 1. device1 -> device2 (powerline, sub: null) - implies subject = device1
        // 2. device2 -> device3 (sub: some_other_resource) - INVALID!
        //
        // The second delegation cannot grant authority over some_other_resource
        // because the powerline only grants authority for device1 (the issuer).

        let device1_signer = generate_signer();
        let device2_signer = generate_signer();
        let device3_signer = generate_signer();
        let some_other_resource = generate_signer().did().clone();

        // Powerline delegation at root: device1 -> device2 (sub: null)
        // This grants device2 authority to act on behalf of device1
        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(device2_signer.did().clone())
            .subject(DelegatedSubject::Any) // 👈 Powerline at root
            .command(vec!["storage".to_string()])
            .try_build(&device1_signer)
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        // Invalid redelegation: device2 -> device3 with a DIFFERENT subject
        // device2 only has authority for device1, not some_other_resource
        let bad_redelegation = DelegationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(device3_signer.did().clone())
            .subject(DelegatedSubject::Specific(some_other_resource.clone())) // 👈 Wrong subject!
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build(&device2_signer)
            .await
            .expect("Failed to build redelegation");

        let bad_cid = bad_redelegation.to_cid();

        // Invocation from device3 trying to act on some_other_resource
        let invocation = InvocationBuilder::new()
            .issuer(device3_signer.clone())
            .audience(some_other_resource.clone())
            .subject(some_other_resource)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid, bad_cid]) // root first, then redelegation
            .try_build(&device3_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));
        delegations.insert(bad_cid, Arc::new(bad_redelegation));

        let chain = InvocationChain::new(invocation, delegations);

        // Should fail - redelegation claims authority over wrong subject
        let result = chain.verify().await;
        assert!(
            result.is_err(),
            "Expected verification to fail when redelegation after powerline root uses wrong subject"
        );
    }

    #[dialog_common::test]
    async fn it_verifies_when_redelegation_after_powerline_root_uses_correct_subject() {
        // Scenario: Powerline at root, then a valid redelegation that correctly
        // delegates authority for the powerline root issuer.
        //
        // Chain:
        // 1. device1 -> device2 (powerline, sub: null) - implies subject = device1
        // 2. device2 -> device3 (sub: device1) - valid redelegation
        //
        // The second delegation correctly grants authority over device1.

        let device1_signer = generate_signer();
        let device1_did = device1_signer.did().clone();
        let device2_signer = generate_signer();
        let device3_signer = generate_signer();

        // Powerline delegation at root: device1 -> device2 (sub: null)
        let powerline_root = DelegationBuilder::new()
            .issuer(device1_signer.clone())
            .audience(device2_signer.did().clone())
            .subject(DelegatedSubject::Any) // 👈 Powerline at root
            .command(vec!["storage".to_string()])
            .try_build(&device1_signer)
            .await
            .expect("Failed to build powerline delegation");

        let powerline_cid = powerline_root.to_cid();

        // Valid redelegation: device2 -> device3 with correct subject (device1)
        let valid_redelegation = DelegationBuilder::new()
            .issuer(device2_signer.clone())
            .audience(device3_signer.did().clone())
            .subject(DelegatedSubject::Specific(device1_did.clone())) // 👈 Correct subject
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build(&device2_signer)
            .await
            .expect("Failed to build redelegation");

        let valid_cid = valid_redelegation.to_cid();

        // Invocation from device3 acting on device1
        let invocation = InvocationBuilder::new()
            .issuer(device3_signer.clone())
            .audience(device1_did.clone())
            .subject(device1_did.clone())
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![powerline_cid, valid_cid]) // root first, then redelegation
            .try_build(&device3_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(powerline_cid, Arc::new(powerline_root));
        delegations.insert(valid_cid, Arc::new(valid_redelegation));

        let chain = InvocationChain::new(invocation, delegations);

        // Should succeed - redelegation correctly uses the powerline root issuer as subject
        let result = chain.verify().await;
        assert!(
            result.is_ok(),
            "Expected verification to succeed when redelegation after powerline root uses correct subject: {:?}",
            result
        );
    }

    /// Test invocation chain with archive/put command roundtrips correctly.
    #[dialog_common::test]
    async fn it_roundtrips_archive_put_invocation() {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();

        // Create delegation granting /archive capability
        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            &["archive"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation for /archive/put
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did.clone())
            .command(vec!["archive".to_string(), "put".to_string()])
            .proofs(vec![delegation_cid])
            .try_build(&operator_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        // Verify command
        assert_eq!(chain.command().to_string(), "/archive/put");

        // Serialize and deserialize via container format
        let bytes = chain.to_bytes().expect("Failed to serialize");
        let restored = InvocationChain::try_from(bytes.as_slice()).expect("Failed to deserialize");

        assert_eq!(restored.command().to_string(), "/archive/put");
        assert_eq!(restored.subject(), &subject_did);
    }

    /// Test invocation chain with serde DAG-CBOR roundtrip for archive/put.
    #[dialog_common::test]
    async fn it_serde_roundtrips_archive_put_invocation() {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();

        // Create delegation granting /archive capability
        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            &["archive"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation for /archive/put
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did.clone())
            .command(vec!["archive".to_string(), "put".to_string()])
            .proofs(vec![delegation_cid])
            .try_build(&operator_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        // Serialize via serde to DAG-CBOR
        let cbor_bytes = serde_ipld_dagcbor::to_vec(&chain).expect("Failed to serialize");

        // Deserialize via serde from DAG-CBOR
        let restored: InvocationChain =
            serde_ipld_dagcbor::from_slice(&cbor_bytes).expect("Failed to deserialize");

        assert_eq!(restored.command().to_string(), "/archive/put");
        assert_eq!(restored.subject(), &subject_did);
    }

    /// Test that a delegation granting /archive can authorize an /archive/put invocation.
    /// This demonstrates the delegation chain where the delegation grants a parent
    /// capability and the invocation uses a more specific child capability.
    #[dialog_common::test]
    async fn it_verifies_archive_delegation_authorizes_archive_put_invocation() {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();

        // Create delegation granting /archive (parent capability)
        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            &["archive"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation for /archive/put (child capability - more specific)
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did.clone())
            .command(vec!["archive".to_string(), "put".to_string()])
            .proofs(vec![delegation_cid])
            .try_build(&operator_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);

        // The /archive delegation should authorize the /archive/put invocation
        let result = chain.verify().await;
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
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();

        // Create delegation granting /archive
        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            &["archive"],
        )
        .await
        .expect("Failed to create delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation for /archive/put
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did.clone())
            .command(vec!["archive".to_string(), "put".to_string()])
            .proofs(vec![delegation_cid])
            .try_build(&operator_signer)
            .await
            .expect("Failed to build invocation");

        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));

        let original_chain = InvocationChain::new(invocation, delegations);

        // Verify original
        assert!(
            original_chain.verify().await.is_ok(),
            "Original chain should verify"
        );

        // Serialize via serde DAG-CBOR
        let cbor_bytes = serde_ipld_dagcbor::to_vec(&original_chain).expect("Failed to serialize");

        // Deserialize
        let restored_chain: InvocationChain =
            serde_ipld_dagcbor::from_slice(&cbor_bytes).expect("Failed to deserialize");

        // Verify restored chain still works
        let result = restored_chain.verify().await;
        assert!(
            result.is_ok(),
            "Restored chain should still verify: {:?}",
            result
        );

        // Verify properties preserved
        assert_eq!(
            restored_chain.command().to_string(),
            original_chain.command().to_string()
        );
        assert_eq!(restored_chain.subject(), original_chain.subject());
        assert_eq!(restored_chain.proofs().len(), original_chain.proofs().len());
    }

    #[dialog_common::test]
    async fn it_verifies_self_invocation_with_empty_proofs() {
        // Self-invocation: issuer == subject, no delegation needed.
        // This is the case when a subject is acting on itself (e.g., a Space
        // accessing its own storage), which is inherently authorized.
        let signer = generate_signer();
        let did = signer.did().clone();

        // Create invocation where issuer == subject, empty proofs
        let invocation = InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(did.clone())
            .subject(did)
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![]) // Empty proofs for self-auth
            .try_build(&signer)
            .await
            .expect("Failed to build invocation");

        let chain = InvocationChain::new(invocation, HashMap::new());

        // Should verify successfully - subject acting on itself
        let result = chain.verify().await;
        assert!(
            result.is_ok(),
            "Self-invocation (issuer == subject, empty proofs) should verify: {:?}",
            result
        );
    }

    #[dialog_common::test]
    async fn it_fails_self_invocation_with_wrong_subject() {
        // Self-invocation fails if the issuer claims to act on a different subject
        let signer = generate_signer();
        let other_subject = generate_signer().did().clone();

        // Create invocation where issuer != subject but no proofs
        // This should fail because the issuer has no authority over other_subject
        let invocation = InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(other_subject.clone())
            .subject(other_subject) // Different from issuer!
            .command(vec!["storage".to_string(), "get".to_string()])
            .proofs(vec![]) // No proofs to establish authority
            .try_build(&signer)
            .await
            .expect("Failed to build invocation");

        let chain = InvocationChain::new(invocation, HashMap::new());

        // Should fail - issuer has no authority over other_subject
        let result = chain.verify().await;
        assert!(
            result.is_err(),
            "Invocation with issuer != subject and no proofs should fail verification"
        );
    }
}
