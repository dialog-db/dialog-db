//! UCAN delegation chain management.
//!
//! This module provides [`DelegationChain`], which represents a chain of UCAN delegations
//! proving authority from a subject to an operator.
//!
//! Delegations are stored in subject-first (root-to-leaf) order:
//! - Index 0 is the root delegation (closest to subject, its `iss` is the subject)
//! - Last index is closest to the invoker (its `aud` is the operator)
//!
//! This matches the proof order expected by UCAN invocation verification.

use super::{Container, ContainerError};
use crate::Delegation;
use crate::subject::Subject;
use crate::time::Timestamp;
use dialog_varsig::Did;
use dialog_varsig::eddsa::Ed25519Signature;
use ipld_core::cid::Cid;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::sync::Arc;

/// A chain of UCAN delegations proving authority over a subject.
///
/// A delegation chain consists of one or more delegations that together prove
/// that the operator has been granted authority over a subject. Each delegation
/// in the chain grants authority from one party to another, forming a chain
/// from the subject (root authority) to the operator.
///
/// A chain must have at least one delegation. For cases where no delegations
/// are present, use `Option<DelegationChain>` instead.
#[derive(Debug, Clone)]
pub struct DelegationChain {
    /// The delegation proofs keyed by CID.
    delegations: HashMap<Cid, Arc<Delegation<Ed25519Signature>>>,
    /// The CIDs of the delegation proofs in subject-first (root-to-leaf) order.
    /// This is guaranteed to be non-empty.
    proof_cids: Vec<Cid>,
}

impl PartialEq for DelegationChain {
    fn eq(&self, other: &Self) -> bool {
        // Delegation chains are equal if they have the same proof CIDs
        // (CIDs are content-addressed, so same CIDs means same content)
        self.proof_cids == other.proof_cids
    }
}

impl Eq for DelegationChain {}

impl DelegationChain {
    /// Create a new delegation chain with a single delegation.
    ///
    /// This is the primary constructor for creating a delegation chain from a single
    /// root delegation (typically subject -> operator).
    pub fn new(delegation: Delegation<Ed25519Signature>) -> Self {
        let cid = delegation.to_cid();
        let mut delegations = HashMap::with_capacity(1);
        delegations.insert(cid, Arc::new(delegation));

        Self {
            delegations,
            proof_cids: vec![cid],
        }
    }

    /// Create from raw delegation bytes (deserializes each as a Delegation).
    ///
    /// This is a lower-level method that takes a list of DAG-CBOR encoded delegation bytes.
    /// For parsing from the container format, use the `TryFrom<&[u8]>` implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes list is empty or if any delegation fails to deserialize.
    pub fn from_delegation_bytes(proof_bytes: Vec<Vec<u8>>) -> Result<Self, ContainerError> {
        if proof_bytes.is_empty() {
            return Err(ContainerError::Configuration(
                "DelegationChain requires at least one delegation".to_string(),
            ));
        }

        let mut delegations_vec = Vec::with_capacity(proof_bytes.len());
        for (i, bytes) in proof_bytes.iter().enumerate() {
            let delegation: Delegation<Ed25519Signature> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    ContainerError::Invocation(format!("failed to decode delegation {}: {}", i, e))
                })?;
            delegations_vec.push(delegation);
        }
        Self::try_from(delegations_vec)
    }

    /// Serialize to DAG-CBOR bytes in the UCAN container format.
    ///
    /// The container format is: `{ "ctn-v1": [delegation_0_bytes, ...] }`
    /// where delegations are in subject-first (root-to-leaf) order.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContainerError> {
        Container::from(self).to_bytes()
    }

    /// Get the CIDs for use in invocation proofs field.
    pub fn proof_cids(&self) -> &[Cid] {
        &self.proof_cids
    }

    /// Get the delegations map for building InvocationChain.
    pub fn delegations(&self) -> &HashMap<Cid, Arc<Delegation<Ed25519Signature>>> {
        &self.delegations
    }

    /// Get the audience of the last delegation in the chain (closest to invoker).
    ///
    /// This is the operator/invoker DID.
    /// Since the chain is guaranteed non-empty, this always returns a value.
    pub fn audience(&self) -> &Did {
        let cid = &self.proof_cids[self.proof_cids.len() - 1];
        self.delegations.get(cid).unwrap().audience()
    }

    /// Get the subject of the delegation chain.
    ///
    /// The root delegation's (index 0) subject should match the claimed subject.
    ///
    /// Returns `None` if the delegation has no specific subject (i.e., `Subject::Any`).
    pub fn subject(&self) -> Option<&Did> {
        let cid = &self.proof_cids[0];
        let delegation = self.delegations.get(cid).unwrap();
        match delegation.subject() {
            Subject::Specific(did) => Some(did),
            Subject::Any => None,
        }
    }

    /// Get the issuer of the root delegation (index 0, closest to subject).
    ///
    /// The root delegation's issuer is the original authority that started the
    /// delegation chain. For powerline delegations (`Subject::Any`), this issuer
    /// is typically used as the effective subject.
    pub fn issuer(&self) -> &Did {
        let cid = &self.proof_cids[0];
        self.delegations.get(cid).unwrap().issuer()
    }

    /// Get the ability path of the last delegation (closest to invoker).
    ///
    /// Returns the ability as a string path (e.g., "/storage/get").
    /// The leaf delegation defines the most attenuated capability.
    pub fn ability(&self) -> String {
        let cid = &self.proof_cids[self.proof_cids.len() - 1];
        let delegation = self.delegations.get(cid).unwrap();
        let cmd = delegation.command();
        if cmd.0.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", cmd.0.join("/"))
        }
    }

    /// The effective earliest validity time across all delegations.
    ///
    /// Returns the latest `not_before` in the chain (most restrictive).
    /// `None` means no lower bound is imposed by any delegation.
    pub fn not_before(&self) -> Option<Timestamp> {
        self.proof_cids
            .iter()
            .filter_map(|cid| self.delegations.get(cid)?.not_before())
            .max()
    }

    /// The effective expiration time across all delegations.
    ///
    /// Returns the earliest `expiration` in the chain (most restrictive).
    /// `None` means no delegation in the chain expires.
    pub fn expiration(&self) -> Option<Timestamp> {
        self.proof_cids
            .iter()
            .filter_map(|cid| self.delegations.get(cid)?.expiration())
            .min()
    }

    /// Push a delegation onto the chain (closer to invoker).
    ///
    /// Its issuer must match the current chain's audience.
    pub fn push(&self, delegation: Delegation<Ed25519Signature>) -> Result<Self, ContainerError> {
        let current_audience = self.audience();
        let new_issuer = delegation.issuer();
        if new_issuer != current_audience {
            return Err(ContainerError::Invocation(format!(
                "Principal alignment error: delegation issuer '{}' does not match chain audience '{}'",
                new_issuer, current_audience
            )));
        }

        let cid = delegation.to_cid();

        let mut delegations = self.delegations.clone();
        delegations.insert(cid, Arc::new(delegation));

        let mut proof_cids = self.proof_cids.clone();
        proof_cids.push(cid);

        Ok(Self {
            delegations,
            proof_cids,
        })
    }
}

impl TryFrom<Vec<Delegation<Ed25519Signature>>> for DelegationChain {
    type Error = ContainerError;

    /// Create a delegation chain from a vector of delegations.
    ///
    /// The delegations must be in subject-first (root-to-leaf) order:
    /// - delegations[0] is closest to subject (its `iss` is the subject)
    /// - delegations[n-1] is closest to invoker (its `aud` is the operator)
    ///
    /// # Principal Alignment
    ///
    /// For each consecutive pair (i, i+1), the audience of delegation[i] must match
    /// the issuer of delegation[i+1]. This ensures a proper chain of authority.
    ///
    /// # Errors
    ///
    /// Returns an error if the vector is empty or if principal alignment fails.
    fn try_from(delegations_vec: Vec<Delegation<Ed25519Signature>>) -> Result<Self, Self::Error> {
        if delegations_vec.is_empty() {
            return Err(ContainerError::Configuration(
                "DelegationChain requires at least one delegation".to_string(),
            ));
        }

        // Verify principal alignment between consecutive delegations
        // In subject-first order: delegation[i].aud must == delegation[i+1].iss
        for i in 0..delegations_vec.len().saturating_sub(1) {
            let current = &delegations_vec[i];
            let next = &delegations_vec[i + 1];

            if current.audience() != next.issuer() {
                return Err(ContainerError::Invocation(format!(
                    "Principal alignment error at position {}: delegation audience '{}' does not match next delegation issuer '{}'",
                    i,
                    current.audience(),
                    next.issuer()
                )));
            }
        }

        let mut map = HashMap::with_capacity(delegations_vec.len());
        let mut cids = Vec::with_capacity(delegations_vec.len());

        for delegation in delegations_vec {
            let cid = delegation.to_cid();
            cids.push(cid);
            map.insert(cid, Arc::new(delegation));
        }

        Ok(Self {
            delegations: map,
            proof_cids: cids,
        })
    }
}

impl From<Delegation<Ed25519Signature>> for DelegationChain {
    fn from(delegation: Delegation<Ed25519Signature>) -> Self {
        Self::new(delegation)
    }
}

impl TryFrom<&[u8]> for DelegationChain {
    type Error = ContainerError;

    /// Deserialize a delegation chain from DAG-CBOR container format.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let container = Container::from_bytes(bytes)?;
        DelegationChain::try_from(container)
    }
}

impl TryFrom<Container> for DelegationChain {
    type Error = ContainerError;

    /// Convert a container to a delegation chain.
    fn try_from(container: Container) -> Result<Self, Self::Error> {
        let token_bytes = container.into_tokens();

        // Deserialize delegations and verify principal alignment
        let mut delegations_vec: Vec<Delegation<Ed25519Signature>> =
            Vec::with_capacity(token_bytes.len());
        for (i, bytes) in token_bytes.iter().enumerate() {
            let delegation: Delegation<Ed25519Signature> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    ContainerError::Invocation(format!("failed to decode delegation {}: {}", i, e))
                })?;
            delegations_vec.push(delegation);
        }

        // Use TryFrom<Vec<...>> to get principal alignment validation
        DelegationChain::try_from(delegations_vec)
    }
}

impl From<&DelegationChain> for Container {
    fn from(chain: &DelegationChain) -> Self {
        // Serialize delegations in proof_cids order
        let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(chain.proof_cids.len());

        for cid in &chain.proof_cids {
            if let Some(delegation) = chain.delegations.get(cid) {
                // Note: This unwrap is safe because we're serializing from a valid delegation
                if let Ok(bytes) = serde_ipld_dagcbor::to_vec(delegation.as_ref()) {
                    tokens.push(bytes);
                }
            }
        }

        Container::new(tokens)
    }
}

impl Serialize for DelegationChain {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for DelegationChain {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Use serde_bytes::ByteBuf to properly deserialize CBOR byte strings
        let bytes: serde_bytes::ByteBuf = serde_bytes::ByteBuf::deserialize(deserializer)?;
        DelegationChain::try_from(bytes.as_slice()).map_err(serde::de::Error::custom)
    }
}

/// Helper functions for testing delegation chains.
///
/// These are exported when the `helpers` feature is enabled so that
/// other crates can use them in their tests.
#[cfg(test)]
pub mod helpers {
    use super::*;
    use crate::DelegationBuilder;
    use dialog_credentials::Ed25519Signer;
    use dialog_varsig::Principal;

    /// Generate a new random Ed25519 signer.
    ///
    /// This is useful for creating space signers in tests.
    pub async fn generate_signer() -> Ed25519Signer {
        Ed25519Signer::generate()
            .await
            .expect("Failed to generate signer")
    }

    /// Create a delegation from issuer to audience for a subject with the given command.
    ///
    /// This is a convenience function for building simple delegations in tests.
    pub async fn create_delegation(
        issuer: &Ed25519Signer,
        audience: &impl Principal,
        subject: &impl Principal,
        command: &[&str],
    ) -> Result<Delegation<Ed25519Signature>, ContainerError> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(Subject::Specific(subject.did()))
            .command(command.iter().map(|&s| s.to_string()).collect())
            .try_build()
            .await
            .map_err(|e| ContainerError::Invocation(format!("Failed to build delegation: {:?}", e)))
    }
}

/// Tests for delegation chains.
///
/// These are only compiled when running tests (not when `helpers` feature is enabled),
/// because they use `#[dialog_common::test]` which requires dev-dependencies like
/// `wasm-bindgen-test` and `tokio` that are only available in test builds.
#[cfg(test)]
mod tests {
    use super::helpers::*;
    use super::*;
    use crate::DelegationBuilder;
    use crate::time::Timestamp;
    use crate::time::timestamp::{Duration, UNIX_EPOCH};
    use dialog_varsig::Principal;

    #[test]
    fn it_requires_non_empty_chain() {
        let result = DelegationChain::try_from(vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least one"));
    }

    #[dialog_common::test]
    async fn it_creates_chain_from_single_delegation() {
        let space_signer = generate_signer().await;
        let space_did = space_signer.did();
        let operator_signer = generate_signer().await;

        let delegation = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);
        assert_eq!(chain.proof_cids().len(), 1);
        assert_eq!(chain.delegations().len(), 1);
        assert_eq!(chain.audience(), &operator_signer.did());
        assert_eq!(chain.subject(), Some(&space_did));
    }

    #[dialog_common::test]
    async fn it_creates_chain_from_vec() {
        let space_signer = generate_signer().await;
        let space_did = space_signer.did();
        let operator1_signer = generate_signer().await;
        let operator2_signer = generate_signer().await;

        // First delegation: space -> operator1
        let delegation1 = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator1_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        // Second delegation: operator1 -> operator2
        let delegation2 = DelegationBuilder::new()
            .issuer(operator1_signer.clone())
            .audience(&operator2_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .unwrap();

        // Subject-first order: root delegation first, leaf delegation last
        let chain = DelegationChain::try_from(vec![delegation1, delegation2]).unwrap();
        assert_eq!(chain.proof_cids().len(), 2);
        assert_eq!(chain.delegations().len(), 2);
    }

    #[dialog_common::test]
    async fn it_extends_chain_with_new_delegation() {
        // Create initial delegation: space -> operator1
        let space_signer = generate_signer().await;
        let space_did = space_signer.did();
        let operator1_signer = generate_signer().await;

        let initial_delegation = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator1_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(initial_delegation);
        assert_eq!(chain.proof_cids().len(), 1);

        // Extend: operator1 -> operator2
        let operator2_signer = generate_signer().await;

        let second_delegation = DelegationBuilder::new()
            .issuer(operator1_signer.clone())
            .audience(&operator2_signer)
            .subject(Subject::Specific(space_did))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .unwrap();

        let extended_chain = chain.push(second_delegation).unwrap();

        // Extended chain should have 2 delegations
        assert_eq!(extended_chain.proof_cids().len(), 2);
        assert_eq!(extended_chain.delegations().len(), 2);

        // Original chain should be unchanged
        assert_eq!(chain.proof_cids().len(), 1);
    }

    #[dialog_common::test]
    async fn it_fails_extend_on_principal_misalignment() {
        // Create initial delegation: space -> operator1
        let space_signer = generate_signer().await;
        let space_did = space_signer.did();
        let operator1_signer = generate_signer().await;

        let initial_delegation = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator1_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(initial_delegation);

        // Try to extend with wrong issuer (operator2 instead of operator1)
        let operator2_signer = generate_signer().await;
        let operator3_signer = generate_signer().await;

        let bad_delegation = DelegationBuilder::new()
            .issuer(operator2_signer.clone()) // Wrong! Should be operator1
            .audience(&operator3_signer)
            .subject(Subject::Specific(space_did))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .unwrap();

        let result = chain.push(bad_delegation);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Principal alignment error")
        );
    }

    #[dialog_common::test]
    async fn it_fails_try_from_on_principal_misalignment() {
        // Create delegations that don't align
        let space_signer = generate_signer().await;
        let space_did = space_signer.did();
        let operator1_signer = generate_signer().await;
        let operator2_signer = generate_signer().await;
        let operator3_signer = generate_signer().await;

        // Root delegation: space -> operator1 (closest to subject)
        let delegation1 = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator1_signer) // Wrong! Should be operator2 for alignment
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        // Leaf delegation: operator2 -> operator3 (closest to invoker)
        let delegation2 = DelegationBuilder::new()
            .issuer(operator2_signer.clone())
            .audience(&operator3_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .await
            .unwrap();

        // Subject-first order: delegation1.aud (operator1) != delegation2.iss (operator2)
        let result = DelegationChain::try_from(vec![delegation1, delegation2]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Principal alignment error")
        );
    }

    #[dialog_common::test]
    async fn it_serializes_and_deserializes_roundtrip() {
        let space_signer = generate_signer().await;
        let space_did = space_signer.did();
        let operator_signer = generate_signer().await;

        let delegation = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);

        // Serialize to bytes
        let bytes = chain.to_bytes().unwrap();

        // Deserialize back
        let restored = DelegationChain::try_from(bytes.as_slice()).unwrap();

        // Verify the chains match
        assert_eq!(chain.proof_cids().len(), restored.proof_cids().len());
        assert_eq!(chain.audience(), restored.audience());
        assert_eq!(chain.subject(), restored.subject());
    }

    #[dialog_common::test]
    async fn it_serde_roundtrips_via_dagcbor() {
        let space_signer = generate_signer().await;
        let space_did = space_signer.did();
        let operator_signer = generate_signer().await;

        let delegation = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);

        // Serialize via serde to DAG-CBOR (this uses serialize_bytes internally)
        let cbor_bytes = serde_ipld_dagcbor::to_vec(&chain).unwrap();

        // Deserialize via serde from DAG-CBOR (this uses dialog_common::Bytes)
        let restored: DelegationChain = serde_ipld_dagcbor::from_slice(&cbor_bytes).unwrap();

        // Verify the chains match
        assert_eq!(chain, restored);
        assert_eq!(chain.proof_cids(), restored.proof_cids());
        assert_eq!(chain.audience(), restored.audience());
        assert_eq!(chain.subject(), restored.subject());
    }

    /// Test that a delegation for archive capability roundtrips correctly.
    /// This tests creating a delegation that grants /archive access.
    #[dialog_common::test]
    async fn it_roundtrips_archive_delegation() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        // Create delegation granting /archive capability
        let delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(subject_did.clone()))
            .command(vec!["archive".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);

        // Verify ability path
        assert_eq!(chain.ability(), "/archive");

        // Serialize and deserialize
        let bytes = chain.to_bytes().unwrap();
        let restored = DelegationChain::try_from(bytes.as_slice()).unwrap();

        assert_eq!(chain, restored);
        assert_eq!(restored.ability(), "/archive");
    }

    #[dialog_common::test]
    async fn it_reports_unbounded_when_no_time_constraints() {
        let space_signer = generate_signer().await;
        let operator_signer = generate_signer().await;

        let delegation = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(space_signer.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);
        assert!(chain.not_before().is_none());
        assert!(chain.expiration().is_none());
    }

    #[dialog_common::test]
    async fn it_reports_time_bounds_from_single_delegation() {
        let space_signer = generate_signer().await;
        let operator_signer = generate_signer().await;

        let nbf = Timestamp::new(UNIX_EPOCH + Duration::from_secs(1000)).unwrap();
        let exp = Timestamp::new(UNIX_EPOCH + Duration::from_secs(5000)).unwrap();

        let delegation = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(space_signer.did()))
            .command(vec!["storage".to_string()])
            .not_before(nbf)
            .expiration(exp)
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);
        assert_eq!(chain.not_before(), Some(nbf));
        assert_eq!(chain.expiration(), Some(exp));
    }

    #[dialog_common::test]
    async fn it_computes_tightest_bounds_from_chain() {
        let space_signer = generate_signer().await;
        let mid_signer = generate_signer().await;
        let operator_signer = generate_signer().await;

        // First delegation: valid from 100 to 10000
        let d1 = DelegationBuilder::new()
            .issuer(space_signer.clone())
            .audience(&mid_signer)
            .subject(Subject::Specific(space_signer.did()))
            .command(vec!["storage".to_string()])
            .not_before(Timestamp::new(UNIX_EPOCH + Duration::from_secs(100)).unwrap())
            .expiration(Timestamp::new(UNIX_EPOCH + Duration::from_secs(10000)).unwrap())
            .try_build()
            .await
            .unwrap();

        // Second delegation: valid from 500 to 5000 (tighter)
        let d2 = DelegationBuilder::new()
            .issuer(mid_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(space_signer.did()))
            .command(vec!["storage".to_string()])
            .not_before(Timestamp::new(UNIX_EPOCH + Duration::from_secs(500)).unwrap())
            .expiration(Timestamp::new(UNIX_EPOCH + Duration::from_secs(5000)).unwrap())
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::try_from(vec![d1, d2]).unwrap();

        // Effective bounds should be the tightest: not_before=500, expiration=5000
        let nbf = chain.not_before().unwrap();
        let exp = chain.expiration().unwrap();
        assert_eq!(nbf.to_unix(), 500);
        assert_eq!(exp.to_unix(), 5000);
    }

    /// Test that a delegation for archive/put capability roundtrips correctly.
    /// This tests the more specific command path.
    #[dialog_common::test]
    async fn it_roundtrips_archive_put_delegation() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        // Create delegation granting /archive/put capability
        let delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Specific(subject_did.clone()))
            .command(vec!["archive".to_string(), "put".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);

        // Verify ability path
        assert_eq!(chain.ability(), "/archive/put");

        // Serialize via serde to DAG-CBOR
        let cbor_bytes = serde_ipld_dagcbor::to_vec(&chain).unwrap();
        let restored: DelegationChain = serde_ipld_dagcbor::from_slice(&cbor_bytes).unwrap();

        assert_eq!(chain, restored);
        assert_eq!(restored.ability(), "/archive/put");
    }
}
