//! UCAN delegation chain management.
//!
//! This module provides [`DelegationChain`], which represents a chain of UCAN delegations
//! proving authority from a subject to an operator.
//!
//! # Container Format
//!
//! `DelegationChain` can serialize to/from the [UCAN Container spec](https://github.com/ucan-wg/container):
//!
//! ```text
//! { "ctn-v1": [delegation_0_bytes, delegation_1_bytes, ...] }
//! ```
//!
//! Where tokens are DAG-CBOR serialized delegations, ordered from closest to invoker
//! (index 0) to closest to subject (last index).

use super::container::Container;
use crate::access::AuthorizationError;
use ipld_core::cid::Cid;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::sync::Arc;
use ucan::Delegation;
use ucan::did::Ed25519Did;

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
    delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>>,
    /// The CIDs of the delegation proofs (for reference in invocations).
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
    /// root delegation (typically subject → operator).
    pub fn new(delegation: Delegation<Ed25519Did>) -> Self {
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
    pub fn from_delegation_bytes(proof_bytes: Vec<Vec<u8>>) -> Result<Self, AuthorizationError> {
        if proof_bytes.is_empty() {
            return Err(AuthorizationError::Configuration(
                "DelegationChain requires at least one delegation".to_string(),
            ));
        }

        let mut delegations_vec = Vec::with_capacity(proof_bytes.len());
        for (i, bytes) in proof_bytes.iter().enumerate() {
            let delegation: Delegation<Ed25519Did> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    AuthorizationError::Invocation(format!(
                        "failed to decode delegation {}: {}",
                        i, e
                    ))
                })?;
            delegations_vec.push(delegation);
        }
        Self::try_from(delegations_vec)
    }

    /// Serialize to DAG-CBOR bytes in the UCAN container format.
    ///
    /// The container format is: `{ "ctn-v1": [delegation_0_bytes, ...] }`
    /// where delegations are ordered from closest to invoker to closest to subject.
    pub fn to_bytes(&self) -> Result<Vec<u8>, AuthorizationError> {
        Container::from(self).to_bytes()
    }

    /// Get the CIDs for use in invocation proofs field.
    pub fn proof_cids(&self) -> &[Cid] {
        &self.proof_cids
    }

    /// Get the delegations map for building InvocationChain.
    pub fn delegations(&self) -> &HashMap<Cid, Arc<Delegation<Ed25519Did>>> {
        &self.delegations
    }

    /// Get the audience of the first delegation in the chain.
    ///
    /// Per UCAN spec, the first delegation's `aud` should match the invoker (operator).
    /// Since the chain is guaranteed non-empty, this always returns a value.
    pub fn audience(&self) -> &Ed25519Did {
        // Safe because chain is guaranteed non-empty
        let cid = &self.proof_cids[0];
        self.delegations.get(cid).unwrap().audience()
    }

    /// Get the subject of the delegation chain.
    ///
    /// Per UCAN spec, the last delegation's `iss` (issuer) should match the `sub` (subject).
    /// This returns the subject from the last delegation in the chain, which represents
    /// the root authority being delegated from.
    ///
    /// Returns `None` if the delegation has no specific subject (i.e., `DelegatedSubject::Any`).
    pub fn subject(&self) -> Option<&Ed25519Did> {
        use ucan::delegation::subject::DelegatedSubject;

        // Safe because chain is guaranteed non-empty
        let cid = &self.proof_cids[self.proof_cids.len() - 1];
        let delegation = self.delegations.get(cid).unwrap();
        match delegation.subject() {
            DelegatedSubject::Specific(did) => Some(did),
            DelegatedSubject::Any => None,
        }
    }

    /// Get the command (ability) path of the first delegation.
    ///
    /// Returns the command as a string path (e.g., "/storage/get").
    /// The first delegation (closest to invoker) defines the most attenuated capability.
    pub fn can(&self) -> String {
        // Safe because chain is guaranteed non-empty
        let cid = &self.proof_cids[0];
        let delegation = self.delegations.get(cid).unwrap();
        let cmd = delegation.command();
        // Command is a newtype around Vec<String>, access inner via .0
        if cmd.0.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", cmd.0.join("/"))
        }
    }

    /// Create a new chain by extending this one with an additional delegation.
    ///
    /// The new delegation is added to the front of the proof chain (closest to invoker).
    ///
    /// # Principal Alignment
    ///
    /// This method verifies that the new delegation's issuer matches the current
    /// chain's audience (the first delegation's `aud`). This ensures proper chain
    /// alignment per the UCAN spec:
    ///
    /// ```text
    /// Subject → ... → CurrentAudience → NewAudience
    ///                 (new delegation's iss must match current audience)
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the new delegation's issuer doesn't match the current
    /// chain's audience.
    pub fn extend(&self, delegation: Delegation<Ed25519Did>) -> Result<Self, AuthorizationError> {
        // Verify principal alignment: new delegation's issuer must match current audience
        let current_audience = self.audience();
        let new_issuer = delegation.issuer();
        if new_issuer != current_audience {
            return Err(AuthorizationError::Invocation(format!(
                "Principal alignment error: delegation issuer '{}' does not match chain audience '{}'",
                new_issuer, current_audience
            )));
        }

        let cid = delegation.to_cid();

        let mut delegations = self.delegations.clone();
        delegations.insert(cid, Arc::new(delegation));

        // New delegation goes at the front (closest to invoker)
        let mut proof_cids = vec![cid];
        proof_cids.extend(self.proof_cids.iter().cloned());

        Ok(Self {
            delegations,
            proof_cids,
        })
    }
}

impl TryFrom<Vec<Delegation<Ed25519Did>>> for DelegationChain {
    type Error = AuthorizationError;

    /// Create a delegation chain from a vector of delegations.
    ///
    /// The delegations must be ordered from invoker to subject:
    /// - delegations[0] is closest to invoker (its `aud` is the operator)
    /// - delegations[n-1] is closest to subject (its `iss` is the subject)
    ///
    /// # Principal Alignment
    ///
    /// For each consecutive pair (i, i+1), the issuer of delegation[i] must match
    /// the audience of delegation[i+1]. This ensures a proper chain of authority.
    ///
    /// # Errors
    ///
    /// Returns an error if the vector is empty or if principal alignment fails.
    fn try_from(delegations_vec: Vec<Delegation<Ed25519Did>>) -> Result<Self, Self::Error> {
        if delegations_vec.is_empty() {
            return Err(AuthorizationError::Configuration(
                "DelegationChain requires at least one delegation".to_string(),
            ));
        }

        // Verify principal alignment between consecutive delegations
        for i in 0..delegations_vec.len().saturating_sub(1) {
            let current = &delegations_vec[i];
            let next = &delegations_vec[i + 1];

            // The issuer of current delegation must be the audience of the next delegation
            // (moving from invoker toward subject)
            if current.issuer() != next.audience() {
                return Err(AuthorizationError::Invocation(format!(
                    "Principal alignment error at position {}: delegation issuer '{}' does not match next delegation audience '{}'",
                    i,
                    current.issuer(),
                    next.audience()
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

impl From<Delegation<Ed25519Did>> for DelegationChain {
    fn from(delegation: Delegation<Ed25519Did>) -> Self {
        Self::new(delegation)
    }
}

impl TryFrom<&[u8]> for DelegationChain {
    type Error = AuthorizationError;

    /// Deserialize a delegation chain from DAG-CBOR container format.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let container = Container::from_bytes(bytes)?;
        DelegationChain::try_from(container)
    }
}

impl TryFrom<Container> for DelegationChain {
    type Error = AuthorizationError;

    /// Convert a container to a delegation chain.
    fn try_from(container: Container) -> Result<Self, Self::Error> {
        let token_bytes = container.into_tokens();

        // Deserialize delegations and verify principal alignment
        let mut delegations_vec: Vec<Delegation<Ed25519Did>> =
            Vec::with_capacity(token_bytes.len());
        for (i, bytes) in token_bytes.iter().enumerate() {
            let delegation: Delegation<Ed25519Did> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    AuthorizationError::Invocation(format!(
                        "failed to decode delegation {}: {}",
                        i, e
                    ))
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
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        DelegationChain::try_from(bytes.as_slice()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use ucan::delegation::builder::DelegationBuilder;
    use ucan::delegation::subject::DelegatedSubject;
    use ucan::did::Ed25519Signer;

    /// Generate a new random Ed25519 signer.
    ///
    /// This is useful for creating space signers in tests.
    pub fn generate_signer() -> Ed25519Signer {
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng);
        Ed25519Signer::new(signing_key)
    }

    /// Create a delegation from issuer to audience for a subject with the given command.
    ///
    /// This is a convenience function for building simple delegations in tests.
    pub fn create_delegation(
        issuer: &Ed25519Signer,
        audience: &Ed25519Did,
        subject: &Ed25519Did,
        command: &[&str],
    ) -> Result<Delegation<Ed25519Did>, AuthorizationError> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience.clone())
            .subject(DelegatedSubject::Specific(subject.clone()))
            .command(
                command
                    .iter()
                    .map(|&s| s.to_string()) // or .map(String::from)
                    .collect(),
            )
            .try_build()
            .map_err(|e| {
                AuthorizationError::Invocation(format!("Failed to build delegation: {:?}", e))
            })
    }

    #[test]
    fn it_requires_non_empty_chain() {
        let result = DelegationChain::try_from(vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least one"));
    }

    #[test]
    fn it_creates_chain_from_single_delegation() {
        let space_signer = generate_signer();
        let space_did = space_signer.did().clone();
        let operator_signer = generate_signer();

        let delegation = DelegationBuilder::new()
            .issuer(space_signer)
            .audience(operator_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .unwrap();

        let chain = DelegationChain::new(delegation);
        assert_eq!(chain.proof_cids().len(), 1);
        assert_eq!(chain.delegations().len(), 1);
        assert_eq!(chain.audience(), operator_signer.did());
        assert_eq!(chain.subject(), Some(&space_did));
    }

    #[test]
    fn it_creates_chain_from_vec() {
        let space_signer = generate_signer();
        let space_did = space_signer.did().clone();
        let operator1_signer = generate_signer();
        let operator2_signer = generate_signer();

        // First delegation: space -> operator1
        let delegation1 = DelegationBuilder::new()
            .issuer(space_signer)
            .audience(operator1_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .unwrap();

        // Second delegation: operator1 -> operator2
        let delegation2 = DelegationBuilder::new()
            .issuer(operator1_signer)
            .audience(operator2_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .unwrap();

        // Note: order matters - first in vec is first in proof chain (closest to invoker)
        let chain = DelegationChain::try_from(vec![delegation2, delegation1]).unwrap();
        assert_eq!(chain.proof_cids().len(), 2);
        assert_eq!(chain.delegations().len(), 2);
    }

    #[test]
    fn it_extends_chain_with_new_delegation() {
        // Create initial delegation: space -> operator1
        let space_signer = generate_signer();
        let space_did = space_signer.did().clone();
        let operator1_signer = generate_signer();

        let initial_delegation = DelegationBuilder::new()
            .issuer(space_signer)
            .audience(operator1_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .unwrap();

        let chain = DelegationChain::new(initial_delegation);
        assert_eq!(chain.proof_cids().len(), 1);

        // Extend: operator1 -> operator2
        let operator2_signer = generate_signer();

        let second_delegation = DelegationBuilder::new()
            .issuer(operator1_signer)
            .audience(operator2_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .unwrap();

        let extended_chain = chain.extend(second_delegation).unwrap();

        // Extended chain should have 2 delegations
        assert_eq!(extended_chain.proof_cids().len(), 2);
        assert_eq!(extended_chain.delegations().len(), 2);

        // Original chain should be unchanged
        assert_eq!(chain.proof_cids().len(), 1);
    }

    #[test]
    fn it_fails_extend_on_principal_misalignment() {
        // Create initial delegation: space -> operator1
        let space_signer = generate_signer();
        let space_did = space_signer.did().clone();
        let operator1_signer = generate_signer();

        let initial_delegation = DelegationBuilder::new()
            .issuer(space_signer)
            .audience(operator1_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .unwrap();

        let chain = DelegationChain::new(initial_delegation);

        // Try to extend with wrong issuer (operator2 instead of operator1)
        let operator2_signer = generate_signer();
        let operator3_signer = generate_signer();

        let bad_delegation = DelegationBuilder::new()
            .issuer(operator2_signer) // Wrong! Should be operator1
            .audience(operator3_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .unwrap();

        let result = chain.extend(bad_delegation);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Principal alignment error")
        );
    }

    #[test]
    fn it_fails_try_from_on_principal_misalignment() {
        // Create delegations that don't align
        let space_signer = generate_signer();
        let space_did = space_signer.did().clone();
        let operator1_signer = generate_signer();
        let operator2_signer = generate_signer();
        let operator3_signer = generate_signer();

        // First delegation: operator2 -> operator3 (closest to invoker)
        let delegation1 = DelegationBuilder::new()
            .issuer(operator2_signer.clone())
            .audience(operator3_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string(), "get".to_string()])
            .try_build()
            .unwrap();

        // Second delegation: space -> operator1 (closest to subject)
        // This should fail because operator2 != operator1
        let delegation2 = DelegationBuilder::new()
            .issuer(space_signer)
            .audience(operator1_signer.did().clone()) // Wrong! Should be operator2
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
            .unwrap();

        // Order: [delegation1, delegation2] means delegation1.issuer should == delegation2.audience
        // But operator2 != operator1, so this should fail
        let result = DelegationChain::try_from(vec![delegation1, delegation2]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Principal alignment error")
        );
    }

    #[test]
    fn it_serializes_and_deserializes_roundtrip() {
        let space_signer = generate_signer();
        let space_did = space_signer.did().clone();
        let operator_signer = generate_signer();

        let delegation = DelegationBuilder::new()
            .issuer(space_signer)
            .audience(operator_signer.did().clone())
            .subject(DelegatedSubject::Specific(space_did.clone()))
            .command(vec!["storage".to_string()])
            .try_build()
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
}
