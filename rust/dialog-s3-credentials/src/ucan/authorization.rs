//! UCAN authorization proof management.
//!
//! This module provides [`UcanAuthorization`], which represents a proof of authority
//! for a specific capability claim using UCAN delegations.

use super::authority::UcanAuthority;
use super::delegation::DelegationChain;
use dialog_common::capability::Ability;
use ucan::delegation::builder::DelegationBuilder;
use ucan::delegation::subject::DelegatedSubject;
use ucan::did::{Ed25519Did, Ed25519Signer};

/// UCAN-based authorization proof for a capability.
///
/// This struct holds the proof of authority (as a serialized UCAN invocation chain)
/// for a specific capability claim.
///
/// The delegation chain is `None` for self-issued authorizations where the subject
/// is directly authorizing without any delegation.
#[derive(Debug, Clone)]
pub struct UcanAuthorization<C: Ability> {
    /// The capability claim (capability + audience).
    claim: dialog_common::capability::Claim<C>,
    /// The delegation chain providing proof of authority.
    /// `None` for self-issued authorizations where subject == audience.
    chain: Option<DelegationChain>,
}

impl<C: Ability + Clone> UcanAuthorization<C> {
    /// Create a new UCAN authorization from a claim and delegation chain.
    pub fn new(claim: dialog_common::capability::Claim<C>, chain: DelegationChain) -> Self {
        Self {
            claim,
            chain: Some(chain),
        }
    }

    /// Create a self-issued UCAN authorization (no delegation needed).
    ///
    /// This is used when the subject is directly authorizing themselves.
    pub fn self_issued(claim: dialog_common::capability::Claim<C>) -> Self {
        Self { claim, chain: None }
    }

    /// Get the delegation chain, if present.
    pub fn chain(&self) -> Option<&DelegationChain> {
        self.chain.as_ref()
    }

    /// Delegate this authorization to another party using UCAN.
    ///
    /// This creates a real UCAN delegation from the issuer to the audience,
    /// extending the delegation chain. The issuer must be the current audience
    /// of this authorization.
    ///
    /// # Arguments
    ///
    /// * `audience` - The DID of the party to delegate to
    /// * `issuer` - The UCAN authority of the current holder (must match claim.audience)
    /// * `command` - The command path to delegate (e.g., ["storage", "get"])
    ///
    /// # Returns
    ///
    /// A new `UcanAuthorization` for the audience with an extended delegation chain.
    pub fn delegate_ucan(
        &self,
        audience: &Ed25519Did,
        issuer: &UcanAuthority,
        command: Vec<String>,
    ) -> Result<Self, dialog_common::capability::AuthorizationError> {
        // Check that issuer is the current audience
        let issuer_did = issuer.did();
        let issuer_did_str = issuer_did.to_string();
        if self.claim.audience() != &issuer_did_str {
            return Err(dialog_common::capability::AuthorizationError::NotAudience {
                audience: self.claim.audience().clone(),
                issuer: issuer_did_str,
            });
        }

        // Parse the subject DID for the delegation
        let subject_did: Ed25519Did = self.claim.subject().parse().map_err(|e| {
            dialog_common::capability::AuthorizationError::Serialization(format!(
                "Invalid subject DID: {:?}",
                e
            ))
        })?;

        // Build the UCAN delegation
        let delegation = DelegationBuilder::new()
            .issuer(issuer.signer().clone())
            .audience(audience.clone())
            .subject(DelegatedSubject::Specific(subject_did))
            .command(command)
            .try_build()
            .map_err(|e| {
                dialog_common::capability::AuthorizationError::Serialization(format!(
                    "Failed to build delegation: {:?}",
                    e
                ))
            })?;

        // Either extend existing chain or create new one
        let new_chain = match &self.chain {
            Some(existing) => existing.extend(delegation).map_err(|e| {
                dialog_common::capability::AuthorizationError::Serialization(format!(
                    "Failed to extend delegation chain: {}",
                    e
                ))
            })?,
            None => {
                // Self-issued authorization: this is the first delegation
                DelegationChain::new(delegation)
            }
        };

        // Create new claim for the audience
        let new_claim = dialog_common::capability::Claim::new(
            self.claim.capability.clone(),
            audience.to_string(),
        );

        Ok(Self {
            claim: new_claim,
            chain: Some(new_chain),
        })
    }
}

impl<C: Ability + Clone + Send + 'static> dialog_common::capability::Authorization<C>
    for UcanAuthorization<C>
{
    fn claim(&self) -> &dialog_common::capability::Claim<C> {
        &self.claim
    }

    fn proof(&self) -> dialog_common::capability::Proof {
        // Serialize the delegation chain as proof bytes.
        // The proof format is the list of delegation CIDs followed by the serialized delegations.
        let mut proof_bytes = Vec::new();

        // Encode proof CIDs if chain exists
        if let Some(chain) = &self.chain {
            for cid in chain.proof_cids() {
                proof_bytes.extend_from_slice(&cid.to_bytes());
            }
        }

        dialog_common::capability::Proof::new(proof_bytes)
    }

    fn issue<A: dialog_common::capability::Authority>(
        capability: C,
        issuer: &A,
    ) -> Result<Self, dialog_common::capability::AuthorizationError> {
        // Self-issue: subject must equal issuer's DID
        let subject = capability.subject();
        let issuer_did = issuer.did();

        if subject != issuer_did {
            return Err(dialog_common::capability::AuthorizationError::NotOwner {
                subject: subject.clone(),
                issuer: issuer_did.clone(),
            });
        }

        // For self-issued authorization, the claim audience is the issuer,
        // and there's no delegation chain needed (None).
        let claim = dialog_common::capability::Claim::new(capability, issuer_did.clone());

        Ok(Self { claim, chain: None })
    }

    fn delegate<A: dialog_common::capability::Authority>(
        &self,
        audience: &dialog_common::capability::Did,
        issuer: &A,
    ) -> Result<
        dialog_common::capability::Delegation<C, Self>,
        dialog_common::capability::AuthorizationError,
    >
    where
        C: Clone,
        Self: Clone,
    {
        // Get the secret key bytes from the issuer
        let secret_bytes = issuer.secret_key_bytes().ok_or_else(|| {
            dialog_common::capability::AuthorizationError::Serialization(
                "Authority does not support Ed25519 key export for UCAN delegation".to_string(),
            )
        })?;

        // Reconstruct the signing key
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
        let signer = Ed25519Signer::new(signing_key);

        // Check that issuer is the current audience
        let issuer_did_str = signer.did().to_string();
        if self.claim.audience() != &issuer_did_str {
            return Err(dialog_common::capability::AuthorizationError::NotAudience {
                audience: self.claim.audience().clone(),
                issuer: issuer_did_str,
            });
        }

        // Parse the audience DID
        let audience_did: Ed25519Did = audience.parse().map_err(|e| {
            dialog_common::capability::AuthorizationError::Serialization(format!(
                "Invalid audience DID: {:?}",
                e
            ))
        })?;

        // Parse the subject DID for the delegation
        let subject_did: Ed25519Did = self.claim.subject().parse().map_err(|e| {
            dialog_common::capability::AuthorizationError::Serialization(format!(
                "Invalid subject DID: {:?}",
                e
            ))
        })?;

        // Build the command from the capability
        // The capability command is a string like "/storage/get", convert to Vec<String>
        let command_str = self.claim.capability.command();
        let command: Vec<String> = command_str
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        // Build the UCAN delegation
        let delegation = DelegationBuilder::new()
            .issuer(signer)
            .audience(audience_did.clone())
            .subject(DelegatedSubject::Specific(subject_did))
            .command(command)
            .try_build()
            .map_err(|e| {
                dialog_common::capability::AuthorizationError::Serialization(format!(
                    "Failed to build delegation: {:?}",
                    e
                ))
            })?;

        // Either extend existing chain or create new one
        let new_chain = match &self.chain {
            Some(existing) => existing.extend(delegation).map_err(|e| {
                dialog_common::capability::AuthorizationError::Serialization(format!(
                    "Failed to extend delegation chain: {}",
                    e
                ))
            })?,
            None => {
                // Self-issued authorization: this is the first delegation
                DelegationChain::new(delegation)
            }
        };

        // Create new claim for the audience
        let new_claim = dialog_common::capability::Claim::new(
            self.claim.capability.clone(),
            audience_did.to_string(),
        );

        let new_auth = Self {
            claim: new_claim,
            chain: Some(new_chain),
        };

        Ok(dialog_common::capability::Delegation::new(
            issuer_did_str,
            audience_did.to_string(),
            self.claim.capability.clone(),
            new_auth,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_common::capability::{Ability, Authorization};

    /// A simple test capability for testing delegation.
    #[derive(Debug, Clone)]
    struct TestCapability {
        subject: String,
    }

    impl Ability for TestCapability {
        fn subject(&self) -> &dialog_common::capability::Did {
            &self.subject
        }

        fn command(&self) -> String {
            "/storage/get".to_string()
        }
    }

    #[test]
    fn it_delegates_using_authority_trait() {
        // Create subject authority
        let subject_authority = UcanAuthority::from_secret(&[1u8; 32]);
        let subject_did = subject_authority.did().to_string();

        // Create a capability for the subject
        let capability = TestCapability {
            subject: subject_did.clone(),
        };

        // Issue authorization from subject
        let auth = UcanAuthorization::issue(capability.clone(), &subject_authority)
            .expect("Failed to issue authorization");

        // Create operator authority to delegate to
        let operator_authority = UcanAuthority::from_secret(&[2u8; 32]);
        let operator_did = operator_authority.did().to_string();

        // Delegate using the generic Authority trait
        let delegation = auth
            .delegate(&operator_did, &subject_authority)
            .expect("Failed to delegate");

        // Verify delegation
        assert_eq!(delegation.issuer(), &subject_did);
        assert_eq!(delegation.audience(), &operator_did);

        // The new authorization should have a delegation chain
        let new_auth = delegation.authorization();
        assert!(new_auth.chain().is_some());
        assert_eq!(new_auth.chain().unwrap().proof_cids().len(), 1);
    }

    #[test]
    fn it_fails_delegation_when_issuer_is_not_audience() {
        // Create subject authority
        let subject_authority = UcanAuthority::from_secret(&[1u8; 32]);
        let subject_did = subject_authority.did().to_string();

        // Create a different authority (not the subject)
        let other_authority = UcanAuthority::from_secret(&[3u8; 32]);

        // Create a capability for the subject
        let capability = TestCapability {
            subject: subject_did.clone(),
        };

        // Issue authorization from subject
        let auth = UcanAuthorization::issue(capability, &subject_authority)
            .expect("Failed to issue authorization");

        // Try to delegate using wrong authority (not the current audience)
        let operator_authority = UcanAuthority::from_secret(&[2u8; 32]);
        let operator_did = operator_authority.did().to_string();

        let result = auth.delegate(&operator_did, &other_authority);

        assert!(result.is_err());
        match result.unwrap_err() {
            dialog_common::capability::AuthorizationError::NotAudience { .. } => {}
            e => panic!("Expected NotAudience error, got: {:?}", e),
        }
    }

    #[test]
    fn it_chains_multiple_delegations() {
        // Create subject authority
        let subject_authority = UcanAuthority::from_secret(&[1u8; 32]);
        let subject_did = subject_authority.did().to_string();

        // Create a capability for the subject
        let capability = TestCapability {
            subject: subject_did.clone(),
        };

        // Issue authorization from subject
        let auth = UcanAuthorization::issue(capability, &subject_authority)
            .expect("Failed to issue authorization");

        // First delegation: subject -> operator1
        let operator1_authority = UcanAuthority::from_secret(&[2u8; 32]);
        let operator1_did = operator1_authority.did().to_string();

        let delegation1 = auth
            .delegate(&operator1_did, &subject_authority)
            .expect("Failed to first delegate");

        // Second delegation: operator1 -> operator2
        let operator2_authority = UcanAuthority::from_secret(&[3u8; 32]);
        let operator2_did = operator2_authority.did().to_string();

        let delegation2 = delegation1
            .authorization()
            .delegate(&operator2_did, &operator1_authority)
            .expect("Failed to second delegate");

        // The final authorization should have a chain of 2 delegations
        let final_auth = delegation2.authorization();
        assert!(final_auth.chain().is_some());
        assert_eq!(final_auth.chain().unwrap().proof_cids().len(), 2);
    }
}
