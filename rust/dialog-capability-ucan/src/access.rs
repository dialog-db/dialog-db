//! UCAN Protocol implementation.
//!
//! Implements [`Protocol`](dialog_capability::access::Protocol) for [`Ucan`],
//! defining the UCAN-specific proof, permit, and authorization types.

use dialog_capability::Did;
use dialog_capability::access::{self, AuthorizeError};
use dialog_credentials::Ed25519Signer;
use dialog_ucan::DelegationChain;
use dialog_varsig::eddsa::Ed25519Signature;

use super::Ucan;
use super::scope::Scope;

/// A single UCAN delegation — one proof link in a chain.
///
/// Implements [`Delegation`](access::Delegation) for generic chain verification.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct UcanProof(pub dialog_ucan::Delegation<Ed25519Signature>);

impl access::Delegation for UcanProof {
    type Access = Scope;

    fn issuer(&self) -> &Did {
        self.0.issuer()
    }

    fn audience(&self) -> &Did {
        self.0.audience()
    }

    fn subject(&self) -> Option<&Did> {
        use dialog_ucan::subject::Subject as UcanSubject;
        match self.0.subject() {
            UcanSubject::Specific(did) => Some(did),
            UcanSubject::Any => None,
        }
    }

    fn verify(&self, access: &Scope) -> Result<access::TimeRange, AuthorizeError> {
        // Command attenuation: delegation command must be a prefix of requested command
        if !access.command.starts_with(self.0.command()) {
            return Err(AuthorizeError::Denied(format!(
                "command '{}' not covered by delegation '{}'",
                access.command,
                self.0.command()
            )));
        }

        // Policy predicates: all must pass against the access parameters
        let args = ipld_core::ipld::Ipld::Map(access.parameters.as_map().clone());
        let all_pass = self
            .0
            .policy()
            .iter()
            .all(|pred| pred.clone().run(&args).unwrap_or(false));

        if !all_pass {
            return Err(AuthorizeError::Denied(
                "policy predicates not satisfied".into(),
            ));
        }

        Ok(access::TimeRange {
            not_before: self.0.not_before().map(|t| t.to_unix()),
            expiration: self.0.expiration().map(|t| t.to_unix()),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, AuthorizeError> {
        serde_ipld_dagcbor::to_vec(&self.0)
            .map_err(|e| AuthorizeError::Configuration(format!("Failed to encode proof: {e}")))
    }

    fn decode(bytes: &[u8]) -> Result<Self, AuthorizeError> {
        serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|e| AuthorizeError::Configuration(format!("Failed to decode proof: {e}")))
    }
}

/// Verified UCAN permit — delegation chain without a signer.
///
/// Built incrementally: create with `new(scope)`, push proofs
/// as the chain is walked, then `claim(signer)` to authorize.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct UcanPermit {
    /// The collected proofs (individual delegations).
    pub proofs: Vec<UcanProof>,
    /// The scope of access being authorized.
    pub scope: Scope,
}

impl UcanPermit {
    /// Build a permit from a delegation chain and scope.
    ///
    /// Used when importing externally-built delegation chains.
    pub fn from_chain(chain: &DelegationChain, scope: Scope) -> Self {
        let proofs = chain
            .delegations()
            .values()
            .map(|d| UcanProof(d.as_ref().clone()))
            .collect();
        Self { proofs, scope }
    }
}

impl access::ProofChain<Ucan> for UcanPermit {
    fn new(access: Scope) -> Self {
        Self {
            proofs: Vec::new(),
            scope: access,
        }
    }

    fn access(&self) -> &Scope {
        &self.scope
    }

    fn push(&mut self, proof: UcanProof) {
        self.proofs.push(proof);
    }

    fn proofs(&self) -> &[UcanProof] {
        &self.proofs
    }

    fn claim(self, signer: Ed25519Signer) -> Result<UcanAuthorization, AuthorizeError> {
        let chain = if self.proofs.is_empty() {
            None
        } else {
            let mut iter = self.proofs.into_iter();
            let first = iter.next().expect("non-empty proofs").0;
            let mut chain = DelegationChain::new(first);
            for proof in iter {
                chain = chain
                    .push(proof.0)
                    .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
            }
            Some(chain)
        };

        Ok(UcanAuthorization {
            chain,
            signer,
            scope: self.scope,
        })
    }
}

/// Full UCAN authorization — can delegate and invoke.
///
/// Created by [`UcanPermit::claim`]. Holds the verified delegation
/// chain, signer, and scope.
pub struct UcanAuthorization {
    /// The delegation chain proving authority (None if self-authorized).
    pub chain: Option<DelegationChain>,
    /// The signer (operator key).
    pub signer: Ed25519Signer,
    /// The scope of the capability being authorized.
    pub scope: Scope,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl access::Authorization<Ucan> for UcanAuthorization {
    async fn delegate(&self, audience: Did) -> Result<DelegationChain, AuthorizeError> {
        use dialog_ucan::delegation::builder::DelegationBuilder;

        let delegation = DelegationBuilder::new()
            .issuer(self.signer.clone())
            .audience(&audience)
            .subject(self.scope.subject.clone())
            .command(self.scope.command.segments().clone())
            .policy(self.scope.policy())
            .try_build()
            .await
            .map_err(|e| AuthorizeError::Configuration(format!("{e:?}")))?;

        match &self.chain {
            Some(chain) => chain
                .push(delegation)
                .map_err(|e| AuthorizeError::Configuration(format!("{e}"))),
            None => Ok(DelegationChain::new(delegation)),
        }
    }

    async fn invoke(&self) -> Result<dialog_capability::ucan::UcanInvocation, AuthorizeError> {
        use dialog_capability::ANY_SUBJECT;
        use dialog_ucan::InvocationBuilder;
        use dialog_ucan::subject::Subject as UcanSubject;

        let subject_did = match &self.scope.subject {
            UcanSubject::Specific(did) => did.clone(),
            UcanSubject::Any => ANY_SUBJECT.parse().expect("valid DID"),
        };

        let command: Vec<String> = self.scope.command.segments().clone();
        let args = self.scope.parameters.args();

        let (proofs, delegations_map) = match &self.chain {
            Some(chain) => (chain.proof_cids().into(), chain.delegations().clone()),
            None => (vec![], Default::default()),
        };

        let ability = if command.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", command.join("/"))
        };

        let invocation = InvocationBuilder::new()
            .issuer(self.signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(command)
            .arguments(args)
            .proofs(proofs)
            .try_build()
            .await
            .map_err(|e| AuthorizeError::Denied(format!("{e:?}")))?;

        let chain = dialog_ucan::InvocationChain::new(invocation, delegations_map);

        Ok(dialog_capability::ucan::UcanInvocation {
            chain: Box::new(chain),
            subject: subject_did,
            ability,
        })
    }
}

impl access::Protocol for Ucan {
    type Access = Scope;
    type Signer = Ed25519Signer;
    type Proof = UcanProof;
    type Delegation = DelegationChain;
    type Invocation = dialog_capability::ucan::UcanInvocation;
    type ProofChain = UcanPermit;
    type Authorization = UcanAuthorization;

    fn proofs(delegation: &DelegationChain) -> Vec<UcanProof> {
        delegation
            .delegations()
            .values()
            .map(|d| UcanProof(d.as_ref().clone()))
            .collect()
    }
}
