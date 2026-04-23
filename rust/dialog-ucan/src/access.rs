//! UCAN Protocol implementation.
//!
//! Implements [`Protocol`](dialog_capability::access::Protocol) for [`Ucan`],
//! defining the UCAN-specific proof, permit, and authorization types.

use dialog_capability::Did;
use dialog_capability::access::{
    Authorization, AuthorizeError, Certificate, Delegation as AccessDelegation, Proof, Protocol,
    TimeRange,
};
use dialog_credentials::Ed25519Signer;
use dialog_ucan_core::delegation::builder::DelegationBuilder;
use dialog_ucan_core::subject::Subject as UcanSubject;
use dialog_ucan_core::time::Timestamp;
use dialog_ucan_core::time::timestamp::{Duration, UNIX_EPOCH};
use dialog_ucan_core::{Delegation, DelegationChain, InvocationBuilder, InvocationChain};
use dialog_varsig::eddsa::Ed25519Signature;

use super::scope::Scope;
use super::{Ucan, UcanInvocation};

/// A single UCAN delegation — one proof link in a chain.
///
/// Implements [`Certificate`] for generic chain verification.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct UcanCertificate(pub Delegation<Ed25519Signature>);

impl Certificate for UcanCertificate {
    type Access = Scope;

    fn issuer(&self) -> &Did {
        self.0.issuer()
    }

    fn audience(&self) -> &Did {
        self.0.audience()
    }

    fn subject(&self) -> Option<&Did> {
        match self.0.subject() {
            UcanSubject::Specific(did) => Some(did),
            UcanSubject::Any => None,
        }
    }

    fn verify(&self, access: &Scope) -> Result<TimeRange, AuthorizeError> {
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

        Ok(TimeRange {
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
pub struct UcanProof {
    /// The collected proofs (individual delegations).
    pub proofs: Vec<UcanCertificate>,
    /// The scope of access being authorized.
    pub scope: Scope,
    /// The time range this proof covers.
    pub duration: TimeRange,
}

impl UcanProof {
    /// Build a permit from a delegation chain and scope.
    ///
    /// Used when importing externally-built delegation chains.
    pub fn from_chain(chain: &DelegationChain, scope: Scope) -> Self {
        let proofs = chain.proofs().map(|d| UcanCertificate(d.clone())).collect();
        let duration = TimeRange {
            not_before: chain.not_before().map(|t| t.to_unix()),
            expiration: chain.expiration().map(|t| t.to_unix()),
        };
        Self {
            proofs,
            scope,
            duration,
        }
    }
}

impl Proof<Ucan> for UcanProof {
    fn new(access: Scope) -> Self {
        Self {
            proofs: Vec::new(),
            scope: access,
            duration: TimeRange::unbounded(),
        }
    }

    fn access(&self) -> &Scope {
        &self.scope
    }

    fn push(&mut self, proof: UcanCertificate) {
        self.proofs.push(proof);
    }

    fn proofs(&self) -> &[UcanCertificate] {
        &self.proofs
    }

    fn duration(&self) -> &TimeRange {
        &self.duration
    }

    fn set_duration(&mut self, duration: TimeRange) {
        self.duration = duration;
    }

    fn claim(self, signer: Ed25519Signer) -> Result<UcanAuthorization, AuthorizeError> {
        let mut iter = self.proofs.into_iter();
        let chain = match iter.next() {
            None => None,
            Some(first) => {
                let mut chain = DelegationChain::new(first.0);
                for proof in iter {
                    chain = chain
                        .push(proof.0)
                        .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
                }
                Some(chain)
            }
        };

        Ok(UcanAuthorization {
            chain,
            signer,
            scope: self.scope,
            duration: self.duration,
        })
    }
}

/// Full UCAN authorization — can delegate and invoke.
///
/// Created by [`UcanProof::claim`]. Holds the verified delegation
/// chain, signer, and scope.
pub struct UcanAuthorization {
    /// The delegation chain proving authority (None if self-authorized).
    pub chain: Option<DelegationChain>,
    /// The signer (operator key).
    pub signer: Ed25519Signer,
    /// The scope of the capability being authorized.
    pub scope: Scope,
    /// The time range this authorization is valid for.
    pub duration: TimeRange,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Authorization<Ucan> for UcanAuthorization {
    fn duration(&self) -> &TimeRange {
        &self.duration
    }

    fn not_before(mut self, timestamp: u64) -> Result<Self, AuthorizeError> {
        if let Some(nbf) = self.duration.not_before
            && timestamp < nbf
        {
            return Err(AuthorizeError::Denied(format!(
                "cannot set not_before to {timestamp}, proof is not valid before {nbf}"
            )));
        }
        self.duration.not_before = Some(timestamp);
        Ok(self)
    }

    fn expires(mut self, timestamp: u64) -> Result<Self, AuthorizeError> {
        if let Some(exp) = self.duration.expiration
            && timestamp > exp
        {
            return Err(AuthorizeError::Denied(format!(
                "cannot set expiration to {timestamp}, proof expires at {exp}"
            )));
        }
        self.duration.expiration = Some(timestamp);
        Ok(self)
    }

    async fn delegate(&self, audience: Did) -> Result<UcanDelegation, AuthorizeError> {
        let mut builder = DelegationBuilder::new()
            .issuer(self.signer.clone())
            .audience(&audience)
            .subject(self.scope.subject.clone())
            .command(self.scope.command.segments().clone())
            .policy(self.scope.policy());

        if let Some(exp) = self.duration.expiration
            && let Ok(ts) = Timestamp::new(UNIX_EPOCH + Duration::from_secs(exp))
        {
            builder = builder.expiration(ts);
        }
        if let Some(nbf) = self.duration.not_before
            && let Ok(ts) = Timestamp::new(UNIX_EPOCH + Duration::from_secs(nbf))
        {
            builder = builder.not_before(ts);
        }

        let delegation = builder
            .try_build()
            .await
            .map_err(|e| AuthorizeError::Configuration(format!("{e:?}")))?;

        let chain = match &self.chain {
            Some(chain) => chain
                .push(delegation)
                .map_err(|e| AuthorizeError::Configuration(format!("{e}")))?,
            None => DelegationChain::new(delegation),
        };

        Ok(UcanDelegation::from(chain))
    }

    async fn invoke(&self) -> Result<UcanInvocation, AuthorizeError> {
        let subject_did = match &self.scope.subject {
            UcanSubject::Specific(did) => did.clone(),
            UcanSubject::Any => match dialog_capability::ANY_SUBJECT.parse() {
                Ok(did) => did,
                Err(_) => {
                    unreachable!("ANY_SUBJECT is a fixed compile-time constant DID and must parse")
                }
            },
        };

        let command: Vec<String> = self.scope.command.segments().clone();
        let args = self.scope.parameters.args();

        let (proofs, delegations_map) = match &self.chain {
            Some(chain) => (chain.proof_cids().into(), chain.export().collect()),
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

        let chain = InvocationChain::new(invocation, delegations_map);

        Ok(UcanInvocation {
            chain: Box::new(chain),
            subject: subject_did,
            ability,
        })
    }
}

/// A UCAN delegation bundle — wraps [`DelegationChain`] to implement
/// [`Delegation`](dialog_capability::access::Delegation).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct UcanDelegation(pub DelegationChain);

impl UcanDelegation {
    /// Create a new delegation from a chain.
    pub fn new(chain: DelegationChain) -> Self {
        Self(chain)
    }

    /// The inner delegation chain.
    pub fn chain(&self) -> &DelegationChain {
        &self.0
    }

    /// Consume and return the inner delegation chain.
    pub fn into_chain(self) -> DelegationChain {
        self.0
    }
}

impl From<DelegationChain> for UcanDelegation {
    fn from(chain: DelegationChain) -> Self {
        Self(chain)
    }
}

impl AccessDelegation for UcanDelegation {
    type Certificate = UcanCertificate;

    fn certificates(&self) -> Vec<UcanCertificate> {
        self.0
            .proofs()
            .map(|d| UcanCertificate(d.clone()))
            .collect()
    }
}

impl Protocol for Ucan {
    type Access = Scope;
    type Signer = Ed25519Signer;
    type Certificate = UcanCertificate;
    type Delegation = UcanDelegation;
    type Invocation = UcanInvocation;
    type Proof = UcanProof;
    type Authorization = UcanAuthorization;
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_varsig::Principal;

    async fn signer(seed: u8) -> Ed25519Signer {
        Ed25519Signer::import(&[seed; 32]).await.unwrap()
    }

    async fn build_delegation(
        issuer: Ed25519Signer,
        audience: &Did,
        subject: &Did,
    ) -> Delegation<Ed25519Signature> {
        DelegationBuilder::new()
            .issuer(issuer)
            .audience(audience)
            .subject(UcanSubject::Specific(subject.clone()))
            .command(vec![])
            .try_build()
            .await
            .unwrap()
    }

    /// `UcanProof::from_chain` must yield proofs in root-to-leaf
    /// order. Before `DelegationChain::proofs` existed, callers
    /// iterated `chain.delegations().values()` — a `HashMap` walk in
    /// unspecified order — so for multi-hop chains `Proof::claim`
    /// would intermittently fail with a principal alignment error
    /// when `chain.push` saw a delegation whose issuer didn't match
    /// the current chain's audience.
    #[dialog_common::test]
    async fn from_chain_preserves_root_to_leaf_order_for_multi_hop_chains() {
        // root -> mid -> leaf, all scoped to `subject`.
        let root = signer(1).await;
        let mid = signer(2).await;
        let leaf_did = signer(3).await.did();
        let subject_did = signer(9).await.did();

        let mid_did = mid.did();
        let first = build_delegation(root, &mid_did, &subject_did).await;
        let chain = DelegationChain::new(first);
        let second = build_delegation(mid, &leaf_did, &subject_did).await;
        let chain = chain.push(second).unwrap();
        assert_eq!(chain.proof_cids().len(), 2);

        // Run repeatedly — `HashMap` iteration order can vary across
        // process boots; building the proof and claiming it from the
        // same process is deterministic, but repeated runs under the
        // same seed would surface regressions reintroducing the
        // iteration-order dependency.
        for _ in 0..8 {
            let scope = crate::Scope::from_chain(&chain);
            let proof = UcanProof::from_chain(&chain, scope);
            assert_eq!(proof.proofs.len(), 2);
            // The leaf signer is allowed to claim — that exercises
            // `Proof::claim` which rebuilds the chain via
            // `DelegationChain::new` + `push`, rejecting misordered
            // proofs.
            let leaf_signer = signer(3).await;
            Proof::claim(proof, leaf_signer).expect("chain must rebuild in root-to-leaf order");
        }
    }
}
