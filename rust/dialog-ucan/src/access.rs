//! UCAN Protocol implementation.
//!
//! Implements [`Protocol`](dialog_capability::access::Protocol) for [`Ucan`],
//! defining the UCAN-specific proof, permit, and authorization types.

use dialog_capability::Did;
use dialog_capability::access::{self, AuthorizeError};
use dialog_credentials::Ed25519Signer;
use dialog_ucan_core::DelegationChain;
use dialog_varsig::eddsa::Ed25519Signature;

use super::Ucan;
use super::scope::Scope;

/// A single UCAN delegation — one proof link in a chain.
///
/// Implements [`Delegation`](access::Delegation) for generic chain verification.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct UcanCertificate(pub dialog_ucan_core::Delegation<Ed25519Signature>);

impl access::Certificate for UcanCertificate {
    type Access = Scope;

    fn issuer(&self) -> &Did {
        self.0.issuer()
    }

    fn audience(&self) -> &Did {
        self.0.audience()
    }

    fn subject(&self) -> Option<&Did> {
        use dialog_ucan_core::subject::Subject as UcanSubject;
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
pub struct UcanProof {
    /// The collected proofs (individual delegations).
    pub proofs: Vec<UcanCertificate>,
    /// The scope of access being authorized.
    pub scope: Scope,
    /// The time range this proof covers.
    pub duration: access::TimeRange,
}

impl UcanProof {
    /// Build a permit from a delegation chain and scope.
    ///
    /// Used when importing externally-built delegation chains.
    pub fn from_chain(chain: &DelegationChain, scope: Scope) -> Self {
        let proofs = chain
            .delegations()
            .values()
            .map(|d| UcanCertificate(d.as_ref().clone()))
            .collect();
        let duration = access::TimeRange {
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

impl access::Proof<Ucan> for UcanProof {
    fn new(access: Scope) -> Self {
        Self {
            proofs: Vec::new(),
            scope: access,
            duration: access::TimeRange::unbounded(),
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

    fn duration(&self) -> &access::TimeRange {
        &self.duration
    }

    fn set_duration(&mut self, duration: access::TimeRange) {
        self.duration = duration;
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
    pub duration: access::TimeRange,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl access::Authorization<Ucan> for UcanAuthorization {
    fn duration(&self) -> &access::TimeRange {
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
        use dialog_ucan_core::delegation::builder::DelegationBuilder;
        use dialog_ucan_core::time::Timestamp;
        use dialog_ucan_core::time::timestamp::{Duration, UNIX_EPOCH};

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

    async fn invoke(&self) -> Result<super::UcanInvocation, AuthorizeError> {
        use dialog_capability::ANY_SUBJECT;
        use dialog_ucan_core::InvocationBuilder;
        use dialog_ucan_core::subject::Subject as UcanSubject;

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

        let chain = dialog_ucan_core::InvocationChain::new(invocation, delegations_map);

        Ok(super::UcanInvocation {
            chain: Box::new(chain),
            subject: subject_did,
            ability,
        })
    }
}

/// A UCAN delegation bundle — wraps [`DelegationChain`] to implement [`Delegation`](access::Delegation).
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

impl access::Delegation for UcanDelegation {
    type Certificate = UcanCertificate;

    fn certificates(&self) -> Vec<UcanCertificate> {
        self.0
            .delegations()
            .values()
            .map(|d| UcanCertificate(d.as_ref().clone()))
            .collect()
    }
}

impl access::Protocol for Ucan {
    type Access = Scope;
    type Signer = Ed25519Signer;
    type Certificate = UcanCertificate;
    type Delegation = UcanDelegation;
    type Invocation = super::UcanInvocation;
    type Proof = UcanProof;
    type Authorization = UcanAuthorization;
}
