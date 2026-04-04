//! Access — authorization for capability execution.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (profile DID)
//! └── Permit
//!     ├── Claim { access, by, time } → ProofChain
//!     └── Save { permit: ProofChain } → ()
//! ```
//!
//! # Authorization Flow
//!
//! 1. `Subject.attenuate(Permit).invoke(Authorize { .. }).perform(&store)`
//!    returns a [`ProofChain`] (type-erased proof, no signer).
//! 2. `proof_chain.claim(signer)` binds a signer to produce an
//!    [`Authorization`] that can `delegate()` and `invoke()`.

use crate::Did;
use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Describes the scope of access being requested or granted.
///
/// Lighter than [`Ability`](crate::Ability) — only requires the subject DID.
/// Protocol-specific scope types add ability paths, parameters, etc.
pub trait Scope {
    /// The subject (resource) this scope applies to.
    fn subject(&self) -> &Did;
}

/// The time range during which a delegation is valid.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    /// Earliest time this delegation is valid.
    pub not_before: Option<u64>,
    /// When this delegation expires.
    pub expiration: Option<u64>,
}

impl TimeRange {
    /// Check whether the given time falls within this range.
    pub fn contains(&self, time: u64) -> bool {
        if let Some(nbf) = self.not_before {
            if time < nbf {
                return false;
            }
        }
        if let Some(exp) = self.expiration {
            if time >= exp {
                return false;
            }
        }
        true
    }
}

/// An individual delegation record — a single proof link in a chain.
///
/// Each delegation links an issuer to an audience. The [`verify`](Delegation::verify)
/// method checks whether the delegation covers the requested access and
/// returns the time range during which it is valid.
pub trait Delegation {
    /// The access type this delegation verifies against.
    type Access: Scope;

    /// Who issued (signed) this delegation.
    fn issuer(&self) -> &Did;

    /// Who receives the delegated authority.
    fn audience(&self) -> &Did;

    /// The subject this delegation applies to.
    ///
    /// `None` means a powerline delegation — grants access to any subject.
    fn subject(&self) -> Option<&Did>;

    /// Verify this delegation grants the requested access.
    ///
    /// Returns the time range during which the delegation is valid.
    /// Errors if the delegation does not cover the requested access
    /// (wrong command, policy mismatch, etc.).
    fn verify(&self, access: &Self::Access) -> Result<TimeRange, AuthorizeError>;

    /// Encode this delegation to bytes for storage.
    fn encode(&self) -> Result<Vec<u8>, AuthorizeError>;

    /// Decode a delegation from stored bytes.
    fn decode(bytes: &[u8]) -> Result<Self, AuthorizeError>
    where
        Self: Sized;
}

/// A verified proof chain — type-erased proof of authorization without a signer.
///
/// Built incrementally by the store: create with [`new`](ProofChain::new),
/// then [`push`](ProofChain::push) proofs as the chain is walked.
/// Finally, [`claim`](ProofChain::claim) binds a signer to produce a full
/// [`Authorization`].
pub trait ProofChain<P: Protocol>:
    Sized + ConditionalSend + ConditionalSync + Serialize + for<'de> Deserialize<'de>
{
    /// Create a new empty proof chain for the given access scope.
    fn new(access: P::Access) -> Self;

    /// The access scope this proof chain was created for.
    fn access(&self) -> &P::Access;

    /// Add a verified proof to this chain.
    fn push(&mut self, proof: P::Proof);

    /// The proofs collected in this chain.
    fn proofs(&self) -> &[P::Proof];

    /// Bind a signer to this proof chain, producing a full authorization.
    fn claim(self, signer: P::Signer) -> Result<P::Authorization, AuthorizeError>;
}

/// Permit attenuation — parent for authorization effects.
///
/// Attaches to [`Subject`](crate::Subject) and provides the `/permit`
/// ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Permit;

impl crate::Attenuation for Permit {
    type Of = crate::Subject;
}

/// Access protocol — defines how authorization is produced.
///
/// Different protocols use different access representations, proof
/// formats, and authorization/invocation materials.
pub trait Protocol: Sized + ConditionalSend + 'static {
    /// The type-erased form of a capability for this protocol.
    type Access: Scope + Serialize + for<'de> Deserialize<'de>;

    /// The signer type for this protocol.
    type Signer: crate::Principal + ConditionalSend;

    /// An individual delegation record (proof link) in this protocol's format.
    type Proof: Delegation<Access = Self::Access> + ConditionalSend + ConditionalSync;

    /// A delegation chain — what [`Authorization::delegate`] produces.
    type Delegation: ConditionalSend;

    /// An invocation chain — what [`Authorization::invoke`] produces.
    type Invocation: ConditionalSend;

    /// Verified proof chain (no signer). Returned by [`Authorize`].
    type ProofChain: ProofChain<Self> + ConditionalSend;

    /// Full authorization with signer bound. Can delegate and invoke.
    type Authorization: Authorization<Self> + ConditionalSend;
}

/// Full authorization — can produce delegations and invocations.
///
/// Created by [`ProofChain::claim`] after binding a signer. Holds the
/// verified delegation chain, signer, and scope.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Authorization<P: Protocol> {
    /// Delegate this authorization to another principal.
    async fn delegate(&self, audience: Did) -> Result<P::Delegation, AuthorizeError>;

    /// Create a signed invocation from this authorization.
    async fn invoke(&self) -> Result<P::Invocation, AuthorizeError>;
}

/// Claim effect — requests authorization for a capability.
///
/// An [`Effect`](crate::Effect) on [`Permit`]. The subject DID
/// in the capability chain determines which store handles the request.
#[derive(Serialize, Deserialize, crate::Claim)]
#[serde(bound(
    serialize = "P::Access: Serialize",
    deserialize = "P::Access: for<'a> Deserialize<'a>"
))]
pub struct Claim<P: Protocol> {
    /// The DID of the principal claiming access.
    pub by: Did,
    /// The access being claimed.
    pub access: P::Access,
    /// Optional time bound for validation.
    pub time: Option<u64>,
}

impl<P: Protocol> Claim<P> {
    /// Create a new claim request.
    pub fn new(by: Did, access: P::Access) -> Self {
        Self {
            by,
            access,
            time: None,
        }
    }

    /// Set the time bound for validation.
    pub fn at(mut self, time: u64) -> Self {
        self.time = Some(time);
        self
    }
}

impl<P: Protocol> crate::Effect for Claim<P>
where
    P::Access: ConditionalSend + 'static,
{
    type Of = Permit;
    type Output = Result<P::ProofChain, AuthorizeError>;
}

/// Save effect — stores a proof chain's proofs for future authorization lookups.
///
/// An [`Effect`](crate::Effect) on [`Permit`]. The subject DID
/// in the capability chain determines where proofs are stored.
#[derive(Serialize, Deserialize, crate::Claim)]
#[serde(bound(
    serialize = "P::ProofChain: Serialize",
    deserialize = "P::ProofChain: for<'a> Deserialize<'a>"
))]
pub struct Save<P: Protocol> {
    /// The proof chain whose proofs should be stored.
    pub proof_chain: P::ProofChain,
}

impl<P: Protocol> Save<P> {
    /// Create a new save effect.
    pub fn new(proof_chain: P::ProofChain) -> Self {
        Self { proof_chain }
    }
}

impl<P: Protocol> crate::Effect for Save<P>
where
    P::ProofChain: ConditionalSend + 'static,
{
    type Of = Permit;
    type Output = Result<(), AuthorizeError>;
}

/// Storage backend for delegation proofs.
///
/// Each storage backend (FileStore, Volatile, IndexedDb) implements this
/// to provide proof lookup and storage for the authorization system.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait ProofStore<P: Protocol> {
    /// Maximum chain depth for BFS delegation chain walking.
    const MAX_DEPTH: usize = 10;

    /// List proofs where the given DID is the audience.
    ///
    /// `subject` scopes the lookup:
    /// - `Some(did)` — subject-specific delegations
    /// - `None` — powerline delegations (any subject)
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Proof>, AuthorizeError>;

    /// Store a proof chain's proofs for future authorization lookups.
    async fn save(&self, proof_chain: &P::ProofChain) -> Result<(), AuthorizeError>;

    /// Resolve a delegation chain for the given claim.
    ///
    /// Default implementation: BFS from claimant toward subject.
    /// Searches subject-specific delegations first, then powerline.
    /// Prioritizes direct grants (issuer == subject) over intermediate links.
    async fn authorize(&self, input: Claim<P>) -> Result<P::ProofChain, AuthorizeError>
    where
        P::Access: Clone + ConditionalSend + ConditionalSync,
        P::Proof: Clone + ConditionalSend + ConditionalSync,
    {
        let authority = &input.by;
        let access = &input.access;
        let time = input.time;
        let subject = access.subject().clone();

        if *authority == subject {
            return Ok(P::ProofChain::new(access.clone()));
        }

        let mut queue: Vec<(Did, Vec<P::Proof>, usize)> = vec![(authority.clone(), vec![], 0)];

        while let Some((current_audience, chain_so_far, depth)) = queue.pop() {
            if depth >= Self::MAX_DEPTH {
                continue;
            }

            let specific = self.list(&current_audience, Some(&subject)).await?;
            let powerline = self.list(&current_audience, None).await?;

            let candidates = specific.into_iter().chain(powerline).filter_map(|proof| {
                let range = proof.verify(access).ok()?;
                if let Some(t) = time {
                    if !range.contains(t) {
                        return None;
                    }
                }
                Some(proof)
            });

            let (direct, indirect): (Vec<_>, Vec<_>) =
                candidates.partition(|proof| proof.issuer() == &subject);

            for proof in direct.into_iter().chain(indirect) {
                let issuer = proof.issuer().clone();
                let mut new_chain = chain_so_far.clone();
                new_chain.insert(0, proof);

                if issuer == subject {
                    let mut proof_chain = P::ProofChain::new(access.clone());
                    for p in new_chain {
                        proof_chain.push(p);
                    }
                    return Ok(proof_chain);
                }

                queue.push((issuer, new_chain, depth + 1));
            }
        }

        Err(AuthorizeError::Denied(format!(
            "no delegation chain found for '{}' to access '{}'",
            authority, subject
        )))
    }
}

/// Error during the authorize step.
#[derive(Debug, Error)]
pub enum AuthorizeError {
    /// Authorization was denied.
    #[error("Authorization denied: {0}")]
    Denied(String),

    /// Configuration error (e.g., missing delegation chain).
    #[error("Authorization configuration error: {0}")]
    Configuration(String),
}
