//! Access — authorization for capability execution.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (profile DID)
//! └── Access
//!     ├── Prove { access, by, time } → ProofChain
//!     └── Retain { delegation } → ()
//! ```
//!
//! # Authorization Flow
//!
//! 1. `Subject.attenuate(Access).invoke(Prove { .. }).perform(&store)`
//!    returns a [`Proof`] (verified chain, no signer).
//! 2. `proof.claim(signer)` binds a signer to produce an
//!    [`Authorization`] that can `delegate()` and `invoke()`.

use crate::{Ability, Attenuate, Capability, Constraint, Did, Effect};
use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::marker::PhantomData;
use thiserror::Error;

/// Describes the scope of access being requested or granted.
///
/// Lighter than [`Ability`](crate::Ability) — only requires the subject DID.
/// Protocol-specific scope types add ability paths, parameters, etc.
pub trait Scope {
    /// The subject (resource) this scope applies to.
    fn subject(&self) -> &Did;

    /// The command being requested, as ability path segments.
    ///
    /// Two scopes that differ only in their parameters share a command,
    /// so this names the access independently of the arguments any one
    /// invocation carries.
    fn command(&self) -> &[String];
}

/// Derive an access scope from an invocable capability.
///
/// Protocol-specific scope types implement this to extract the subject,
/// command path, and parameters from a capability chain. The Operator
/// uses this to build the [`Prove`] request generically across protocols.
pub trait FromCapability: Scope {
    /// Derive a scope from an effect capability.
    fn from_capability<Fx>(capability: &Capability<Fx>) -> Self
    where
        Fx: Effect + Clone,
        Fx::Of: Constraint,
        Capability<Fx>: Ability;
}

/// The time range during which a delegation is valid.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeRange {
    /// Earliest time this delegation is valid.
    pub not_before: Option<u64>,
    /// When this delegation expires.
    pub expiration: Option<u64>,
}

impl TimeRange {
    /// An unbounded time range (no constraints).
    pub fn unbounded() -> Self {
        Self {
            not_before: None,
            expiration: None,
        }
    }

    /// Check whether the given time falls within this range.
    pub fn contains(&self, time: u64) -> bool {
        if let Some(nbf) = self.not_before
            && time < nbf
        {
            return false;
        }
        if let Some(exp) = self.expiration
            && time >= exp
        {
            return false;
        }
        true
    }

    /// Check whether this range overlaps with the required duration.
    ///
    /// A delegation's time range overlaps the required duration when:
    /// - The delegation doesn't expire before the required not_before
    /// - The delegation isn't not-yet-valid after the required expiration
    pub fn overlaps(&self, required: &TimeRange) -> bool {
        if let (Some(req_nbf), Some(exp)) = (required.not_before, self.expiration)
            && exp <= req_nbf
        {
            return false;
        }
        if let (Some(req_exp), Some(nbf)) = (required.expiration, self.not_before)
            && nbf >= req_exp
        {
            return false;
        }
        true
    }

    /// Whether this range has any constraints.
    pub fn is_unbounded(&self) -> bool {
        self.not_before.is_none() && self.expiration.is_none()
    }

    /// Check whether this range covers (is at least as wide as) the required range.
    ///
    /// A `None` bound in the requirement means "no constraint" on that side.
    /// A `None` bound in `self` means unbounded on that side (covers any requirement).
    ///
    /// - If required `not_before` is `Some(100)`, this range must start at or before 100.
    /// - If required `expiration` is `Some(500)`, this range must not expire before 500.
    /// - If required bound is `None`, any value in `self` is acceptable.
    pub fn covers(&self, required: &TimeRange) -> bool {
        if let Some(req_nbf) = required.not_before
            && let Some(nbf) = self.not_before
            && nbf > req_nbf
        {
            return false;
        }
        if let Some(req_exp) = required.expiration
            && let Some(exp) = self.expiration
            && exp < req_exp
        {
            return false;
        }
        true
    }

    /// Compute the intersection of two time ranges (most restrictive).
    pub fn intersect(&self, other: &TimeRange) -> TimeRange {
        let not_before = match (self.not_before, other.not_before) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        let expiration = match (self.expiration, other.expiration) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        TimeRange {
            not_before,
            expiration,
        }
    }
}

/// A delegation bundle — contains one or more signed certificates.
///
/// Produced by [`Authorization::delegate`]. Stored via [`Save`].
pub trait Delegation:
    ConditionalSend + ConditionalSync + Serialize + for<'de> Deserialize<'de>
{
    /// The certificate type contained in this delegation.
    type Certificate;

    /// Extract individual certificates from this delegation.
    fn certificates(&self) -> Vec<Self::Certificate>;
}

/// An individual delegation record — a single proof link in a chain.
///
/// Each delegation links an issuer to an audience. The [`verify`](Delegation::verify)
/// method checks whether the delegation covers the requested access and
/// returns the time range during which it is valid.
pub trait Certificate: ConditionalSend + ConditionalSync {
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
pub trait Proof<P: Protocol>:
    Sized + ConditionalSend + ConditionalSync + Serialize + for<'de> Deserialize<'de>
{
    /// Create a new empty proof chain for the given access scope.
    fn new(access: P::Access) -> Self;

    /// The access scope this proof chain was created for.
    fn access(&self) -> &P::Access;

    /// Add a verified proof to this chain.
    fn push(&mut self, proof: P::Certificate);

    /// The proofs collected in this chain.
    fn proofs(&self) -> &[P::Certificate];

    /// The effective time range this proof covers.
    ///
    /// Computed as the intersection of all certificate time ranges
    /// in the chain. Unbounded if self-authorized.
    fn duration(&self) -> &TimeRange;

    /// Set the effective time range for this proof.
    fn set_duration(&mut self, duration: TimeRange);

    /// Bind a signer to this proof chain, producing a full authorization.
    fn claim(self, signer: P::Signer) -> Result<P::Authorization, AuthorizeError>;
}

/// Access attenuation — parent for authorization effects.
///
/// Attaches to [`Subject`](crate::Subject) and provides the `/access`
/// ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Access;

impl crate::Attenuation for Access {
    type Of = crate::Subject;
}

/// Access protocol — defines how capability-based authorization is produced.
///
/// Different protocols use different access representations, proof
/// formats, and authorization/invocation materials.
pub trait Protocol: Sized + ConditionalSend + 'static {
    /// The type-erased form of a capability for this protocol.
    ///
    /// Must implement [`FromCapability`] so the Operator can derive
    /// a scope from any capability for the Prove request.
    type Access: FromCapability
        + Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + ConditionalSend
        + ConditionalSync;

    /// The signer type for this protocol.
    type Signer: crate::Principal + ConditionalSend;

    /// An individual delegation (signed certificate) in this protocol's format.
    type Certificate: Certificate<Access = Self::Access> + Clone + ConditionalSend + ConditionalSync;

    /// A delegation bundle — what [`Authorization::delegate`] produces.
    type Delegation: Delegation<Certificate = Self::Certificate>;

    /// An invocation — what [`Authorization::invoke`] produces.
    type Invocation: ConditionalSend;

    /// Verified proof (no signer). Returned by [`Prove`].
    type Proof: Proof<Self> + ConditionalSend;

    /// Full authorization with signer bound. Can delegate and invoke.
    type Authorization: Authorization<Self> + ConditionalSend;
}

/// Full authorization — can produce delegations and invocations.
///
/// Created by [`Proof::claim`] after binding a signer. Holds the
/// verified delegation chain, signer, and scope.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Authorization<P: Protocol>: Sized {
    /// The time range this authorization is valid for.
    fn duration(&self) -> &TimeRange;

    /// Constrain the earliest time this authorization is valid.
    ///
    /// Fails if the timestamp is earlier than the proof's `not_before`,
    /// since the authorization cannot be widened beyond what was proven.
    fn not_before(self, timestamp: u64) -> Result<Self, AuthorizeError>;

    /// Constrain when this authorization expires.
    ///
    /// Fails if the timestamp is later than the proof's `expiration`,
    /// since the authorization cannot be widened beyond what was proven.
    fn expires(self, timestamp: u64) -> Result<Self, AuthorizeError>;

    /// Delegate this authorization to another principal.
    async fn delegate(&self, audience: Did) -> Result<P::Delegation, AuthorizeError>;

    /// Create a signed invocation from this authorization.
    async fn invoke(&self) -> Result<P::Invocation, AuthorizeError>;
}

/// Proved effect — requests proof of access.
///
/// An [`Effect`](crate::Effect) on [`Access`]. The subject DID
/// in the capability chain determines which store handles the request.
#[derive(Serialize, Deserialize, Attenuate)]
#[serde(bound(
    serialize = "P::Access: Serialize",
    deserialize = "P::Access: for<'a> Deserialize<'a>"
))]
pub struct Prove<P: Protocol> {
    /// The DID of the principal claiming access.
    pub principal: Did,
    /// The access being claimed.
    pub access: P::Access,
    /// Time range the authorization must cover.
    pub duration: TimeRange,
}

impl<P: Protocol> Prove<P> {
    /// Create a new claim request with unbounded duration.
    pub fn new(by: Did, access: P::Access) -> Self {
        Self {
            principal: by,
            access,
            duration: TimeRange::unbounded(),
        }
    }

    /// Constrain the claim to a specific time range.
    pub fn during(mut self, duration: TimeRange) -> Self {
        self.duration = duration;
        self
    }
}

/// Written by hand because a derive would demand `P: Clone`, and the
/// protocol marker is never a value.
impl<P: Protocol> Clone for Prove<P> {
    fn clone(&self) -> Self {
        Self {
            principal: self.principal.clone(),
            access: self.access.clone(),
            duration: self.duration,
        }
    }
}

impl<P: Protocol> crate::Effect for Prove<P>
where
    P::Access: ConditionalSend + 'static,
{
    type Of = Access;
    type Output = Result<P::Proof, AuthorizeError>;
}

/// Authorize effect — proves access and binds a signer in one step.
///
/// Like [`Prove`], but also binds a signer to produce a full
/// [`Protocol::Authorization`] rather than an unsigned proof.
/// The provider (typically the Operator) handles both the proof
/// lookup and the signing internally.
#[derive(Serialize, Deserialize, Attenuate)]
#[serde(bound(
    serialize = "P::Access: Serialize",
    deserialize = "P::Access: for<'a> Deserialize<'a>"
))]
pub struct Authorize<P: Protocol> {
    /// The DID of the principal claiming access.
    pub principal: Did,
    /// The access being claimed.
    pub access: P::Access,
    /// Time range the authorization must cover.
    pub duration: TimeRange,
}

impl<P: Protocol> Authorize<P> {
    /// Create a new authorization request with unbounded duration.
    pub fn new(by: Did, access: P::Access) -> Self {
        Self {
            principal: by,
            access,
            duration: TimeRange::unbounded(),
        }
    }

    /// Constrain the authorization to a specific time range.
    pub fn during(mut self, duration: TimeRange) -> Self {
        self.duration = duration;
        self
    }
}

impl<P: Protocol> From<Authorize<P>> for Prove<P> {
    fn from(authorize: Authorize<P>) -> Self {
        Prove {
            principal: authorize.principal,
            access: authorize.access,
            duration: authorize.duration,
        }
    }
}

impl<P: Protocol> Effect for Authorize<P>
where
    P::Access: ConditionalSend + 'static,
{
    type Of = Access;
    type Output = Result<P::Authorization, AuthorizeError>;
}

/// Retain effect — retains a delegation for future proof lookups.
///
/// An [`Effect`](crate::Effect) on [`Access`]. The subject DID
/// in the capability chain determines where proofs are stored.
#[derive(Serialize, Deserialize, Attenuate)]
#[serde(bound(
    serialize = "P::Delegation: Serialize",
    deserialize = "P::Delegation: for<'a> Deserialize<'a>"
))]
pub struct Retain<P: Protocol> {
    /// The delegation to retain.
    pub delegation: P::Delegation,
}

impl<P: Protocol> Retain<P> {
    /// Create a new retain effect.
    pub fn new(delegation: P::Delegation) -> Self {
        Self { delegation }
    }
}

impl<P: Protocol> crate::Effect for Retain<P>
where
    P::Delegation: ConditionalSend + 'static,
{
    type Of = Access;
    type Output = Result<(), AuthorizeError>;
}

/// Forget effect — removes specific certificates from a store.
///
/// An [`Effect`](crate::Effect) on [`Access`]. The counterpart of
/// [`Export`] for migration: after certificates are re-retained
/// elsewhere, this drains exactly those from the store they came from,
/// leaving everything else in place.
#[derive(Serialize, Deserialize, Attenuate)]
#[serde(bound(
    serialize = "P::Certificate: Serialize",
    deserialize = "P::Certificate: for<'a> Deserialize<'a>"
))]
pub struct Forget<P: Protocol> {
    /// The certificates to remove.
    pub certificates: Vec<P::Certificate>,
}

impl<P: Protocol> Forget<P> {
    /// Create a new forget request.
    pub fn new(certificates: Vec<P::Certificate>) -> Self {
        Self { certificates }
    }
}

impl<P: Protocol> crate::Effect for Forget<P>
where
    P::Certificate: Serialize + for<'de> Deserialize<'de> + ConditionalSend + 'static,
{
    type Of = Access;
    type Output = Result<(), AuthorizeError>;
}

/// Export effect — enumerates every retained certificate.
///
/// An [`Effect`](crate::Effect) on [`Access`]. The subject DID in the
/// capability chain determines which store is enumerated. Exists for
/// migration: a caller moving certificates from one store to another
/// (the legacy per-provider certificate stores into the synced
/// delegation records) reads them all through this rather than knowing
/// each store's layout.
#[derive(Serialize, Deserialize, Attenuate)]
pub struct Export<P: Protocol> {
    #[serde(skip)]
    marker: PhantomData<fn() -> P>,
}

impl<P: Protocol> Export<P> {
    /// Create a new export request.
    pub fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<P: Protocol> Default for Export<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Protocol> crate::Effect for Export<P>
where
    P::Certificate: 'static,
{
    type Of = Access;
    type Output = Result<Vec<P::Certificate>, AuthorizeError>;
}

/// Storage backend for delegation proofs.
///
/// Each storage backend (FileStore, Volatile, IndexedDb) implements this
/// to provide proof lookup and storage for the authorization system.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait CertificateStore<P: Protocol> {
    /// Maximum chain depth for BFS delegation chain walking.
    const MAX_DEPTH: usize = 10;

    /// List certificates where the given DID is the audience.
    ///
    /// `subject` scopes the lookup:
    /// - `Some(did)` — subject-specific delegations
    /// - `None` — powerline delegations (any subject)
    async fn list(
        &self,
        audience: &Did,
        subject: Option<&Did>,
    ) -> Result<Vec<P::Certificate>, AuthorizeError>;

    /// Store a delegation for future authorization lookups.
    async fn save(&self, delegation: &P::Delegation) -> Result<(), AuthorizeError>;

    /// Enumerate every certificate this store retains, for migration into
    /// another store (see [`Export`]).
    async fn export(&self) -> Result<Vec<P::Certificate>, AuthorizeError>;

    /// Remove specific certificates from this store (see [`Forget`]).
    /// Removing an absent certificate is a no-op.
    async fn forget(&self, certificates: &[P::Certificate]) -> Result<(), AuthorizeError>;

    /// Resolve a delegation chain for the given claim.
    ///
    /// Default implementation: BFS from claimant toward subject.
    /// Searches subject-specific delegations first, then powerline.
    /// Prioritizes direct grants (issuer == subject) over intermediate links.
    async fn prove(&self, input: Prove<P>) -> Result<P::Proof, AuthorizeError>
    where
        P::Access: Clone + ConditionalSend + ConditionalSync,
        P::Certificate: Clone + ConditionalSend + ConditionalSync,
    {
        let authority = &input.principal;
        let access = &input.access;
        let duration = &input.duration;
        let subject = access.subject().clone();

        if *authority == subject || crate::Subject::from(subject.clone()).is_any() {
            return Ok(P::Proof::new(access.clone()));
        }

        let mut queue: Vec<(Did, Vec<(P::Certificate, TimeRange)>, TimeRange, usize)> =
            vec![(authority.clone(), vec![], TimeRange::unbounded(), 0)];

        while let Some((current_audience, chain_so_far, effective_range, depth)) = queue.pop() {
            if depth >= Self::MAX_DEPTH {
                continue;
            }

            let specific = self.list(&current_audience, Some(&subject)).await?;
            let powerline = self.list(&current_audience, None).await?;

            let candidates = specific.into_iter().chain(powerline).filter_map(|proof| {
                let range = proof.verify(access).ok()?;
                if !range.covers(duration) {
                    return None;
                }
                Some((proof, range))
            });

            let (direct, indirect): (Vec<_>, Vec<_>) =
                candidates.partition(|(proof, _)| proof.issuer() == &subject);

            for (proof, range) in direct.into_iter().chain(indirect) {
                let issuer = proof.issuer().clone();
                let mut new_chain = chain_so_far.clone();
                let new_range = effective_range.intersect(&range);
                new_chain.insert(0, (proof, new_range));

                if issuer == subject {
                    let effective = new_chain
                        .iter()
                        .fold(TimeRange::unbounded(), |acc, (_, r)| acc.intersect(r));
                    let mut proof_chain = P::Proof::new(access.clone());
                    for (p, _) in new_chain {
                        proof_chain.push(p);
                    }
                    proof_chain.set_duration(effective);
                    return Ok(proof_chain);
                }

                let chain_range = new_chain
                    .iter()
                    .fold(TimeRange::unbounded(), |acc, (_, r)| acc.intersect(r));
                queue.push((issuer, new_chain, chain_range, depth + 1));
            }
        }

        Err(AuthorizeError::UnprovenSubject {
            claimed: authority.clone(),
            authorized: subject.clone(),
        })
    }
}

/// Whether a [`Declined`](AuthorizeError::Declined) refusal can change
/// without the caller doing anything.
///
/// The only thing this crate can say about another party's policy, and
/// the only thing a caller can act on generically: keep the request in
/// hand and try again, or stop. What the policy actually is stays
/// opaque -- it belongs to whoever set it, and this crate does not model
/// other people's rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Recourse {
    /// The refusal stands until something changes elsewhere, and the
    /// caller cannot make that happen by retrying. Stop.
    None,
    /// The condition is expected to clear on its own -- someone else
    /// completes a step, a state settles -- so the same request may
    /// succeed later. Retrying is the intended behavior, and a caller
    /// waiting on it should hold what it has rather than start over.
    Retry,
}

impl Display for Recourse {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(match self {
            Self::None => "no recourse",
            Self::Retry => "may succeed later",
        })
    }
}

/// Error during the authorize step.
///
/// Most variants here are the caller's input failing to authorize what it
/// asked for -- the request is answerable, and the answer is no. The
/// exceptions are [`UnavailableProof`](Self::UnavailableProof) and
/// [`Malformed`](Self::Malformed), which mean no decision could be reached
/// at all. A backend that broke while trying to decide is a
/// [`StorageError`](crate::StorageError), not one of these.
///
/// Variant names and `claimed`/`authorized` field naming follow
/// `dialog_ucan_core::invocation::CheckFailed`, which classifies the same
/// failures one layer down. This enum cannot reuse it -- that would invert
/// the dependency, since the UCAN implementation must not depend on dialog
/// types -- so it mirrors the vocabulary instead, and abilities arrive as
/// `String` paths rather than a `Command` for the same reason.
///
/// The split matters because callers act differently on the reasons.
/// [`Expired`](Self::Expired) means obtain a fresh proof and retry;
/// [`Revoked`](Self::Revoked) means stop, since retrying presents the same
/// withdrawn authority.
#[derive(Debug, Error, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum AuthorizeError {
    /// No delegation chain connects the authority to the subject.
    ///
    /// A real access decision: the delegations were searched and none of
    /// them authorizes this. Distinct from
    /// [`UnavailableProof`](Self::UnavailableProof), where a chain may well
    /// exist but could not be evaluated.
    ///
    /// Mirrors `CheckFailed::UnprovenSubject`.
    #[error("No delegation chain proves '{claimed}' may access '{authorized}'")]
    UnprovenSubject {
        /// The principal attempting access.
        claimed: Did,
        /// The subject it attempted to access.
        authorized: Did,
    },

    /// A delegation exists but does not cover the requested ability.
    ///
    /// Abilities are paths (`/storage/get`), matching
    /// [`Ability::ability`](crate::Ability::ability), which is how this crate
    /// represents them everywhere else.
    ///
    /// Mirrors `CheckFailed::CommandEscalation`.
    #[error("Claimed ability '{claimed}' is not authorized by ability '{authorized}'")]
    CommandEscalation {
        /// The ability the invocation asked for.
        claimed: String,
        /// The ability the delegation actually grants.
        authorized: String,
    },

    /// A delegation covers the ability, but its policy rejected the arguments.
    ///
    /// Mirrors `CheckFailed::PolicyViolation`. That variant carries the
    /// `Predicate` that failed; this one cannot name it without depending on
    /// the UCAN types, so it carries the rendered predicate instead.
    #[error("Invocation arguments violate delegation policy: {predicate}")]
    PolicyViolation {
        /// The predicate that evaluated to `false`, as rendered by the
        /// protocol that evaluated it.
        predicate: String,
    },

    /// The proof was issued for a different audience than the one presenting it.
    ///
    /// Mirrors `CheckFailed::DelegationAudienceMismatch`.
    #[error("Claimed audience '{claimed}' does not match authorized audience '{authorized}'")]
    InvalidAudience {
        /// The audience the proof names.
        claimed: Did,
        /// The audience the invocation requires.
        authorized: Did,
    },

    /// The proof's validity window has not opened yet.
    ///
    /// Mirrors the `TooEarly` side of `CheckFailed::TimeBound`.
    #[error("Proof is not valid before {not_before}")]
    NotValidBefore {
        /// Unix timestamp the proof becomes valid at.
        not_before: u64,
        /// Unix timestamp the check was made at.
        at: u64,
    },

    /// The proof's validity window has closed.
    ///
    /// Distinct from [`Revoked`](Self::Revoked): the authority was never
    /// withdrawn, it simply lapsed, so obtaining a fresh proof is expected
    /// to succeed.
    ///
    /// Mirrors the `Expired` side of `CheckFailed::TimeBound`.
    #[error("Proof expired at {expiration}")]
    Expired {
        /// Unix timestamp the proof expired at.
        expiration: u64,
        /// Unix timestamp the check was made at.
        at: u64,
    },

    /// Authority in the chain has been withdrawn.
    ///
    /// Terminal in a way [`Expired`](Self::Expired) is not: re-obtaining a
    /// proof presents the same revoked authority, so callers should stop
    /// rather than retry.
    #[error("Authority for '{subject}' has been revoked")]
    Revoked {
        /// The subject whose authority was withdrawn.
        subject: Did,
    },

    /// The chain authorizes the request and the responder declined it
    /// anyway, on a policy of its own.
    ///
    /// Unlike every decision variant above, this says nothing about the
    /// caller's proof: the authority is real and would be honored if the
    /// responder were willing. What refused is a rule this crate does
    /// not model and should not -- whose resources these are, what they
    /// cost, what state an account is in are the responder's business.
    ///
    /// Mirrors no `CheckFailed` case, because no chain check produced it.
    ///
    /// So `reason` is opaque: it is whatever the responder said, kept
    /// for logs and error surfaces, and matching on its text is the
    /// mistake it looks like. The one thing a caller can act on
    /// generically is [`Recourse`] -- whether the same request is worth
    /// making again. A responder that wants to be understood more
    /// precisely than that publishes its own vocabulary alongside.
    #[error("The request was declined ({recourse}): {reason}")]
    Declined {
        /// Whether retrying the same request may succeed later.
        recourse: Recourse,
        /// The responder's own words, verbatim and unstructured.
        reason: String,
    },

    /// A proof's signature does not verify against its issuer's key.
    #[error("Proof does not carry a valid signature from '{issuer}'")]
    InvalidSignature {
        /// The principal the proof claims to be signed by.
        issuer: Did,
    },

    /// The chain referred to a proof that was not supplied.
    ///
    /// Not an access decision: the chain might well authorize this, but it
    /// cannot be evaluated because a link it names is missing. Proofs travel
    /// with the invocation here, so this is incomplete input rather than a
    /// resolution failure.
    ///
    /// Mirrors the conformance suite's `UnavailableProof`.
    #[error("Chain refers to proof '{link}', which was not supplied")]
    UnavailableProof {
        /// Identifier of the proof the chain referred to.
        link: String,
    },

    /// The caller's authorization material did not decode.
    ///
    /// Strictly that: bytes arrived and could not be read as what they
    /// claimed to be. Anything else that prevents a decision -- a key we
    /// could not load, a store we could not reach, our own signing
    /// failing -- is [`Unavailable`](Self::Unavailable), because it says
    /// nothing about the caller's input and a caller cannot act on it the
    /// same way.
    ///
    /// Also distinct from the decision variants above, which all mean
    /// "we understood the request and the answer is no".
    #[error("Authorization could not be evaluated: {detail}")]
    Malformed {
        /// What could not be evaluated.
        detail: String,
    },

    /// The decision could not be reached because our own machinery
    /// failed.
    ///
    /// Signing a payload, reading a key, reaching a store. Nothing is
    /// wrong with the caller's input, and nothing was decided, so this
    /// must not be reported as a denial -- a caller told "no" stops,
    /// where a caller told "we could not answer" may retry.
    ///
    /// The enum's other variants are all statements about the request.
    /// This one is a statement about us.
    #[error("Authorization could not be evaluated: {detail}")]
    Unavailable {
        /// What failed on our side.
        detail: String,
    },
}

impl From<crate::StorageError> for AuthorizeError {
    fn from(e: crate::StorageError) -> Self {
        AuthorizeError::Unavailable {
            detail: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::TimeRange;

    // The reasons cross the wire: the access service answers with these
    // rather than with a code table both sides have to keep in step.
    // Round-tripping every variant is what makes that safe -- a variant
    // that serializes but will not come back is a silent protocol break.
    mod wire {
        use crate::access::{AuthorizeError, Recourse};
        use crate::did;

        fn round_trip(error: AuthorizeError) {
            let encoded = serde_json::to_string(&error).expect("serializes");
            let decoded: AuthorizeError = serde_json::from_str(&encoded).expect("deserializes");
            assert_eq!(error, decoded, "round-tripped through {encoded}");
        }

        #[dialog_common::test]
        async fn it_round_trips_every_reason() {
            let subject = did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX");
            let audience = did!("key:z6MkfQhLHBSFMuR7bQXTQeqe5kYUW51HpfZeaymgy1zkP2jM");

            for error in [
                AuthorizeError::UnprovenSubject {
                    claimed: audience.clone(),
                    authorized: subject.clone(),
                },
                AuthorizeError::CommandEscalation {
                    claimed: "/storage/put".into(),
                    authorized: "/storage/get".into(),
                },
                AuthorizeError::PolicyViolation {
                    predicate: "size < 1024".into(),
                },
                AuthorizeError::Declined {
                    recourse: Recourse::Retry,
                    reason: "the subject's own registration awaits email activation".into(),
                },
                AuthorizeError::Declined {
                    recourse: Recourse::None,
                    reason: "the provider's registration is suspended".into(),
                },
                AuthorizeError::InvalidAudience {
                    claimed: audience.clone(),
                    authorized: subject.clone(),
                },
                AuthorizeError::Revoked {
                    subject: subject.clone(),
                },
                AuthorizeError::InvalidSignature {
                    issuer: subject.clone(),
                },
                AuthorizeError::Malformed {
                    detail: "bad envelope".into(),
                },
            ] {
                round_trip(error);
            }
        }

        // The tag is part of the protocol: renaming a variant renames
        // the wire form, so this pins what the service must emit.
        #[dialog_common::test]
        async fn it_names_the_reason_in_the_payload() {
            let encoded = serde_json::to_string(&AuthorizeError::Revoked {
                subject: did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"),
            })
            .expect("serializes");
            assert!(
                encoded.contains(r#""kind":"Revoked""#),
                "the reason is named in the payload, got {encoded}"
            );
        }
    }

    mod reasons {
        use crate::access::{AuthorizeError, Recourse};
        use crate::did;

        // Being declined is not failing to prove authority. The chain
        // authorizes the request; what says no is a policy of the
        // responder's, so a caller that reads this as "my credentials
        // are wrong" would re-authenticate forever.
        #[dialog_common::test]
        async fn it_separates_a_declined_request_from_an_unproven_one() {
            let declined = AuthorizeError::Declined {
                recourse: Recourse::Retry,
                reason: "awaiting something".into(),
            };
            assert!(matches!(declined, AuthorizeError::Declined { .. }));
            assert!(!matches!(declined, AuthorizeError::UnprovenSubject { .. }));
        }

        // The one thing this crate says about someone else's policy.
        // Everything about WHY stays in `reason`: the responder's own
        // words, which are not ours to interpret.
        #[dialog_common::test]
        async fn it_says_only_whether_the_request_is_worth_repeating() {
            let waiting = AuthorizeError::Declined {
                recourse: Recourse::Retry,
                reason: "settling".into(),
            };
            let final_answer = AuthorizeError::Declined {
                recourse: Recourse::None,
                reason: "withdrawn".into(),
            };
            assert!(matches!(
                waiting,
                AuthorizeError::Declined {
                    recourse: Recourse::Retry,
                    ..
                }
            ));
            assert!(matches!(
                final_answer,
                AuthorizeError::Declined {
                    recourse: Recourse::None,
                    ..
                }
            ));
        }

        // The distinction the split exists for. Both mean "this proof does not
        // authorize you right now", and a single `Denied(String)` could not
        // tell them apart -- but a caller should re-authenticate on one and
        // stop on the other, so they cannot share a variant.
        #[dialog_common::test]
        async fn it_separates_a_lapsed_proof_from_a_withdrawn_one() {
            let lapsed = AuthorizeError::Expired {
                expiration: 100,
                at: 200,
            };
            let withdrawn = AuthorizeError::Revoked {
                subject: did!("key:zSubject"),
            };

            assert!(matches!(lapsed, AuthorizeError::Expired { .. }));
            assert!(matches!(withdrawn, AuthorizeError::Revoked { .. }));
            assert_ne!(lapsed.to_string(), withdrawn.to_string());
        }

        // Operands are carried, not interpolated into prose, so a caller can
        // read them back rather than parse a message.
        #[dialog_common::test]
        async fn it_carries_the_operands_of_a_denial() {
            let claimed_did = did!("key:zAuthority");
            let authorized_did = did!("key:zSubject");

            match (AuthorizeError::UnprovenSubject {
                claimed: claimed_did.clone(),
                authorized: authorized_did.clone(),
            }) {
                AuthorizeError::UnprovenSubject {
                    claimed,
                    authorized,
                } => {
                    assert_eq!(claimed, claimed_did);
                    assert_eq!(authorized, authorized_did);
                }
                other => panic!("expected UnprovenSubject, got {other:?}"),
            }

            match (AuthorizeError::CommandEscalation {
                claimed: "/archive/put".into(),
                authorized: "/archive/get".into(),
            }) {
                AuthorizeError::CommandEscalation {
                    claimed,
                    authorized,
                } => {
                    assert_eq!(claimed, "/archive/put");
                    assert_eq!(authorized, "/archive/get");
                }
                other => panic!("expected CommandEscalation, got {other:?}"),
            }
        }

        // Two failures that are not access decisions at all. Keeping them
        // distinct from the decision variants is what stops either becoming
        // the next catch-all -- and `UnavailableProof` specifically must not
        // read as "denied", because the chain may well authorize once the
        // caller supplies the link it refers to.
        #[dialog_common::test]
        async fn it_keeps_unevaluable_authorizations_out_of_the_denial_reasons() {
            let undecodable = AuthorizeError::Malformed {
                detail: "chain did not decode".into(),
            };
            assert!(
                undecodable.to_string().contains("could not be evaluated"),
                "an unevaluable authorization must not read as a refusal: {undecodable}"
            );

            let incomplete = AuthorizeError::UnavailableProof {
                link: "bafyproof".into(),
            };
            match incomplete {
                AuthorizeError::UnavailableProof { ref link } => assert_eq!(link, "bafyproof"),
                other => panic!("expected UnavailableProof, got {other:?}"),
            }
            assert!(
                incomplete.to_string().contains("not supplied"),
                "a missing proof must name what is absent, not read as a denial: {incomplete}"
            );
        }

        // A chain that was searched and found wanting is a decision; a chain
        // that could not be evaluated is not. Both used to be `Denied`.
        #[dialog_common::test]
        async fn it_separates_an_absent_chain_from_an_unevaluable_one() {
            let decided = AuthorizeError::UnprovenSubject {
                claimed: did!("key:zAuthority"),
                authorized: did!("key:zSubject"),
            };
            let undecided = AuthorizeError::UnavailableProof {
                link: "bafyproof".into(),
            };

            assert!(matches!(decided, AuthorizeError::UnprovenSubject { .. }));
            assert!(matches!(undecided, AuthorizeError::UnavailableProof { .. }));
            assert_ne!(decided.to_string(), undecided.to_string());
        }
    }

    mod covers {
        use super::*;

        #[test]
        fn unbounded_cert_covers_any_requirement() {
            let cert = TimeRange::unbounded();

            assert!(cert.covers(&TimeRange::unbounded()));
            assert!(cert.covers(&TimeRange {
                not_before: Some(100),
                expiration: Some(500),
            }));
        }

        #[test]
        fn unbounded_requirement_accepts_any_cert() {
            // "I don't care about time bounds"
            let required = TimeRange::unbounded();

            assert!(TimeRange::unbounded().covers(&required));
            assert!(
                TimeRange {
                    not_before: Some(100),
                    expiration: Some(200),
                }
                .covers(&required)
            );
            assert!(
                TimeRange {
                    not_before: None,
                    expiration: Some(100),
                }
                .covers(&required)
            );
        }

        #[test]
        fn cert_expiring_before_required_does_not_cover() {
            // "I need it valid until 500"
            let required = TimeRange {
                not_before: None,
                expiration: Some(500),
            };
            // cert expires at 300
            let cert = TimeRange {
                not_before: None,
                expiration: Some(300),
            };
            assert!(!cert.covers(&required));
        }

        #[test]
        fn cert_expiring_after_required_covers() {
            let required = TimeRange {
                not_before: None,
                expiration: Some(500),
            };
            let cert = TimeRange {
                not_before: None,
                expiration: Some(1000),
            };
            assert!(cert.covers(&required));
        }

        #[test]
        fn cert_starting_after_required_does_not_cover() {
            // "I need it valid from 100"
            let required = TimeRange {
                not_before: Some(100),
                expiration: None,
            };
            // cert not valid before 200
            let cert = TimeRange {
                not_before: Some(200),
                expiration: None,
            };
            assert!(!cert.covers(&required));
        }

        #[test]
        fn cert_starting_before_required_covers() {
            let required = TimeRange {
                not_before: Some(100),
                expiration: None,
            };
            let cert = TimeRange {
                not_before: Some(50),
                expiration: None,
            };
            assert!(cert.covers(&required));
        }

        #[test]
        fn cert_with_no_expiry_covers_any_expiry_requirement() {
            // cert has no upper bound (valid forever in UCAN terms)
            let cert = TimeRange {
                not_before: Some(100),
                expiration: None,
            };
            let required = TimeRange {
                not_before: Some(100),
                expiration: Some(999999),
            };
            assert!(cert.covers(&required));
        }

        #[test]
        fn no_expiry_requirement_accepts_cert_with_expiry() {
            // "I don't care when it expires"
            let required = TimeRange {
                not_before: Some(100),
                expiration: None,
            };
            let cert = TimeRange {
                not_before: Some(50),
                expiration: Some(200),
            };
            assert!(cert.covers(&required));
        }

        #[test]
        fn exact_match_covers() {
            let range = TimeRange {
                not_before: Some(100),
                expiration: Some(500),
            };
            assert!(range.covers(&range));
        }

        #[test]
        fn wider_cert_covers_narrower_requirement() {
            let cert = TimeRange {
                not_before: Some(50),
                expiration: Some(1000),
            };
            let required = TimeRange {
                not_before: Some(100),
                expiration: Some(500),
            };
            assert!(cert.covers(&required));
        }

        #[test]
        fn narrower_cert_does_not_cover_wider_requirement() {
            let cert = TimeRange {
                not_before: Some(200),
                expiration: Some(400),
            };
            let required = TimeRange {
                not_before: Some(100),
                expiration: Some(500),
            };
            assert!(!cert.covers(&required));
        }
    }

    mod intersect {
        use super::*;

        #[test]
        fn unbounded_intersect_bounded() {
            let a = TimeRange::unbounded();
            let b = TimeRange {
                not_before: Some(100),
                expiration: Some(500),
            };
            let result = a.intersect(&b);
            assert_eq!(result.not_before, Some(100));
            assert_eq!(result.expiration, Some(500));
        }

        #[test]
        fn takes_latest_not_before() {
            let a = TimeRange {
                not_before: Some(100),
                expiration: None,
            };
            let b = TimeRange {
                not_before: Some(200),
                expiration: None,
            };
            assert_eq!(a.intersect(&b).not_before, Some(200));
        }

        #[test]
        fn takes_earliest_expiration() {
            let a = TimeRange {
                not_before: None,
                expiration: Some(500),
            };
            let b = TimeRange {
                not_before: None,
                expiration: Some(300),
            };
            assert_eq!(a.intersect(&b).expiration, Some(300));
        }
    }
}
