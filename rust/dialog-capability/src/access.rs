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

/// Access protocol — defines how authorization is produced.
///
/// Different protocols use different access representations, proof
/// formats, and authorization/invocation materials.
pub trait Protocol: Sized + ConditionalSend + 'static {
    /// The type-erased form of a capability for this protocol.
    type Access: Scope + Serialize + for<'de> Deserialize<'de>;

    /// The signer type for this protocol.
    type Signer: crate::Principal + ConditionalSend;

    /// An individual delegation (signed certificate) in this protocol's format.
    type Certificate: Certificate<Access = Self::Access>;

    /// A delegation bundle — what [`Authorization::delegate`] produces.
    type Delegation: Delegation<Certificate = Self::Certificate>;

    /// An invocation chain — what [`Authorization::invoke`] produces.
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
#[derive(Serialize, Deserialize, crate::Attenuate)]
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

impl<P: Protocol> crate::Effect for Prove<P>
where
    P::Access: ConditionalSend + 'static,
{
    type Of = Access;
    type Output = Result<P::Proof, AuthorizeError>;
}

/// Retain effect — retains a delegation for future proof lookups.
///
/// An [`Effect`](crate::Effect) on [`Access`]. The subject DID
/// in the capability chain determines where proofs are stored.
#[derive(Serialize, Deserialize, crate::Attenuate)]
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

impl From<crate::StorageError> for AuthorizeError {
    fn from(e: crate::StorageError) -> Self {
        AuthorizeError::Configuration(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::TimeRange;

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
