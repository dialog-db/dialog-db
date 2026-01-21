//! Authorization trait for capability-based access control.
//!
//! The `Authorization` trait represents proof of authority over a capability.

use super::ability::Ability;
use super::authority::Authority;
use super::capability::{Capability, Constraint, Effect};
use super::claim::Claim;
use super::delegation::Delegation;
use super::invocation::{Invocation, Proof};
use super::provider::Provider;
use super::subject::Did;

/// Errors that can occur during authorization.
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    /// Subject does not match the issuer's DID for self-authorization.
    #[error("Not authorized: subject '{subject}' does not match issuer '{issuer}'")]
    NotOwner {
        /// The subject DID from the capability.
        subject: Did,
        /// The issuer's DID.
        issuer: Did,
    },

    /// Audience does not match the issuer's DID for delegation/invocation.
    #[error("Cannot delegate/invoke: audience '{audience}' does not match issuer '{issuer}'")]
    NotAudience {
        /// The audience DID from the authorization.
        audience: Did,
        /// The issuer's DID.
        issuer: Did,
    },

    /// No valid delegation chain found.
    #[error("No valid delegation chain found from '{subject}' to '{audience}'")]
    NoDelegationChain {
        /// The subject DID.
        subject: Did,
        /// The audience DID.
        audience: Did,
    },

    /// Policy constraint violation.
    #[error("Policy constraint violation: {message}")]
    PolicyViolation {
        /// Description of the violation.
        message: String,
    },

    /// Serialization error during signing.
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Trait for proof of authority over a capability.
///
/// `Authorization` represents an abstract proof that `audience` of the
///  `claim` has an authority to excercise claimed capability. It can be:
///
/// - Self-issued (when subject == issuer)
/// - Derived from a delegation chain
pub trait Authorization<C: Ability>: Sized {
    /// The claim being authorized.
    fn claim(&self) -> &Claim<C>;

    /// The subject covered by this authorization.
    fn subject(&self) -> Did {
        self.claim().subject().clone()
    }

    /// The command path covered by this authorization.
    fn command(&self) -> String {
        self.claim().command()
    }

    /// The principal being authorized.
    fn audience(&self) -> Did {
        self.claim().audience().clone()
    }

    /// Binary representation of this authorization ensuring that it can be
    /// persisted.
    fn proof(&self) -> Proof;

    /// Self-issue an authorization in this format.
    ///
    /// Creates an self issued authorization, useful when the issuer owns the
    /// resource (capability.subject() == issuer.did()).
    ///
    /// Fails with `Err(AuthorizationError::NotOwner)` if subject != issuer.
    fn issue<A: Authority>(capability: C, issuer: &A) -> Result<Self, AuthorizationError>;

    /// Delegate this capability to the requested audience, granting them access
    /// to this capability.
    ///
    /// Fails with `Err(AuthorizationError::NotAudience)` if issuer.did() != self.audience().
    fn delegate<A: Authority>(
        &self,
        audience: &Did,
        issuer: &A,
    ) -> Result<Delegation<C, Self>, AuthorizationError>
    where
        C: Clone,
        Self: Clone;
}

/// A capability bundled with its authorization proof.
///
/// `Authorized` combines a capability with the authorization. Once you
/// have an `Authorized`, you can delegate further, and if the constraint is
/// an Effect, you can perform it.
///
/// # Type Parameters
///
/// - `C` - The capability type (must implement `Ability`)
/// - `A` - The authorization type
pub struct Authorized<C: Ability, A: Authorization<C>> {
    /// The capability.
    pub capability: C,
    /// The authorization proof.
    authorization: A,
}

impl<C: Ability, A: Authorization<C>> Authorized<C, A> {
    /// Create a new authorized capability.
    pub fn new(capability: C, authorization: A) -> Self {
        Self {
            capability,
            authorization,
        }
    }

    /// Get the capability.
    pub fn capability(&self) -> &C {
        &self.capability
    }

    /// Get the authorization.
    pub fn authorization(&self) -> &A {
        &self.authorization
    }

    /// Delegate this capability to another audience.
    pub fn delegate<Auth: Authority>(
        &self,
        audience: &Did,
        authority: &Auth,
    ) -> Result<Delegation<C, A>, AuthorizationError>
    where
        C: Clone,
        A: Clone,
    {
        self.authorization.delegate(audience, authority)
    }
}

/// Authorized capabilities implement Invocation when wrapping effect capabilities.
impl<Fx, A> Invocation for Authorized<Capability<Fx>, A>
where
    Fx: Effect,
    Fx::Of: Constraint,
    A: Authorization<Capability<Fx>>,
{
    type Input = Self;
    type Output = Fx::Output;
}

/// Perform method for authorized effect capabilities.
impl<Fx, A> Authorized<Capability<Fx>, A>
where
    Fx: Effect,
    Fx::Of: Constraint,
    A: Authorization<Capability<Fx>>,
{
    /// Perform the authorized invocation.
    pub async fn perform<Env>(self, env: &mut Env) -> Fx::Output
    where
        Env: Provider<Authorized<Capability<Fx>, A>>,
    {
        env.execute(self).await
    }
}