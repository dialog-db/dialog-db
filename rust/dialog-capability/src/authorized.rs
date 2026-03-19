use crate::{Capability, Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use std::fmt::{Debug, Formatter};

/// A capability paired with its authorization proof.
///
/// `Authorized` bundles a capability with proof that the invoker has
/// permission to execute it. This is the input to authorized [`Provider`]
/// implementations.
///
/// - `C` is the constraint type (e.g., `storage::Get`)
/// - `A` is the authorization type (e.g., `AuthorizedRequest`)
pub struct Authorized<C: Constraint, A> {
    capability: Capability<C>,
    authorization: A,
}

impl<C: Constraint, A: Clone> Clone for Authorized<C, A>
where
    C::Capability: Clone,
{
    fn clone(&self) -> Self {
        Self {
            capability: Capability(self.capability.0.clone()),
            authorization: self.authorization.clone(),
        }
    }
}

impl<C: Constraint + Debug, A: Debug> Debug for Authorized<C, A>
where
    C::Capability: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authorized")
            .field("capability", &self.capability)
            .field("authorization", &self.authorization)
            .finish()
    }
}

impl<C: Constraint, A> Authorized<C, A> {
    /// Create a new authorized capability.
    pub fn new(capability: Capability<C>, authorization: A) -> Self {
        Self {
            capability,
            authorization,
        }
    }

    /// Get the capability.
    pub fn capability(&self) -> &Capability<C> {
        &self.capability
    }

    /// Get the authorization proof.
    pub fn authorization(&self) -> &A {
        &self.authorization
    }

    /// Consume and return the inner capability.
    pub fn into_capability(self) -> Capability<C> {
        self.capability
    }

    /// Consume and return the inner authorization.
    pub fn into_authorization(self) -> A {
        self.authorization
    }

    /// Consume and return both parts.
    pub fn into_parts(self) -> (Capability<C>, A) {
        (self.capability, self.authorization)
    }
}

impl<Fx: Effect + Constraint, A> Authorized<Fx, A> {
    /// Perform the authorized capability.
    ///
    /// The authorization proof was already fully formed during `acquire`,
    /// so this just delegates to the env's `Provider<Authorized<Fx, A>>`.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Env: Provider<Self> + ConditionalSend + ConditionalSync,
    {
        env.execute(self).await
    }
}
