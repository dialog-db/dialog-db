use crate::access::Access;
use crate::{Capability, Constraint};
use std::fmt::{Debug, Formatter};

/// A capability paired with its access context and authorization proof.
///
/// Produced by `Provider<Authorize<Fx, A>>` and consumed by
/// `From<Authorized<Fx, A>>` on invocation types.
pub struct Authorized<Fx: Constraint, A: Access> {
    /// The authorized capability.
    pub capability: Capability<Fx>,
    /// The access context (carries addressing info).
    pub access: A,
    /// The authorization proof.
    pub authorization: A::Authorization,
}

impl<Fx: Constraint, A: Access + Clone> Clone for Authorized<Fx, A>
where
    Fx::Capability: Clone,
    A::Authorization: Clone,
{
    fn clone(&self) -> Self {
        Self {
            capability: self.capability.clone(),
            access: self.access.clone(),
            authorization: self.authorization.clone(),
        }
    }
}

impl<Fx: Constraint + Debug, A: Access + Debug> Debug for Authorized<Fx, A>
where
    Fx::Capability: Debug,
    A::Authorization: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authorized")
            .field("capability", &self.capability)
            .field("access", &self.access)
            .field("authorization", &self.authorization)
            .finish()
    }
}
