//! Site trait for declaring authorization requirements.
//!
//! A [`Site`] represents pure configuration for a target location — no
//! credential material. It declares what access format it uses and what
//! invocation type it produces via a GAT.

use crate::access::{Access, LocalAccess};
use crate::authorization::Authorized;
use crate::command::Command;
use crate::effect::Effect;
use crate::{Capability, Constraint};
use dialog_common::ConditionalSend;

/// Pure site configuration — no credential material.
///
/// Implemented by types that describe where an operation should be directed.
/// The `Invocation` GAT declares what type `acquire` produces for each effect.
pub trait Site:
    Clone + ConditionalSend + serde::Serialize + serde::de::DeserializeOwned + 'static
{
    /// The access format this site uses.
    type Access: Access;

    /// The invocation type produced by `acquire` for a given effect.
    type Invocation<Fx: Effect>: Command<Output = Fx::Output> + From<Authorized<Fx, Self::Access>>;

    /// Extract the access context from this site.
    fn access(&self) -> Self::Access;
}

/// Local site — no remote backend needed.
///
/// Used for operations that execute directly without remote authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Local;

impl Site for Local {
    type Access = LocalAccess;
    type Invocation<Fx: Effect> = Allowed<Fx>;

    fn access(&self) -> LocalAccess {
        LocalAccess
    }
}

/// An allowed local invocation — wraps a bare capability after permission check.
pub struct Allowed<Fx: Effect>(pub Capability<Fx>);

impl<Fx: Effect> Allowed<Fx>
where
    Fx::Of: Constraint,
{
    /// Perform the allowed capability against a provider.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Env: crate::Provider<Fx>,
    {
        env.execute(self.0).await
    }
}

impl<Fx: Effect> Command for Allowed<Fx>
where
    Fx::Of: Constraint,
{
    type Input = Capability<Fx>;
    type Output = Fx::Output;
}

impl<Fx: Effect> From<Authorized<Fx, LocalAccess>> for Allowed<Fx> {
    fn from(auth: Authorized<Fx, LocalAccess>) -> Self {
        Allowed(auth.capability)
    }
}
