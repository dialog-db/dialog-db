//! Invocation trait and types for requesting effect execution.
//!
//! The `Invocation` trait defines what can be passed to `Provider::execute`.

use super::Authorization;
use super::authorized::Authorized;
use super::constraint::Constraint;
use super::effect::Effect;
use super::interface::Capability;

/// Trait for types that can be invoked via a Provider.
///
/// This trait connects an invocation type to what the provider receives
/// as input and what it produces as output.
pub trait Invocation {
    /// The input type passed to Provider::execute.
    type Input;
    /// The output type returned from Provider::execute.
    type Output;
}

/// Effects can be invoked directly (without authorization).
impl<Fx> Invocation for Fx
where
    Fx: Effect,
    Fx::Of: Constraint,
{
    type Input = Capability<Fx>;
    type Output = Fx::Output;
}

/// Authorized effects can be invoked with authorization proof.
impl<Fx, A> Invocation for Authorized<Fx, A>
where
    Fx: Effect,
    Fx::Of: Constraint,
    A: Authorization,
{
    type Input = Authorized<Fx, A>;
    type Output = Fx::Output;
}
