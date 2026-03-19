use crate::{Authorization, Capability, Constraint, Effect};

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
impl<Fx, Site> Invocation for Authorization<Fx, Site>
where
    Fx: Effect,
    Fx::Of: Constraint,
{
    type Input = Authorization<Fx, Site>;
    type Output = Fx::Output;
}
